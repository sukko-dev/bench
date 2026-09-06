package sub

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/sukko-dev/bench/internal/rlog"
	"github.com/sukko-dev/bench/internal/wire"
)

func benchData(t *testing.T, run, ch string, seq uint64) json.RawMessage {
	t.Helper()
	raw, err := wire.Encode(wire.Payload{Run: run, Channel: ch, Seq: seq, IntendedUnixNano: 1_757_000_000_000_000_000}, 200)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func waitConn(t *testing.T, g *fakeGateway) *fakeConn {
	t.Helper()
	select {
	case fc := <-g.newConnC:
		return fc
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber never connected")
		return nil
	}
}

func waitSubscribed(t *testing.T, fc *fakeConn) {
	t.Helper()
	select {
	case <-fc.gotSub:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber never sent subscribe")
	}
}

func startSubscriber(t *testing.T, g *fakeGateway, dir string) (*Subscriber, string) {
	t.Helper()
	logPath := filepath.Join(dir, "sub-0.rlog")
	s, err := Start(context.Background(), Config{
		URL: g.wsURL(), Token: "jwt-test", RunID: "r1", ClientID: "bench-sub-0",
		Channels: []string{"t.a", "t.b"}, LogPath: logPath,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop() })
	return s, logPath
}

func TestSubscribesAndLogsBenchMessages(t *testing.T) {
	g := newFakeGateway(t)
	s, logPath := startSubscriber(t, g, t.TempDir())
	fc := waitConn(t, g)
	waitSubscribed(t, fc)

	fc.sendMessage(1, "t.a", benchData(t, "r1", "t.a", 1), "1-100", "mid-a1", false)
	fc.sendMessage(2, "t.b", benchData(t, "r1", "t.b", 1), "1-101", "mid-b1", false)
	fc.sendMessage(3, "t.a", benchData(t, "r1", "t.a", 2), "1-102", "mid-a2", false)

	waitRecords(t, s, 3)
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	recs, err := rlog.ReadAll(logPath)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("logged %d records, want 3", len(recs))
	}
	r := recs[0]
	if r.Channel != "t.a" || r.Seq != 1 || r.Mid != "mid-a1" || r.IntendedUnixNano != 1_757_000_000_000_000_000 {
		t.Errorf("record 0 = %+v", r)
	}
	if r.ArrivalUnixNano == 0 {
		t.Error("arrival timestamp not stamped")
	}
}

func TestForeignMessagesAreNotLogged(t *testing.T) {
	g := newFakeGateway(t)
	s, logPath := startSubscriber(t, g, t.TempDir())
	fc := waitConn(t, g)
	waitSubscribed(t, fc)

	fc.sendMessage(1, "t.a", json.RawMessage(`{"price": 1.91}`), "1-1", "mid-x", false)  // not a bench payload
	fc.sendMessage(2, "t.a", benchData(t, "OTHER-RUN", "t.a", 1), "1-2", "mid-y", false) // foreign run
	fc.sendMessage(3, "t.a", benchData(t, "r1", "t.a", 1), "1-3", "mid-a1", false)       // ours

	waitRecords(t, s, 1)
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	recs, err := rlog.ReadAll(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].Mid != "mid-a1" {
		t.Fatalf("records = %+v, want only mid-a1", recs)
	}
}

func TestReconnectSendsReconnectBeforeSubscribeWithLastPos(t *testing.T) {
	g := newFakeGateway(t)
	s, _ := startSubscriber(t, g, t.TempDir())
	fc1 := waitConn(t, g)
	waitSubscribed(t, fc1)

	// Deliver pos-bearing messages, then kill the connection.
	fc1.sendMessage(1, "t.a", benchData(t, "r1", "t.a", 1), "1-100", "mid-a1", false)
	fc1.sendMessage(2, "t.b", benchData(t, "r1", "t.b", 1), "2-50", "mid-b1", false)
	fc1.sendMessage(3, "t.a", benchData(t, "r1", "t.a", 2), "1-101", "mid-a2", false)
	waitRecords(t, s, 3)
	_ = fc1.ws.Close() // abrupt kill

	fc2 := waitConn(t, g) // subscriber must redial
	waitSubscribed(t, fc2)

	types := fc2.frameTypes()
	if len(types) < 2 || types[0] != "reconnect" || types[1] != "subscribe" {
		t.Fatalf("resume frame order = %v, want reconnect before subscribe", types)
	}
	rec := fc2.frameOfType("reconnect")
	data, _ := rec["data"].(map[string]any)
	if data["client_id"] != "bench-sub-0" {
		t.Errorf("reconnect client_id = %v", data["client_id"])
	}
	lastPos, _ := data["last_pos"].(map[string]any)
	if lastPos["t.a"] != "1-101" || lastPos["t.b"] != "2-50" {
		t.Errorf("last_pos = %v, want latest pos per channel", lastPos)
	}
}

func TestReplayedMessagesAreLoggedLikeLive(t *testing.T) {
	g := newFakeGateway(t)
	s, logPath := startSubscriber(t, g, t.TempDir())
	fc := waitConn(t, g)
	waitSubscribed(t, fc)

	fc.sendMessage(1, "t.a", benchData(t, "r1", "t.a", 1), "1-100", "mid-a1", true) // history/replay copy
	fc.sendMessage(2, "t.a", benchData(t, "r1", "t.a", 2), "1-101", "mid-a2", false)

	waitRecords(t, s, 2)
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	recs, err := rlog.ReadAll(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("records = %d, want 2 (replay copies count — checker dedupes by mid)", len(recs))
	}
}

func TestDisconnectEventsAreRecorded(t *testing.T) {
	g := newFakeGateway(t)
	s, _ := startSubscriber(t, g, t.TempDir())
	fc1 := waitConn(t, g)
	waitSubscribed(t, fc1)
	_ = fc1.ws.Close()
	fc2 := waitConn(t, g)
	waitSubscribed(t, fc2)

	deadline := time.Now().Add(2 * time.Second)
	for {
		evs := s.Events()
		if len(evs) >= 2 {
			if evs[0].Kind != EventDisconnected || evs[1].Kind != EventResubscribed {
				t.Fatalf("events = %+v, want disconnect then resubscribed", evs)
			}
			if !evs[1].At.After(evs[0].At) {
				t.Fatal("event timestamps not ordered")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("events never recorded: %+v", evs)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestStartFailsClosedOnBadAuth(t *testing.T) {
	g := newFakeGateway(t)
	_, err := Start(context.Background(), Config{
		URL: g.wsURL(), Token: "", RunID: "r1", ClientID: "c",
		Channels: []string{"t.a"}, LogPath: filepath.Join(t.TempDir(), "x.rlog"),
	})
	if err == nil {
		t.Fatal("Start succeeded without a token")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want a dial/auth error, got %v", err)
	}
}

// waitRecords polls until the subscriber has logged n records (its counter —
// the log itself is only readable after Stop flushes).
func waitRecords(t *testing.T, s *Subscriber, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for s.Received() < n {
		if time.Now().After(deadline) {
			t.Fatalf("received %d records, want %d", s.Received(), n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
