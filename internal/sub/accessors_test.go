package sub

import (
	"context"
	"path/filepath"
	"testing"
)

func TestAccessorsExposeIdentityChannelsAndRecords(t *testing.T) {
	g := newFakeGateway(t)
	logPath := filepath.Join(t.TempDir(), "sub-0.rlog")
	s, err := Start(context.Background(), Config{
		URL: g.wsURL(), Token: "jwt", RunID: "r1", ClientID: "bench-sub-7",
		Channels: []string{"t.a"}, LogPath: logPath,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	fc := waitConn(t, g)
	waitSubscribed(t, fc)

	if s.ID() != "bench-sub-7" {
		t.Errorf("ID() = %q, want bench-sub-7", s.ID())
	}
	if got := s.Channels(); len(got) != 1 || got[0] != "t.a" {
		t.Errorf("Channels() = %v, want [t.a]", got)
	}

	fc.sendMessage(1, "t.a", benchData(t, "r1", "t.a", 1), "1-1", "mid-1", false)
	waitRecords(t, s, 1)

	// Records() reads the flushed log — only valid after Stop.
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	recs, err := s.Records()
	if err != nil {
		t.Fatalf("Records: %v", err)
	}
	if len(recs) != 1 || recs[0].Mid != "mid-1" {
		t.Fatalf("Records() = %+v, want the one logged record", recs)
	}
}
