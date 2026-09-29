package sub

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sukko-dev/bench/internal/wire"
)

// sseEnvelope builds a broadcast-shaped SSE `data:` payload wrapping a bench wire
// payload, matching what the gateway writes for a delivery. MUST be called from
// the test goroutine (it uses t.Fatalf), so callers pre-build envelope strings
// before starting the httptest server.
func sseEnvelope(t *testing.T, runID, channel string, seq uint64, pos, mid string) string {
	t.Helper()
	data, err := wire.Encode(wire.Payload{
		Run:              runID,
		Channel:          channel,
		Seq:              seq,
		IntendedUnixNano: time.Now().UnixNano(),
	}, 256) // 256 ≥ the ~100-byte minimum payload; wire.Encode pads to the target
	if err != nil {
		t.Fatalf("wire.Encode: %v", err)
	}
	return fmt.Sprintf(`{"type":"message","channel":%q,"data":%s,"pos":%q,"mid":%q,"ts":%d}`,
		channel, data, pos, mid, seq)
}

func writeSSE(w http.ResponseWriter, id, dataLine string) {
	if id != "" {
		fmt.Fprintf(w, "id: %s\n", id)
	}
	fmt.Fprintf(w, "event: message\ndata: %s\n\n", dataLine)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// TestSSESubscriber_RecoversGapViaLastEventID is the load-bearing test: a stream
// delivers records, then ends (a simulated ws-server kill). The subscriber must
// redial echoing the last opaque id: cursor verbatim, and the records the server
// replays on that reconnect must be logged — no loss across the kill. It also pins
// that the cursor is echoed exactly as received (never decoded/rewritten).
func TestSSESubscriber_RecoversGapViaLastEventID(t *testing.T) {
	const runID, channel, cursor = "run-1", "t.a", "v1:OPAQUE-CURSOR"

	// Pre-build envelopes in the test goroutine (sseEnvelope uses t.Fatalf).
	env1 := sseEnvelope(t, runID, channel, 1, "1-100", "mid-1")
	env2 := sseEnvelope(t, runID, channel, 2, "1-101", "mid-2")
	env3 := sseEnvelope(t, runID, channel, 3, "1-102", "mid-3")

	var mu sync.Mutex
	var connects int
	var gotLastEventID []string // Last-Event-ID header seen on each connect

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		connects++
		n := connects
		gotLastEventID = append(gotLastEventID, r.Header.Get("Last-Event-ID"))
		mu.Unlock()

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		if n == 1 {
			// Live leg: two records, the second carrying the resume cursor. Then the
			// handler returns → stream EOF → the subscriber sees a disconnect.
			writeSSE(w, "", env1)
			writeSSE(w, cursor, env2)
			return
		}
		// Redial leg: replay the gap the subscriber missed (seq 3), then hold the
		// stream open until the run's context ends so Stop can unblock the read.
		writeSSE(w, "v1:CURSOR-2", env3)
		<-r.Context().Done()
	}))
	defer srv.Close()

	s, err := StartSSE(context.Background(), Config{
		URL:         srv.URL,
		Token:       "tok",
		RunID:       runID,
		ClientID:    "sse-1",
		Channels:    []string{channel},
		LogPath:     filepath.Join(t.TempDir(), "sse-1.rlog"),
		RedialDelay: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("StartSSE: %v", err)
	}
	defer func() { _ = s.Stop() }()

	// All three records (2 live + 1 replayed after reconnect) must be logged.
	deadline := time.Now().Add(3 * time.Second)
	for s.Received() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := s.Received(); got != 3 {
		t.Fatalf("received %d records, want 3 (2 live + 1 replayed) — recovery lost the gap", got)
	}

	// The redial must have echoed the opaque cursor verbatim.
	mu.Lock()
	defer mu.Unlock()
	if connects < 2 {
		t.Fatalf("expected a redial after the stream ended, got %d connects", connects)
	}
	if gotLastEventID[0] != "" {
		t.Errorf("first connect carried Last-Event-ID %q, want none", gotLastEventID[0])
	}
	if gotLastEventID[1] != cursor {
		t.Errorf("redial Last-Event-ID = %q, want %q (echoed verbatim)", gotLastEventID[1], cursor)
	}

	// The disconnect/reconnect were recorded as recovery events.
	var sawDisc, sawResub bool
	for _, ev := range s.Events() {
		switch ev.Kind {
		case EventDisconnected:
			sawDisc = true
		case EventResubscribed:
			sawResub = true
		}
	}
	if !sawDisc || !sawResub {
		t.Errorf("recovery events: disconnected=%v resubscribed=%v, want both", sawDisc, sawResub)
	}
}

// TestSSESubscriber_FiltersForeignRun proves records from another bench run are
// never logged — the checker's per-run isolation holds on the SSE path too.
func TestSSESubscriber_FiltersForeignRun(t *testing.T) {
	const runID, channel = "run-mine", "t.a"

	foreign := sseEnvelope(t, "run-OTHER", channel, 1, "1-100", "mid-x")
	mine := sseEnvelope(t, runID, channel, 1, "1-101", "mid-mine")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		writeSSE(w, "", foreign) // foreign run — must be dropped
		writeSSE(w, "", mine)    // this run — must be logged
		<-r.Context().Done()
	}))
	defer srv.Close()

	s, err := StartSSE(context.Background(), Config{
		URL: srv.URL, Token: "tok", RunID: runID, ClientID: "sse-1",
		Channels: []string{channel}, LogPath: filepath.Join(t.TempDir(), "sse-1.rlog"),
	})
	if err != nil {
		t.Fatalf("StartSSE: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for s.Received() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	// Give the foreign record a chance to (wrongly) land, then assert only mine did.
	time.Sleep(50 * time.Millisecond)
	if got := s.Received(); got != 1 {
		t.Fatalf("received %d, want 1 (foreign run must be dropped)", got)
	}
	// Records are read after Stop flushes the log (the scenario engine's usage).
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	recs, err := s.Records()
	if err != nil {
		t.Fatalf("Records: %v", err)
	}
	if len(recs) != 1 || recs[0].Mid != "mid-mine" {
		t.Fatalf("logged records = %+v, want only mid-mine", recs)
	}
}

// TestSSESubscriber_ConnectRejectionIsSetupError proves a non-200 (e.g. the Pro
// gate's 403) fails StartSSE loudly rather than yielding a silent zero-delivery
// subscriber.
func TestSSESubscriber_ConnectRejectionIsSetupError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"code":"EDITION_LIMIT"}`, http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := StartSSE(context.Background(), Config{
		URL: srv.URL, Token: "tok", RunID: "run-1", ClientID: "sse-1",
		Channels: []string{"t.a"}, LogPath: filepath.Join(t.TempDir(), "sse-1.rlog"),
	})
	if err == nil {
		t.Fatal("expected StartSSE to fail on a 403, got nil error")
	}
}

// TestSSESubscriber_BareIDBlockCommitsCursor pins the WHATWG dispatch semantics the
// cursor-commit fix relies on: a keepalive comment is skipped; a bare `id:\n\n` block
// (no data — the gateway's low-traffic keepalive-flush) commits the resume cursor; and
// a following message event with NO id: line does not clobber it. On redial the echoed
// Last-Event-ID must be the bare block's id.
func TestSSESubscriber_BareIDBlockCommitsCursor(t *testing.T) {
	const runID, channel, bareCursor = "run-1", "t.a", "v1:BARE-CURSOR"
	msgNoID := sseEnvelope(t, runID, channel, 1, "1-100", "mid-1")

	var mu sync.Mutex
	var connects int
	var lastEventIDs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		connects++
		n := connects
		lastEventIDs = append(lastEventIDs, r.Header.Get("Last-Event-ID"))
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flush := func() {
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		if n == 1 {
			fmt.Fprint(w, ": keepalive\n\n")                      // comment — skipped
			fmt.Fprintf(w, "id: %s\n\n", bareCursor)              // bare id block — commits cursor, no event
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", msgNoID) // message with NO id: — must not clobber cursor
			flush()
			return // stream ends → subscriber redials
		}
		fmt.Fprintf(w, "event: message\ndata: %s\n\n", sseEnvelope(t, runID, channel, 2, "1-101", "mid-2"))
		flush()
		<-r.Context().Done()
	}))
	defer srv.Close()

	s, err := StartSSE(context.Background(), Config{
		URL: srv.URL, Token: "tok", RunID: runID, ClientID: "sse-1",
		Channels: []string{channel}, LogPath: filepath.Join(t.TempDir(), "sse-1.rlog"),
		RedialDelay: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("StartSSE: %v", err)
	}
	defer func() { _ = s.Stop() }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := connects
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if connects < 2 {
		t.Fatalf("expected a redial, got %d connects", connects)
	}
	if lastEventIDs[1] != bareCursor {
		t.Errorf("redial Last-Event-ID = %q, want %q (bare id block commits the cursor; a later id-less message must not clobber it)", lastEventIDs[1], bareCursor)
	}
}

// TestSSESubscriber_MultilineData proves multiple data: lines in one event are joined
// with '\n' before decode — a split envelope still parses and logs.
func TestSSESubscriber_MultilineData(t *testing.T) {
	const runID, channel = "run-1", "t.a"
	env := sseEnvelope(t, runID, channel, 1, "1-100", "mid-1")
	// Split at a token boundary (just after the comma before "pos") so the rejoined
	// '\n' lands as inter-token whitespace, which JSON tolerates — a mid-string split
	// would not. This exercises the reader's multi-line data: joining.
	cut := strings.Index(env, `,"pos"`) + 1
	if cut <= 0 {
		t.Fatalf("could not find split boundary in %q", env)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// Two data: lines, joined with '\n' by the reader before decode.
		fmt.Fprintf(w, "event: message\ndata: %s\ndata: %s\n\n", env[:cut], env[cut:])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	s, err := StartSSE(context.Background(), Config{
		URL: srv.URL, Token: "tok", RunID: runID, ClientID: "sse-1",
		Channels: []string{channel}, LogPath: filepath.Join(t.TempDir(), "sse-1.rlog"),
	})
	if err != nil {
		t.Fatalf("StartSSE: %v", err)
	}
	defer func() { _ = s.Stop() }()

	deadline := time.Now().Add(2 * time.Second)
	for s.Received() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if s.Received() != 1 {
		t.Fatalf("received %d, want 1 (multi-line data must rejoin and parse)", s.Received())
	}
}
