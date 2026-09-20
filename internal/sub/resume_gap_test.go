package sub

import (
	"strings"
	"testing"
	"time"
)

// waitFrame polls until the client has sent a frame of the given type on fc.
func waitFrame(t *testing.T, fc *fakeConn, typ string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f := fc.frameOfType(typ); f != nil {
			return f
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("client never sent a %q frame; frames = %v", typ, fc.frameTypes())
	return nil
}

// waitEvent polls until the subscriber has recorded an event of the given kind.
func waitEvent(t *testing.T, s *Subscriber, kind EventKind) Event {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, ev := range s.Events() {
			if ev.Kind == kind {
				return ev
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("event %q never recorded; events = %+v", kind, s.Events())
	return Event{}
}

func hasEvent(s *Subscriber, kind EventKind) bool {
	for _, ev := range s.Events() {
		if ev.Kind == kind {
			return true
		}
	}
	return false
}

// TestResumeGapAnchorsStrictlyBeforeGapTs is the load-bearing test: messages
// keep arriving AFTER the lapse began but BEFORE the gap frame is delivered
// (the real-world interleaving), so the latest received pos sits after the
// hole. The replay must anchor at the newest pos whose envelope ts is
// strictly before the gap's ts — never the latest pos.
func TestResumeGapAnchorsStrictlyBeforeGapTs(t *testing.T) {
	g := newFakeGateway(t)
	s, _ := startSubscriber(t, g, t.TempDir())
	fc := waitConn(t, g)
	waitSubscribed(t, fc)

	fc.sendMessageAt(1000, 1, "t.a", benchData(t, "r1", "t.a", 1), "1-100", "mid-a1")
	fc.sendMessageAt(2000, 2, "t.a", benchData(t, "r1", "t.a", 2), "1-101", "mid-a2")
	// Lapse begins at ts 2500. These two arrive after the lapse but before
	// the server notices and emits the gap frame.
	fc.sendMessageAt(3000, 3, "t.a", benchData(t, "r1", "t.a", 3), "1-102", "mid-a3")
	fc.sendMessageAt(3500, 4, "t.a", benchData(t, "r1", "t.a", 4), "1-103", "mid-a4")
	waitRecords(t, s, 4)

	fc.sendGap("t.a", "resume", "", 2500)

	replay := waitFrame(t, fc, "replay")
	data, _ := replay["data"].(map[string]any)
	if data["channel"] != "t.a" {
		t.Errorf("replay channel = %v, want t.a", data["channel"])
	}
	if data["from_pos"] != "1-101" {
		t.Errorf("replay from_pos = %v, want 1-101 (newest pos with ts < 2500) — NOT the latest pos 1-103", data["from_pos"])
	}
	ev := waitEvent(t, s, EventReplayRequested)
	if ev.Channel != "t.a" {
		t.Errorf("replay_requested event channel = %q, want t.a", ev.Channel)
	}
}

// TestResumeGapNoBaselineSkipsReplay: every received message has ts >= gap.ts
// (subscriber effectively joined during the lapse) — there is no valid
// baseline, so no replay request may be sent. Also pins the STRICT
// comparison: a message with ts exactly equal to gap.ts is not a baseline.
func TestResumeGapNoBaselineSkipsReplay(t *testing.T) {
	g := newFakeGateway(t)
	s, _ := startSubscriber(t, g, t.TempDir())
	fc := waitConn(t, g)
	waitSubscribed(t, fc)

	fc.sendMessageAt(2500, 1, "t.a", benchData(t, "r1", "t.a", 1), "1-100", "mid-a1") // ts == gap ts: not strictly before
	fc.sendMessageAt(3000, 2, "t.a", benchData(t, "r1", "t.a", 2), "1-101", "mid-a2")
	waitRecords(t, s, 2)

	fc.sendGap("t.a", "resume", "", 2500)
	ev := waitEvent(t, s, EventReplaySkipped)
	if ev.Channel != "t.a" {
		t.Errorf("replay_skipped event channel = %q, want t.a", ev.Channel)
	}
	if hasEvent(s, EventReplayRequested) {
		t.Fatal("replay was requested despite no valid baseline")
	}
	// Round-trip one more message, then confirm no replay frame ever went out.
	fc.sendMessageAt(4000, 3, "t.a", benchData(t, "r1", "t.a", 3), "1-102", "mid-a3")
	waitRecords(t, s, 3)
	time.Sleep(50 * time.Millisecond)
	if f := fc.frameOfType("replay"); f != nil {
		t.Fatalf("client sent a replay frame with no baseline: %v", f)
	}
}

// TestCheckpointsDiscardedOnReconnect: ts checkpoints are per-connection —
// a reconnect may land on a different server with a different clock. After a
// reconnect, a resume gap whose ts would have matched the OLD connection's
// checkpoints must find no baseline.
func TestCheckpointsDiscardedOnReconnect(t *testing.T) {
	g := newFakeGateway(t)
	s, _ := startSubscriber(t, g, t.TempDir())
	fc1 := waitConn(t, g)
	waitSubscribed(t, fc1)

	fc1.sendMessageAt(1000, 1, "t.a", benchData(t, "r1", "t.a", 1), "1-100", "mid-a1")
	fc1.sendMessageAt(2000, 2, "t.a", benchData(t, "r1", "t.a", 2), "1-101", "mid-a2")
	waitRecords(t, s, 2)
	_ = fc1.ws.Close() // abrupt kill

	fc2 := waitConn(t, g)
	waitSubscribed(t, fc2)

	// Old checkpoints (ts 1000, 2000) would anchor this gap if illegally retained.
	fc2.sendGap("t.a", "resume", "", 5000)
	waitEvent(t, s, EventReplaySkipped)
	if hasEvent(s, EventReplayRequested) {
		t.Fatal("replay anchored on a checkpoint from a previous connection")
	}
	// The disconnect-path cursor is unaffected: reconnect carried last_pos.
	rec := fc2.frameOfType("reconnect")
	data, _ := rec["data"].(map[string]any)
	lastPos, _ := data["last_pos"].(map[string]any)
	if lastPos["t.a"] != "1-101" {
		t.Errorf("reconnect last_pos = %v, want 1-101 (latest pos, unchanged behavior)", lastPos)
	}
}

// TestCheckpointMemoryBoundedAndOldestSurvives: a long run must not grow
// checkpoint memory without bound, and thinning must never evict the oldest
// checkpoint — otherwise "no baseline" could fire spuriously and an old gap
// would become unrecoverable.
func TestCheckpointMemoryBoundedAndOldestSurvives(t *testing.T) {
	g := newFakeGateway(t)
	s, _ := startSubscriber(t, g, t.TempDir())
	fc := waitConn(t, g)
	waitSubscribed(t, fc)

	const n = 1500
	for i := range n {
		fc.sendMessageAt(int64(1000+i), i+1, "t.a", benchData(t, "r1", "t.a", uint64(i+1)), posAt(100+i), midAt(i))
	}
	deadline := time.Now().Add(10 * time.Second)
	for s.Received() < n {
		if time.Now().After(deadline) {
			t.Fatalf("received %d records, want %d", s.Received(), n)
		}
		time.Sleep(2 * time.Millisecond)
	}

	s.mu.Lock()
	got := len(s.ckpts["t.a"])
	s.mu.Unlock()
	if got > checkpointCap {
		t.Fatalf("checkpoint count = %d, exceeds cap %d", got, checkpointCap)
	}
	if got == 0 {
		t.Fatal("no checkpoints recorded")
	}

	// A gap just after the very first message must still anchor at it.
	fc.sendGap("t.a", "resume", "", 1001)
	replay := waitFrame(t, fc, "replay")
	data, _ := replay["data"].(map[string]any)
	if data["from_pos"] != "1-100" {
		t.Errorf("from_pos = %v, want 1-100 (the oldest checkpoint must survive thinning)", data["from_pos"])
	}
}

// TestOverflowGapReplaysFromLastPos: the pre-existing gap reason keeps its
// contract — replay from the gap frame's last_pos, echoed verbatim.
func TestOverflowGapReplaysFromLastPos(t *testing.T) {
	g := newFakeGateway(t)
	s, _ := startSubscriber(t, g, t.TempDir())
	fc := waitConn(t, g)
	waitSubscribed(t, fc)

	fc.sendMessageAt(1000, 1, "t.a", benchData(t, "r1", "t.a", 1), "1-100", "mid-a1")
	waitRecords(t, s, 1)
	fc.sendGap("t.a", "overflow", "1-333", 99999)

	replay := waitFrame(t, fc, "replay")
	data, _ := replay["data"].(map[string]any)
	if data["channel"] != "t.a" || data["from_pos"] != "1-333" {
		t.Errorf("overflow replay = %v, want channel t.a from_pos 1-333", data)
	}
	waitEvent(t, s, EventReplayRequested)
}

// countEvents returns how many recorded events have the given kind.
func countEvents(s *Subscriber, kind EventKind) int {
	n := 0
	for _, ev := range s.Events() {
		if ev.Kind == kind {
			n++
		}
	}
	return n
}

// waitEventCount polls until at least n events of the given kind exist.
func waitEventCount(t *testing.T, s *Subscriber, kind EventKind, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if countEvents(s, kind) >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("never saw %d %q events; events = %+v", n, kind, s.Events())
}

// waitFrames polls until the client has sent n frames of the given type.
func waitFrames(t *testing.T, fc *fakeConn, typ string, n int) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fs := fc.framesOfType(typ); len(fs) >= n {
			return fs
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("client never sent %d %q frames; frames = %v", n, typ, fc.frameTypes())
	return nil
}

// TestReplayCompleteFiresCoalescedPendingReplay pins the serialization and
// mid-flight-gap decision: while a replay is in flight, further gaps do not
// provoke REPLAY_IN_PROGRESS — they coalesce into ONE pending replay holding
// the OLDEST anchor (a replay from pos P re-delivers everything from P
// forward, so the oldest anchor covers every queued gap), which fires when
// replay_complete arrives.
func TestReplayCompleteFiresCoalescedPendingReplay(t *testing.T) {
	g := newFakeGateway(t)
	s, _ := startSubscriber(t, g, t.TempDir())
	fc := waitConn(t, g)
	waitSubscribed(t, fc)

	fc.sendMessageAt(1000, 1, "t.a", benchData(t, "r1", "t.a", 1), "1-100", "mid-a1")
	fc.sendMessageAt(2000, 2, "t.a", benchData(t, "r1", "t.a", 2), "1-101", "mid-a2")
	fc.sendMessageAt(3000, 3, "t.a", benchData(t, "r1", "t.a", 3), "1-102", "mid-a3")
	fc.sendMessageAt(4000, 4, "t.a", benchData(t, "r1", "t.a", 4), "1-103", "mid-a4")
	waitRecords(t, s, 4)

	fc.sendGap("t.a", "resume", "", 3500) // anchors at 1-102 → in flight
	waitFrame(t, fc, "replay")

	fc.sendGap("t.a", "resume", "", 2500) // mid-flight: candidate 1-101 (ts 2000)
	fc.sendGap("t.a", "resume", "", 1500) // mid-flight: candidate 1-100 (ts 1000) — older, replaces
	fc.sendGap("t.a", "resume", "", 2500) // mid-flight: candidate 1-101 — NOT older, pending stays 1-100
	waitEventCount(t, s, EventReplayQueued, 3)

	// Events are recorded synchronously on the read goroutine, so this is the
	// authoritative no-second-request assertion.
	if got := countEvents(s, EventReplayRequested); got != 1 {
		t.Fatalf("replay_requested events = %d, want 1 (replays must serialize per channel)", got)
	}
	time.Sleep(50 * time.Millisecond)
	if fs := fc.framesOfType("replay"); len(fs) != 1 {
		t.Fatalf("replay frames before complete = %d, want 1", len(fs))
	}

	fc.sendReplayComplete("t.a", 5, false)
	ev := waitEvent(t, s, EventReplayCompleted)
	if ev.Channel != "t.a" || !strings.Contains(ev.Detail, "5") {
		t.Errorf("replay_completed event = %+v, want channel t.a and messages_replayed=5 in detail", ev)
	}
	replays := waitFrames(t, fc, "replay", 2)
	data, _ := replays[1]["data"].(map[string]any)
	if data["from_pos"] != "1-100" {
		t.Errorf("pending replay from_pos = %v, want 1-100 (coalesced to the OLDEST anchor)", data["from_pos"])
	}
}

// TestTruncatedReplaySurfacesDistinctly: truncated means the hole was only
// partly filled — that must never look like a clean completion, or a
// measurement run could report improved numbers while silently
// under-recovering. It still closes the in-flight state.
func TestTruncatedReplaySurfacesDistinctly(t *testing.T) {
	g := newFakeGateway(t)
	s, _ := startSubscriber(t, g, t.TempDir())
	fc := waitConn(t, g)
	waitSubscribed(t, fc)

	fc.sendMessageAt(1000, 1, "t.a", benchData(t, "r1", "t.a", 1), "1-100", "mid-a1")
	waitRecords(t, s, 1)
	fc.sendGap("t.a", "resume", "", 1500)
	waitFrame(t, fc, "replay")

	fc.sendReplayComplete("t.a", 2, true)
	ev := waitEvent(t, s, EventReplayTruncated)
	if ev.Channel != "t.a" || !strings.Contains(ev.Detail, "2") {
		t.Errorf("replay_truncated event = %+v, want channel t.a and count in detail", ev)
	}
	if hasEvent(s, EventReplayCompleted) {
		t.Fatal("truncated replay was recorded as a clean completion")
	}
	// In-flight state is closed: the next gap fires immediately, not queued.
	fc.sendGap("t.a", "resume", "", 1500)
	waitFrames(t, fc, "replay", 2)
	if hasEvent(s, EventReplayQueued) {
		t.Fatal("gap after truncated complete was queued; in-flight state not closed")
	}
}

// TestReplayErrorsClassifiedTerminalVsTransient: offset_out_of_range means
// the anchor predates retention — recovery is impossible and must not look
// like a busy server (replay_in_progress / replay_rate_limited, which are
// transient). Errors arrive as the generic "error" envelope. Neither aborts
// the subscriber.
func TestReplayErrorsClassifiedTerminalVsTransient(t *testing.T) {
	g := newFakeGateway(t)
	s, _ := startSubscriber(t, g, t.TempDir())
	fc := waitConn(t, g)
	waitSubscribed(t, fc)

	fc.sendMessageAt(1000, 1, "t.a", benchData(t, "r1", "t.a", 1), "1-100", "mid-a1")
	waitRecords(t, s, 1)

	// Terminal: in-flight replay rejected with offset_out_of_range.
	fc.sendGap("t.a", "resume", "", 1500)
	waitFrame(t, fc, "replay")
	fc.sendError("t.a", "offset_out_of_range", "position is outside available history")
	ev := waitEvent(t, s, EventReplayUnrecoverable)
	if ev.Channel != "t.a" || !strings.Contains(ev.Detail, "offset_out_of_range") {
		t.Errorf("unrecoverable event = %+v, want channel t.a and code in detail", ev)
	}
	if hasEvent(s, EventReplayError) {
		t.Fatal("terminal error also surfaced as transient")
	}

	// The error closed the in-flight state: the next gap fires immediately.
	fc.sendGap("t.a", "resume", "", 1500)
	waitFrames(t, fc, "replay", 2)
	if hasEvent(s, EventReplayQueued) {
		t.Fatal("gap after replay error was queued; in-flight state not closed")
	}

	// Transient: in-flight replay rejected with a rate limit.
	fc.sendError("t.a", "replay_rate_limited", "replay rate limit exceeded")
	ev = waitEvent(t, s, EventReplayError)
	if !strings.Contains(ev.Detail, "replay_rate_limited") {
		t.Errorf("transient event = %+v, want code in detail", ev)
	}

	// A replay-specific code surfaces even with nothing in flight (the
	// harness may be out of sync with the server; visibility wins) …
	fc.sendError("t.a", "replay_in_progress", "replay already in progress for this channel")
	waitEventCount(t, s, EventReplayError, 2)

	// … but a generic code with nothing in flight is not attributable to
	// replay and stays ignored.
	fc.sendError("t.a", "invalid_request", "unrelated failure")
	fc.sendMessageAt(2000, 2, "t.a", benchData(t, "r1", "t.a", 2), "1-101", "mid-a2")
	waitRecords(t, s, 2)
	if got := countEvents(s, EventReplayUnrecoverable); got != 1 {
		t.Fatalf("unrecoverable events = %d, want 1 (generic error with no replay in flight must be ignored)", got)
	}

	if err := s.Stop(); err != nil {
		t.Fatalf("Stop after replay errors = %v, want nil (errors must not abort the run)", err)
	}
}

// TestMidFlightGapWithNoBaselineSkipsNotQueues: a mid-flight gap with no
// checkpoint strictly before its ts has no valid baseline — it must be
// skipped, never queued as a guess.
func TestMidFlightGapWithNoBaselineSkipsNotQueues(t *testing.T) {
	g := newFakeGateway(t)
	s, _ := startSubscriber(t, g, t.TempDir())
	fc := waitConn(t, g)
	waitSubscribed(t, fc)

	fc.sendMessageAt(2000, 1, "t.a", benchData(t, "r1", "t.a", 1), "1-100", "mid-a1")
	waitRecords(t, s, 1)
	fc.sendGap("t.a", "resume", "", 2500) // anchors at 1-100 → in flight
	waitFrame(t, fc, "replay")

	fc.sendGap("t.a", "resume", "", 1500) // no checkpoint with ts < 1500
	waitEvent(t, s, EventReplaySkipped)
	if hasEvent(s, EventReplayQueued) {
		t.Fatal("baseline-less mid-flight gap was queued instead of skipped")
	}

	fc.sendReplayComplete("t.a", 1, false)
	waitEvent(t, s, EventReplayCompleted)
	time.Sleep(50 * time.Millisecond)
	if got := countEvents(s, EventReplayRequested); got != 1 {
		t.Fatalf("replay_requested events = %d, want 1 (nothing valid was pending)", got)
	}
}

// TestInFlightStateDiscardedOnReconnect: replay serialization state is
// per-connection, like the checkpoints — a replay left in flight on a dead
// connection must not queue gaps on the next one.
func TestInFlightStateDiscardedOnReconnect(t *testing.T) {
	g := newFakeGateway(t)
	s, _ := startSubscriber(t, g, t.TempDir())
	fc1 := waitConn(t, g)
	waitSubscribed(t, fc1)

	fc1.sendMessageAt(1000, 1, "t.a", benchData(t, "r1", "t.a", 1), "1-100", "mid-a1")
	waitRecords(t, s, 1)
	fc1.sendGap("t.a", "resume", "", 1500)
	waitFrame(t, fc1, "replay") // in flight on conn 1
	_ = fc1.ws.Close()          // abrupt kill with the replay unresolved

	fc2 := waitConn(t, g)
	waitSubscribed(t, fc2)
	// New connection, new clock: a fresh baseline then a gap must fire
	// immediately — queued would mean in-flight state leaked across.
	fc2.sendMessageAt(500, 2, "t.a", benchData(t, "r1", "t.a", 2), "1-200", "mid-a2")
	waitRecords(t, s, 2)
	fc2.sendGap("t.a", "resume", "", 900)

	replay := waitFrame(t, fc2, "replay")
	data, _ := replay["data"].(map[string]any)
	if data["from_pos"] != "1-200" {
		t.Errorf("post-reconnect replay from_pos = %v, want 1-200", data["from_pos"])
	}
	if hasEvent(s, EventReplayQueued) {
		t.Fatal("gap on the new connection was queued; in-flight state survived the reconnect")
	}
}
