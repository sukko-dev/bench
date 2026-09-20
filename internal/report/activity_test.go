package report

import (
	"strings"
	"testing"
	"time"

	"github.com/sukko-dev/bench/internal/check"
	"github.com/sukko-dev/bench/internal/sub"
)

func evAt(t0 time.Time, offset time.Duration, kind sub.EventKind, channel, detail string) sub.Event {
	return sub.Event{Kind: kind, At: t0.Add(offset), Channel: channel, Detail: detail}
}

func TestBuildActivityAggregatesAndPartitionsByOutcome(t *testing.T) {
	t0 := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	subs := []SubEvents{
		{ID: "bench-sub-0", Events: []sub.Event{ // replica whose replay completed
			evAt(t0, 144200*time.Millisecond, sub.EventResumeGap, "t.a", "gap ts=1757000"),
			evAt(t0, 144300*time.Millisecond, sub.EventReplayRequested, "t.a", "1-1234"),
			evAt(t0, 147100*time.Millisecond, sub.EventReplayCompleted, "t.a", "messages_replayed=63"),
		}},
		{ID: "bench-sub-1", Events: []sub.Event{ // replica where replay is structurally impossible
			evAt(t0, 144250*time.Millisecond, sub.EventResumeGap, "t.b", "gap ts=1757000"),
			evAt(t0, 144350*time.Millisecond, sub.EventReplayRequested, "t.b", "2-999"),
			evAt(t0, 144400*time.Millisecond, sub.EventReplayUnrecoverable, "t.b", "not_available: channel not available for replay"),
		}},
	}
	holes := []check.Hole{
		{Subscriber: "bench-sub-1", Channel: "t.b", FromSeq: 281, ToSeq: 284},
		{Subscriber: "bench-sub-1", Channel: "t.b", FromSeq: 288, ToSeq: 350},
	}

	counts, rows := BuildActivity(subs, holes, t0)

	if counts[string(sub.EventResumeGap)] != 2 || counts[string(sub.EventReplayCompleted)] != 1 || counts[string(sub.EventReplayUnrecoverable)] != 1 {
		t.Errorf("aggregate counts = %v", counts)
	}
	if len(rows) != 2 || rows[0].ID != "bench-sub-0" || rows[1].ID != "bench-sub-1" {
		t.Fatalf("rows = %+v, want both subscribers in input order", rows)
	}

	// The partition the fault triage needs: outcome per subscriber, holes per
	// subscriber, in the same row.
	if rows[0].Events[string(sub.EventReplayCompleted)] != 1 || rows[0].Holes != 0 {
		t.Errorf("completed-replay row = %+v, want replay_completed=1 holes=0", rows[0])
	}
	if rows[1].Events[string(sub.EventReplayUnrecoverable)] != 1 || rows[1].Holes != 2 {
		t.Errorf("failed-replay row = %+v, want replay_unrecoverable=1 holes=2", rows[1])
	}
	if len(rows[1].HolesDetail) != 2 || rows[1].HolesDetail[1] != "t.b seqs 288-350" {
		t.Errorf("holes_detail = %v", rows[1].HolesDetail)
	}

	// Event log lines carry run-relative offsets so hole timing can be
	// correlated with recovery timing.
	if len(rows[0].EventLog) != 3 {
		t.Fatalf("event_log = %v", rows[0].EventLog)
	}
	line := rows[0].EventLog[2]
	if !strings.HasPrefix(line, "+147.1s ") || !strings.Contains(line, "replay_completed") ||
		!strings.Contains(line, "t.a") || !strings.Contains(line, "messages_replayed=63") {
		t.Errorf("event log line = %q", line)
	}
}

func TestBuildActivityRowForHolesWithoutEvents(t *testing.T) {
	// A subscriber with holes but zero events is exactly the datapoint that
	// refutes (or confirms) a replay-induced hypothesis — it must get a row.
	t0 := time.Now()
	subs := []SubEvents{{ID: "bench-sub-3"}}
	holes := []check.Hole{{Subscriber: "bench-sub-3", Channel: "t.c", FromSeq: 5, ToSeq: 9}}
	_, rows := BuildActivity(subs, holes, t0)
	if len(rows) != 1 || rows[0].ID != "bench-sub-3" || rows[0].Holes != 1 {
		t.Fatalf("rows = %+v, want one row with the hole", rows)
	}
	if len(rows[0].Events) != 0 {
		t.Errorf("events = %v, want empty", rows[0].Events)
	}
}

func TestBuildActivityOmitsQuietSubscribers(t *testing.T) {
	t0 := time.Now()
	subs := []SubEvents{
		{ID: "bench-sub-0"}, // no events, no holes: no row — healthy runs stay lean
		{ID: "bench-sub-1", Events: []sub.Event{evAt(t0, time.Second, sub.EventDisconnected, "", "")}},
	}
	counts, rows := BuildActivity(subs, nil, t0)
	if len(rows) != 1 || rows[0].ID != "bench-sub-1" {
		t.Fatalf("rows = %+v, want only the subscriber with activity", rows)
	}
	if counts[string(sub.EventDisconnected)] != 1 {
		t.Errorf("counts = %v", counts)
	}
	// Channel-less events must not carry trailing separators.
	if got := rows[0].EventLog[0]; got != "+1.0s disconnected" {
		t.Errorf("event log line = %q, want %q", got, "+1.0s disconnected")
	}
}

func TestBuildActivityCapsPerSubscriberDetail(t *testing.T) {
	t0 := time.Now()
	var events []sub.Event
	for i := range eventLogMax + 7 {
		events = append(events, evAt(t0, time.Duration(i)*time.Second, sub.EventReplayError, "t.a", "replay_rate_limited"))
	}
	var holes []check.Hole
	for i := range holesDetailMax + 3 {
		holes = append(holes, check.Hole{Subscriber: "bench-sub-0", Channel: "t.a", FromSeq: uint64(10 * i), ToSeq: uint64(10*i + 1)})
	}
	counts, rows := BuildActivity([]SubEvents{{ID: "bench-sub-0", Events: events}}, holes, t0)

	row := rows[0]
	if len(row.EventLog) != eventLogMax || row.EventsOmitted != 7 {
		t.Errorf("event_log len = %d omitted = %d, want %d and 7", len(row.EventLog), row.EventsOmitted, eventLogMax)
	}
	// Counts are NEVER capped — only the human-readable detail is.
	if row.Events[string(sub.EventReplayError)] != eventLogMax+7 || counts[string(sub.EventReplayError)] != eventLogMax+7 {
		t.Errorf("event counts capped: row=%v aggregate=%v", row.Events, counts)
	}
	if len(row.HolesDetail) != holesDetailMax || row.HolesOmitted != 3 || row.Holes != holesDetailMax+3 {
		t.Errorf("holes: detail=%d omitted=%d total=%d", len(row.HolesDetail), row.HolesOmitted, row.Holes)
	}
}

func TestRecoverySoundFailsOnTruncatedReplay(t *testing.T) {
	ok, reason := RecoverySound(map[string]int{
		string(sub.EventReplayCompleted): 237,
		string(sub.EventReplayTruncated): 2,
	})
	if ok {
		t.Fatal("RecoverySound passed a run with truncated replays — the vacuous-green class this exists to kill")
	}
	if !strings.Contains(reason, "2") || !strings.Contains(reason, "truncated") {
		t.Errorf("reason = %q, want the count and the word truncated", reason)
	}

	ok, reason = RecoverySound(map[string]int{string(sub.EventReplayCompleted): 238})
	if !ok || reason != "" {
		t.Errorf("clean run: ok=%v reason=%q, want true and empty", ok, reason)
	}
	ok, reason = RecoverySound(nil)
	if !ok || reason != "" {
		t.Errorf("no events: ok=%v reason=%q, want true and empty", ok, reason)
	}
}

func TestResultJSONCarriesActivityAndRecoveryFault(t *testing.T) {
	var buf strings.Builder
	r := Result{
		RunID: "r1", Scenario: "fault-valkey.toml", Pass: false,
		Events: map[string]int{string(sub.EventReplayTruncated): 2},
		Subscribers: []SubscriberActivity{{
			ID:       "bench-sub-0",
			Events:   map[string]int{string(sub.EventReplayTruncated): 1},
			EventLog: []string{"+144.2s replay_truncated t.a messages_replayed=63 truncated"},
			Holes:    1, HolesDetail: []string{"t.a seqs 288-350"},
		}},
		RecoveryFault: "1 replay(s) completed truncated",
	}
	if err := Write(&buf, r); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, key := range []string{`"events"`, `"subscribers"`, `"recovery_fault"`, `"event_log"`, `"holes_detail"`, `"replay_truncated"`} {
		if !strings.Contains(out, key) {
			t.Errorf("result.json missing %s:\n%s", key, out)
		}
	}

	// A quiet run's artifact stays exactly as lean as before this feature.
	buf.Reset()
	if err := Write(&buf, Result{RunID: "r2", Scenario: "s", Pass: true}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"events"`, `"subscribers"`, `"recovery_fault"`} {
		if strings.Contains(buf.String(), key) {
			t.Errorf("quiet result.json carries %s:\n%s", key, buf.String())
		}
	}
}
