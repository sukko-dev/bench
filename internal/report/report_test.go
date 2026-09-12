package report

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/sukko-dev/bench/internal/rlog"
)

func recs(n int, latencyNs int64) []rlog.Record {
	var rs []rlog.Record
	for i := 1; i <= n; i++ {
		rs = append(rs, rlog.Record{
			Channel: "t.a", Seq: uint64(i), Mid: "m",
			IntendedUnixNano: int64(i) * 1000,
			ArrivalUnixNano:  int64(i)*1000 + latencyNs,
		})
	}
	return rs
}

func TestLatencyPercentilesFromArrivalMinusIntended(t *testing.T) {
	// 100 records at a flat 5ms latency: every percentile is 5ms.
	l := Latencies(recs(100, 5*time.Millisecond.Nanoseconds()))
	if l.Count != 100 {
		t.Fatalf("Count = %d, want 100", l.Count)
	}
	for _, p := range []struct {
		name string
		got  time.Duration
	}{{"p50", l.P50}, {"p99", l.P99}, {"p999", l.P999}, {"max", l.Max}} {
		if p.got != 5*time.Millisecond {
			t.Errorf("%s = %v, want 5ms", p.name, p.got)
		}
	}
}

func TestPercentilesOrdered(t *testing.T) {
	// Mixed latencies: percentiles must be monotonic and bounded by max.
	var rs []rlog.Record
	for i := 1; i <= 1000; i++ {
		rs = append(rs, rlog.Record{
			Channel: "t.a", Seq: uint64(i), IntendedUnixNano: 0,
			ArrivalUnixNano: int64(i) * time.Microsecond.Nanoseconds(), // 1µs .. 1ms
		})
	}
	l := Latencies(rs)
	if !(l.P50 <= l.P99 && l.P99 <= l.P999 && l.P999 <= l.Max) {
		t.Errorf("percentiles not ordered: p50=%v p99=%v p999=%v max=%v", l.P50, l.P99, l.P999, l.Max)
	}
	if l.P50 < 400*time.Microsecond || l.P50 > 600*time.Microsecond {
		t.Errorf("p50 = %v, want ~500µs for a uniform 1µs..1ms spread", l.P50)
	}
}

func TestNegativeLatencyRecordsAreCountedSeparately(t *testing.T) {
	// A record arriving "before" its intended time means clock skew or a
	// same-VM race — it must be reported, not silently folded into p50.
	rs := recs(10, 5*time.Millisecond.Nanoseconds())
	rs = append(rs, rlog.Record{Channel: "t.a", Seq: 11, IntendedUnixNano: 1000, ArrivalUnixNano: 500})
	l := Latencies(rs)
	if l.Negative != 1 {
		t.Errorf("Negative = %d, want 1", l.Negative)
	}
	if l.Count != 10 {
		t.Errorf("Count = %d, want 10 (negatives excluded from the distribution)", l.Count)
	}
}

func TestEmptyLatencySetIsZeroNotPanic(t *testing.T) {
	l := Latencies(nil)
	if l.Count != 0 || l.P50 != 0 {
		t.Errorf("empty set = %+v, want zeroed", l)
	}
}

func TestWriteResultIsValidJSONWithVerdict(t *testing.T) {
	r := Result{
		RunID: "r1", Scenario: "odds-burst", Pass: false,
		Latency:  Latency{Count: 100, P50: 5 * time.Millisecond, P99: 9 * time.Millisecond},
		Holes:    2,
		Recovery: []RecoveryStat{{Fault: "kill-ws-server", ReconnectMs: 320, GapClosedMs: 1400}},
	}
	var sb strings.Builder
	if err := Write(&sb, r); err != nil {
		t.Fatalf("Write: %v", err)
	}
	var back map[string]any
	if err := json.Unmarshal([]byte(sb.String()), &back); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if back["pass"] != false {
		t.Errorf("verdict not serialized: %v", back["pass"])
	}
	// Latency durations serialize as human-readable strings (ms), not raw ns.
	lat, _ := back["latency"].(map[string]any)
	if lat["p99"] != "9ms" {
		t.Errorf("p99 serialized as %v, want \"9ms\"", lat["p99"])
	}
}

// TestHarnessSound pins the harness-health guard. Excluding unconfirmed publishes from
// the loss claim is only honest when there are FEW of them: a run where the harness
// failed to deliver most of its offered load proves nothing, and must fail loudly as a
// harness fault — not pass with a quietly shrunken denominator.
func TestHarnessSound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		confirmed   int
		unconfirmed int
		wantOK      bool
	}{
		{"clean run", 1000, 0, true},
		{"a few retly-limited stragglers under the threshold", 1000, 5, true},
		{"rejection fraction above threshold fails", 1000, 100, false},
		{"nothing confirmed fails even with nothing unconfirmed", 0, 0, false},
		{"everything rejected fails (the original vacuous-pass run)", 0, 2200, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ok, reason := HarnessSound(tt.confirmed, tt.unconfirmed)
			if ok != tt.wantOK {
				t.Errorf("HarnessSound(%d, %d) = %v (%q), want %v", tt.confirmed, tt.unconfirmed, ok, reason, tt.wantOK)
			}
			if !ok && reason == "" {
				t.Error("a failing verdict must carry a reason")
			}
		})
	}
}

// TestLatenciesWindowed pins the warmup exclusion: records INTENDED during warmup are
// excluded from the distribution and counted separately — never silently folded in
// (early sends carry connection-establishment noise the percentiles do not claim to
// measure), and never silently dropped either (the excluded count is reported).
// Windowing keys on the INTENDED time, not the arrival time: a warmup-scheduled message
// arriving late must not sneak into the measured window, and the open-loop schedule
// makes intended times the ground truth.
func TestLatenciesWindowed(t *testing.T) {
	t.Parallel()

	recs := []rlog.Record{
		{IntendedUnixNano: 100, ArrivalUnixNano: 100 + int64(50*time.Millisecond)}, // warmup
		{IntendedUnixNano: 900, ArrivalUnixNano: 900 + int64(40*time.Millisecond)}, // warmup, arrives late
		{IntendedUnixNano: 1000, ArrivalUnixNano: 1000 + int64(10*time.Millisecond)},
		{IntendedUnixNano: 2000, ArrivalUnixNano: 2000 + int64(20*time.Millisecond)},
	}
	l, excluded := LatenciesWindowed(recs, 1000)

	if excluded != 2 {
		t.Errorf("excluded = %d, want 2 (both warmup-intended records)", excluded)
	}
	if l.Count != 2 {
		t.Errorf("Count = %d, want 2", l.Count)
	}
	if l.Max != 20*time.Millisecond {
		t.Errorf("Max = %v, want 20ms — the 40ms and 50ms warmup latencies must not leak in", l.Max)
	}
}

// TestLatenciesWindowed_ZeroCutoffKeepsEverything: warmup=0 (the default) must be a
// no-op, not an accidental exclusion of the record intended exactly at T0.
func TestLatenciesWindowed_ZeroCutoffKeepsEverything(t *testing.T) {
	t.Parallel()

	recs := []rlog.Record{
		{IntendedUnixNano: 0, ArrivalUnixNano: int64(5 * time.Millisecond)},
		{IntendedUnixNano: 500, ArrivalUnixNano: 500 + int64(7*time.Millisecond)},
	}
	l, excluded := LatenciesWindowed(recs, 0)
	if excluded != 0 || l.Count != 2 {
		t.Errorf("excluded = %d, Count = %d; want 0 and 2", excluded, l.Count)
	}
}
