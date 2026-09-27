package matrix

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sukko-dev/bench/internal/report"
)

var ceilings = Thresholds{P50: 60 * time.Millisecond, P99: 120 * time.Millisecond, P999: 200 * time.Millisecond}

// mkResult builds a report.Result for the gate under test. Latencies default to comfortably
// under the ceilings unless overridden.
func mkResult(pass bool, holes, confirmed, unconfirmed int, p50, p99, p999 time.Duration) report.Result {
	return report.Result{
		RunID: "r", Pass: pass, Holes: holes,
		ConfirmedPublishes: confirmed, UnconfirmedPublishes: unconfirmed,
		Latency: report.Latency{P50: p50, P99: p99, P999: p999},
	}
}

const (
	okP50  = 20 * time.Millisecond
	okP99  = 45 * time.Millisecond
	okP999 = 55 * time.Millisecond
)

func clean() report.Result { return mkResult(true, 0, 1000, 0, okP50, okP99, okP999) }

func TestClassify(t *testing.T) {
	// A fault run's tail is dominated by the injected outage (a valkey/redpanda kill delays every
	// message published during the outage by ~the outage), so a real fault run routinely shows
	// multi-second p99/p999 with zero data loss. 6.4s is the actual value observed in the v3 spike.
	const faultTail = 6400 * time.Millisecond
	tests := []struct {
		name    string
		r       report.Result
		gated   bool // latencyGated: true for the clean/no-fault run, false for a fault run
		wantOK  bool
		wantSub string // substring the first reason must contain (when !wantOK)
	}{
		// Gated (clean run): r.Pass gates and the latency ceilings gate.
		{"clean run passes", clean(), true, true, ""},
		{"data loss fails", mkResult(false, 12, 1000, 0, okP50, okP99, okP999), true, false, "data loss: 12 holes"},
		{"harness unsound fails", mkResult(false, 0, 0, 0, okP50, okP99, okP999), true, false, "harness unsound"},
		{"recovery-incomplete fails", mkResult(false, 0, 1000, 0, okP50, okP99, okP999), true, false, "recovery incomplete"},
		{"p99 latency breach fails the clean run", mkResult(true, 0, 1000, 0, okP50, 130*time.Millisecond, okP999), true, false, "p99"},
		{"p999 latency breach fails the clean run", mkResult(true, 0, 1000, 0, okP50, okP99, 250*time.Millisecond), true, false, "p999"},
		{"p50 latency breach fails the clean run", mkResult(true, 0, 1000, 0, 70*time.Millisecond, okP99, okP999), true, false, "p50"},
		// Not gated (fault run): r.Pass still gates, but the latency ceilings do NOT.
		{"fault run with a huge outage tail but zero loss PASSES", mkResult(true, 0, 1000, 0, okP50, faultTail, faultTail), false, true, ""},
		{"fault run still fails on data loss", mkResult(false, 5, 1000, 0, okP50, faultTail, faultTail), false, false, "data loss: 5 holes"},
		{"fault run still fails on incomplete recovery", mkResult(false, 0, 1000, 0, okP50, faultTail, faultTail), false, false, "recovery incomplete"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := Classify(tc.r, ceilings, tc.gated)
			if v.OK != tc.wantOK {
				t.Fatalf("OK = %v (reasons %v), want %v", v.OK, v.Reasons, tc.wantOK)
			}
			if !tc.wantOK {
				if len(v.Reasons) == 0 {
					t.Fatalf("failed run has no reasons")
				}
				if !contains(v.Reasons, tc.wantSub) {
					t.Errorf("reasons %v, want one containing %q", v.Reasons, tc.wantSub)
				}
			}
		})
	}
}

func contains(reasons []string, sub string) bool {
	for _, r := range reasons {
		if sub != "" && strings.Contains(r, sub) {
			return true
		}
	}
	return false
}

func TestRunAllClean(t *testing.T) {
	calls := 0
	run := func(_ string, _ int) (report.Result, error) { calls++; return clean(), nil }
	rep := Run([]string{CleanFault, "valkey", "redpanda"}, 3, ceilings, nil, run)
	if !rep.OK {
		t.Fatalf("report not OK: %+v", rep)
	}
	if calls != 9 {
		t.Errorf("run calls = %d, want 9 (3 faults × 3, no retry)", calls)
	}
	for _, f := range rep.Faults {
		if f.Retried {
			t.Errorf("fault %s retried on an all-clean run", f.Fault)
		}
	}
}

func TestRunFlakeRecoversOnRetry(t *testing.T) {
	// valkey loses data on its first batch, then is clean on the retry → a flake, report OK.
	perFault := map[string]int{}
	run := func(fault string, _ int) (report.Result, error) {
		perFault[fault]++
		if fault == "valkey" && perFault["valkey"] <= 3 { // first batch (3 runs) loses
			return mkResult(false, 5, 1000, 0, okP50, okP99, okP999), nil
		}
		return clean(), nil
	}
	rep := Run([]string{CleanFault, "valkey"}, 3, ceilings, nil, run)
	if !rep.OK {
		t.Fatalf("report should be OK — valkey recovered on retry: %+v", rep)
	}
	var valkey FaultOutcome
	for _, f := range rep.Faults {
		if f.Fault == "valkey" {
			valkey = f
		}
	}
	if !valkey.Retried {
		t.Errorf("valkey should be marked Retried")
	}
	if !valkey.OK {
		t.Errorf("valkey retry batch was clean → should be OK")
	}
}

func TestRunReproducedRegressionFails(t *testing.T) {
	// redpanda loses data on BOTH batches → a real regression.
	run := func(fault string, _ int) (report.Result, error) {
		if fault == "redpanda" {
			return mkResult(false, 8, 1000, 0, okP50, okP99, okP999), nil
		}
		return clean(), nil
	}
	rep := Run([]string{CleanFault, "redpanda"}, 3, ceilings, nil, run)
	if rep.OK {
		t.Fatalf("report should FAIL — redpanda reproduced data loss")
	}
	for _, f := range rep.Faults {
		if f.Fault == "redpanda" {
			if f.OK || !f.Retried {
				t.Errorf("redpanda: OK=%v Retried=%v, want OK=false Retried=true", f.OK, f.Retried)
			}
		}
	}
}

func TestRunLatencyGatedOnCleanRunOnly(t *testing.T) {
	// The identical zero-loss result with a multi-second tail: a PASS as a fault run (its tail is
	// the injected outage, not a regression), a FAIL as the clean run (a real steady-state breach).
	bigTail := mkResult(true, 0, 1000, 0, okP50, 6400*time.Millisecond, 6400*time.Millisecond)
	run := func(_ string, _ int) (report.Result, error) { return bigTail, nil }

	// valkey (a fault) with the big tail → OK: latency is not gated for a fault run.
	rep := Run([]string{"valkey"}, 2, ceilings, nil, run)
	if !rep.OK {
		t.Errorf("fault run with an outage tail but zero loss must PASS: %+v", rep.Faults[0].Runs)
	}

	// The same result under the clean run → FAIL on the latency ceiling.
	rep = Run([]string{CleanFault}, 2, ceilings, nil, run)
	if rep.OK {
		t.Fatalf("clean run with a 6.4s tail must FAIL the latency ceiling")
	}
	if !contains(rep.Faults[0].Runs[0].Reasons, "p99") {
		t.Errorf("clean-run failure should cite the latency ceiling: %+v", rep.Faults[0].Runs[0].Reasons)
	}
}

func TestRunNonGatingFaultDoesNotFail(t *testing.T) {
	// ws-server reproduces data loss on BOTH batches, but it is non-gating: the report stays OK,
	// the outcome is recorded + marked NonGating, and it is NOT retried (no wasted second batch).
	calls := map[string]int{}
	run := func(fault string, _ int) (report.Result, error) {
		calls[fault]++
		if fault == "ws-server" {
			return mkResult(false, 4, 1000, 0, okP50, okP99, okP999), nil
		}
		return clean(), nil
	}
	rep := Run([]string{CleanFault, "ws-server"}, 3, ceilings, map[string]bool{"ws-server": true}, run)
	if !rep.OK {
		t.Fatalf("report must be OK — ws-server is non-gating even though it lost data: %+v", rep)
	}
	var ws FaultOutcome
	for _, f := range rep.Faults {
		if f.Fault == "ws-server" {
			ws = f
		}
	}
	if !ws.NonGating {
		t.Errorf("ws-server should be marked NonGating")
	}
	if ws.OK {
		t.Errorf("ws-server verdict should still record OK=false (visible), got OK=true")
	}
	if ws.Retried {
		t.Errorf("a non-gating fault must not be retried")
	}
	if calls["ws-server"] != 3 {
		t.Errorf("ws-server should run its 3 runs once (no retry), got %d", calls["ws-server"])
	}
}

func TestRunErrorIsAFailure(t *testing.T) {
	run := func(_ string, _ int) (report.Result, error) {
		return report.Result{}, errors.New("stack failed to boot")
	}
	rep := Run([]string{CleanFault}, 2, ceilings, nil, run)
	if rep.OK {
		t.Fatalf("a RunFunc error must fail the report")
	}
	if !contains(rep.Faults[0].Runs[0].Reasons, "stack failed to boot") {
		t.Errorf("run error reason not surfaced: %+v", rep.Faults[0].Runs[0].Reasons)
	}
}
