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
	tests := []struct {
		name    string
		r       report.Result
		wantOK  bool
		wantSub string // substring the first reason must contain (when !wantOK)
	}{
		{"clean run passes", clean(), true, ""},
		{"data loss fails", mkResult(false, 12, 1000, 0, okP50, okP99, okP999), false, "data loss: 12 holes"},
		{"harness unsound fails", mkResult(false, 0, 0, 0, okP50, okP99, okP999), false, "harness unsound"},
		{"recovery-incomplete fails", mkResult(false, 0, 1000, 0, okP50, okP99, okP999), false, "recovery incomplete"},
		{"p99 latency breach fails a zero-loss run", mkResult(true, 0, 1000, 0, okP50, 130*time.Millisecond, okP999), false, "p99"},
		{"p999 latency breach fails", mkResult(true, 0, 1000, 0, okP50, okP99, 250*time.Millisecond), false, "p999"},
		{"p50 latency breach fails", mkResult(true, 0, 1000, 0, 70*time.Millisecond, okP99, okP999), false, "p50"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := Classify(tc.r, ceilings)
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
	rep := Run([]string{"clean", "valkey", "redpanda"}, 3, ceilings, run)
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
	rep := Run([]string{"clean", "valkey"}, 3, ceilings, run)
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
	rep := Run([]string{"clean", "redpanda"}, 3, ceilings, run)
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

func TestRunErrorIsAFailure(t *testing.T) {
	run := func(_ string, _ int) (report.Result, error) {
		return report.Result{}, errors.New("stack failed to boot")
	}
	rep := Run([]string{"clean"}, 2, ceilings, run)
	if rep.OK {
		t.Fatalf("a RunFunc error must fail the report")
	}
	if !contains(rep.Faults[0].Runs[0].Reasons, "stack failed to boot") {
		t.Errorf("run error reason not surfaced: %+v", rep.Faults[0].Runs[0].Reasons)
	}
}
