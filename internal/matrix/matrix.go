// Package matrix aggregates a fault-matrix run into a pass/fail regression verdict.
//
// The gate: every run of every fault must PASS the bench's own verdict (zero data loss +
// harness-sound + recovery-ok, i.e. report.Result.Pass) AND stay under generous absolute
// latency ceilings; a failing fault is retried once, and only a REPRODUCED failure is a
// regression — separating a real regression from an infra flake (ADR-0001).
//
// This is the gate: if it lies, a regression ships silently. So it is pure and unit-tested,
// with the orchestration (boot the stack, inject the fault into the burst window, run the
// bench binary, read result.json) injected as a RunFunc.
package matrix

import (
	"fmt"
	"time"

	"github.com/sukko-dev/bench/internal/report"
)

// Thresholds are the generous absolute latency ceilings (ADR-0001): a run fails only on a
// gross regression, so ordinary tail noise never flakes the gate. A zero ceiling disables that
// percentile's check.
type Thresholds struct {
	P50  time.Duration
	P99  time.Duration
	P999 time.Duration
}

// RunVerdict is one bench run classified against the gate.
type RunVerdict struct {
	RunID   string        `json:"run_id"`
	OK      bool          `json:"ok"`
	Reasons []string      `json:"reasons,omitempty"`
	Holes   int           `json:"holes"`
	P50     time.Duration `json:"p50"`
	P99     time.Duration `json:"p99"`
	P999    time.Duration `json:"p999"`
}

// FaultOutcome is a fault's final verdict. Runs holds the AUTHORITATIVE batch — the retry batch
// when Retried is true (the retry supersedes the flaky first batch), else the only batch.
type FaultOutcome struct {
	Fault   string       `json:"fault"`
	OK      bool         `json:"ok"`
	Retried bool         `json:"retried,omitempty"`
	Runs    []RunVerdict `json:"runs"`
}

// Report is the whole matrix verdict — OK is false if any fault reproduced a failure.
type Report struct {
	OK     bool           `json:"ok"`
	Faults []FaultOutcome `json:"faults"`
}

// RunFunc performs one bench run under fault f (run index 0..n-1) and returns its result. An
// error (the bench binary failed, or the stack could not be driven) classifies that run as
// failed, which triggers the same retry as a data-loss/latency failure.
type RunFunc func(fault string, run int) (report.Result, error)

// Classify judges one bench Result against the gate. Pure.
func Classify(r report.Result, t Thresholds) RunVerdict {
	v := RunVerdict{
		RunID: r.RunID, Holes: r.Holes,
		P50: r.Latency.P50, P99: r.Latency.P99, P999: r.Latency.P999,
	}
	if !r.Pass {
		switch {
		case r.Holes > 0:
			v.Reasons = append(v.Reasons, fmt.Sprintf("data loss: %d holes", r.Holes))
		default:
			if ok, why := report.HarnessSound(r.ConfirmedPublishes, r.UnconfirmedPublishes); !ok {
				v.Reasons = append(v.Reasons, "harness unsound: "+why)
			} else {
				v.Reasons = append(v.Reasons, "run did not pass (recovery incomplete)")
			}
		}
	}
	if t.P50 > 0 && r.Latency.P50 > t.P50 {
		v.Reasons = append(v.Reasons, fmt.Sprintf("p50 %s over ceiling %s", r.Latency.P50, t.P50))
	}
	if t.P99 > 0 && r.Latency.P99 > t.P99 {
		v.Reasons = append(v.Reasons, fmt.Sprintf("p99 %s over ceiling %s", r.Latency.P99, t.P99))
	}
	if t.P999 > 0 && r.Latency.P999 > t.P999 {
		v.Reasons = append(v.Reasons, fmt.Sprintf("p999 %s over ceiling %s", r.Latency.P999, t.P999))
	}
	v.OK = len(v.Reasons) == 0
	return v
}

// runFault runs one fault's batch of n runs and returns its outcome.
func runFault(fault string, n int, t Thresholds, run RunFunc) FaultOutcome {
	oc := FaultOutcome{Fault: fault, OK: true}
	for i := range n {
		r, err := run(fault, i)
		var v RunVerdict
		if err != nil {
			v = RunVerdict{OK: false, Reasons: []string{"run error: " + err.Error()}}
		} else {
			v = Classify(r, t)
		}
		oc.Runs = append(oc.Runs, v)
		if !v.OK {
			oc.OK = false
		}
	}
	return oc
}

// Run executes the fault matrix: for each fault, a batch of n runs; if that batch fails, the
// fault is retried ONCE (a fresh batch), and the retry batch is authoritative. Only a
// reproduced failure fails the report — a first-batch failure that clears on retry is a flake
// (Retried is recorded so the flake is visible). Faults run in the given order.
func Run(faults []string, n int, t Thresholds, run RunFunc) Report {
	rep := Report{OK: true}
	for _, f := range faults {
		oc := runFault(f, n, t, run)
		if !oc.OK {
			oc = runFault(f, n, t, run) // retry-once: the retry batch supersedes the flaky first
			oc.Retried = true
		}
		rep.Faults = append(rep.Faults, oc)
		if !oc.OK {
			rep.OK = false
		}
	}
	return rep
}
