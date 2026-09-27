// Package matrix aggregates a fault-matrix run into a pass/fail regression verdict.
//
// The gate: every run of every fault must PASS the bench's own verdict (zero data loss +
// harness-sound + recovery-ok, i.e. report.Result.Pass); a failing fault is retried once, and
// only a REPRODUCED failure is a regression — separating a real regression from an infra flake
// (ADR-0001).
//
// Latency ceilings apply to the CLEAN (no-fault) run ONLY. This follows the bench's own two
// claims (METHODOLOGY §5): C1 latency-under-burst is a STEADY-STATE claim, while a fault run is a
// C3 bounded-recovery claim judged on the checker verdict and recovery timing. A dependency killed
// mid-burst delays every message published during the outage by ~the outage, so a fault run's tail
// is seconds by construction — holding it to the steady-state ceiling would fail every fault run
// even with perfect zero-loss recovery (the chaos-engineering anti-pattern of gating mid-fault
// latency against the steady-state SLO). Latency is still RECORDED for every run regardless.
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

// CleanFault is the no-fault baseline run — the only run the latency ceilings gate (see Classify).
// Defined once here and referenced everywhere the "clean" run is special-cased (§I: no magic
// strings).
const CleanFault = "clean"

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
	Fault     string       `json:"fault"`
	OK        bool         `json:"ok"`
	Retried   bool         `json:"retried,omitempty"`
	NonGating bool         `json:"non_gating,omitempty"` // recorded + surfaced, but never fails the report
	Runs      []RunVerdict `json:"runs"`
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

// Classify judges one bench Result against the gate. Pure. r.Pass (zero-loss + harness-sound +
// recovery-ok) gates every run; the latency ceilings gate ONLY when latencyGated is true — the
// clean run — because a fault run's tail is dominated by the injected outage, not by a performance
// regression (see the package doc). Latency is recorded on the verdict regardless of latencyGated.
func Classify(r report.Result, t Thresholds, latencyGated bool) RunVerdict {
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
	if latencyGated {
		if t.P50 > 0 && r.Latency.P50 > t.P50 {
			v.Reasons = append(v.Reasons, fmt.Sprintf("p50 %s over ceiling %s", r.Latency.P50, t.P50))
		}
		if t.P99 > 0 && r.Latency.P99 > t.P99 {
			v.Reasons = append(v.Reasons, fmt.Sprintf("p99 %s over ceiling %s", r.Latency.P99, t.P99))
		}
		if t.P999 > 0 && r.Latency.P999 > t.P999 {
			v.Reasons = append(v.Reasons, fmt.Sprintf("p999 %s over ceiling %s", r.Latency.P999, t.P999))
		}
	}
	v.OK = len(v.Reasons) == 0
	return v
}

// runFault runs one fault's batch of n runs and returns its outcome. The clean (no-fault) run is
// the only one whose latency is gated — every other fault is a bounded-recovery test judged on
// zero-loss + recovery, not on a tail the outage inflates by construction.
func runFault(fault string, n int, t Thresholds, run RunFunc) FaultOutcome {
	oc := FaultOutcome{Fault: fault, OK: true}
	latencyGated := fault == CleanFault
	for i := range n {
		r, err := run(fault, i)
		var v RunVerdict
		if err != nil {
			v = RunVerdict{OK: false, Reasons: []string{"run error: " + err.Error()}}
		} else {
			v = Classify(r, t, latencyGated)
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
// A fault in nonGating is still run and classified — its outcome reaches matrix.json so the
// behavior stays visible — but it never fails the report and is not retried (retry-once exists
// only to keep a flaky GATING fault from false-failing the gate). Used for a fault whose failure
// is a known, separately-tracked gap rather than a regression the gate should block on (e.g.
// ws-server replica-kill loss pending ADR-0017 pod-level backfill).
func Run(faults []string, n int, t Thresholds, nonGating map[string]bool, run RunFunc) Report {
	rep := Report{OK: true}
	for _, f := range faults {
		oc := runFault(f, n, t, run)
		if nonGating[f] {
			oc.NonGating = true
			rep.Faults = append(rep.Faults, oc)
			continue // never retried, never gates
		}
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

// FaultDelay returns how long after a run starts the fault should fire: into the SECOND burst
// window (its start_fraction × the run duration, plus an offset so the kill lands inside the
// burst rather than exactly at its edge). This deterministically replaces the human who used to
// run the fault script from a second shell at "about the right moment" (ADR-0001).
func FaultDelay(runDuration time.Duration, secondBurstStartFraction float64, offset time.Duration) time.Duration {
	return time.Duration(float64(runDuration)*secondBurstStartFraction) + offset
}

// FaultLandsInBurst reports whether a fault fired at delay after the run starts lands inside the
// burst window [burstStart, burstStart+burstDuration). A fault injected before the window (or after
// it — or after the run has ended entirely) is fired at the wrong moment: the measured load never
// meets the fault, so the run can pass with the failure mode untested. The runner rejects such a
// configuration up front rather than shipping a silently-green gate (ADR-0001: "a gate that lies is
// worse than no gate").
func FaultLandsInBurst(delay, burstStart, burstDuration time.Duration) bool {
	return delay >= burstStart && delay < burstStart+burstDuration
}
