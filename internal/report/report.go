// Package report turns a run's receive logs and verdict into the committed
// result artifact: an end-to-end latency distribution (arrival − intended,
// the coordinated-omission-safe measure) and a JSON result with the checker
// verdict and recovery stats.
package report

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/sukko-dev/bench/internal/rlog"
)

// Latency is the end-to-end delivery latency distribution.
type Latency struct {
	Count    int           `json:"count"`
	Negative int           `json:"negative"` // records arriving before intended (skew/race) — excluded, reported
	P50      time.Duration `json:"p50"`
	P99      time.Duration `json:"p99"`
	P999     time.Duration `json:"p999"`
	Max      time.Duration `json:"max"`
}

// LatenciesWindowed computes the distribution over records INTENDED at or after
// cutoffUnixNano, returning the count excluded as warmup. Windowing keys on the
// intended time (the open-loop ground truth), not the arrival time — a
// warmup-scheduled message arriving late must not leak into the measured window.
// A zero cutoff keeps everything, including a record intended exactly at T0=0.
func LatenciesWindowed(recs []rlog.Record, cutoffUnixNano int64) (Latency, int) {
	kept := make([]rlog.Record, 0, len(recs))
	excluded := 0
	for _, r := range recs {
		if cutoffUnixNano > 0 && r.IntendedUnixNano < cutoffUnixNano {
			excluded++
			continue
		}
		kept = append(kept, r)
	}
	return Latencies(kept), excluded
}

// Latencies computes the distribution over records with a non-negative
// arrival−intended delta. Negative deltas (clock skew or a same-VM race) are
// counted separately and excluded — never silently folded into the percentiles.
func Latencies(recs []rlog.Record) Latency {
	deltas := make([]time.Duration, 0, len(recs))
	var negative int
	for _, r := range recs {
		d := r.ArrivalUnixNano - r.IntendedUnixNano
		if d < 0 {
			negative++
			continue
		}
		deltas = append(deltas, time.Duration(d))
	}
	l := Latency{Count: len(deltas), Negative: negative}
	if len(deltas) == 0 {
		return l
	}
	sort.Slice(deltas, func(i, j int) bool { return deltas[i] < deltas[j] })
	l.P50 = percentile(deltas, 0.50)
	l.P99 = percentile(deltas, 0.99)
	l.P999 = percentile(deltas, 0.999)
	l.Max = deltas[len(deltas)-1]
	return l
}

// percentile returns the p-quantile (0..1) of a sorted slice via
// nearest-rank: the smallest value whose rank covers the fraction p.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(p * float64(len(sorted)))
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

// RecoveryStat is one fault's recovery timing.
type RecoveryStat struct {
	Fault       string `json:"fault"`
	ReconnectMs int64  `json:"reconnect_ms"`
	GapClosedMs int64  `json:"gap_closed_ms"`
}

// MaxUnconfirmedFraction is the harness-health ceiling: the share of issued
// publishes that may go unacknowledged before the run fails as a HARNESS fault.
// The zero-loss verdict spans only acknowledged messages, so a large unconfirmed
// share would shrink the claim's denominator silently; 1% keeps the verdict
// covering effectively the whole offered load while tolerating stray 429s at
// burst edges.
const MaxUnconfirmedFraction = 0.01

// HarnessSound reports whether the harness delivered enough of its offered load
// for the checker's verdict to mean anything, with a human-readable reason when
// it did not. Zero confirmed publishes always fails — a run that published
// nothing proves nothing, no matter how green its checker is.
func HarnessSound(confirmed, unconfirmed int) (bool, string) {
	if confirmed == 0 {
		return false, "no publish was ever acknowledged — the run offered no load, so the verdict is vacuous"
	}
	frac := float64(unconfirmed) / float64(confirmed+unconfirmed)
	if frac > MaxUnconfirmedFraction {
		return false, fmt.Sprintf("%.1f%% of issued publishes went unacknowledged (max %.0f%%) — harness fault (pacing or admission), not a delivery verdict", frac*100, MaxUnconfirmedFraction*100)
	}
	return true, ""
}

// Result is the committed per-run artifact.
type Result struct {
	RunID    string  `json:"run_id"`
	Scenario string  `json:"scenario"`
	Pass     bool    `json:"pass"`
	Latency  Latency `json:"latency"`
	Holes    int     `json:"holes"`

	// Publish accounting: the loss verdict covers Confirmed only, so these make
	// its span visible instead of hidden. HarnessFault names why a run failed
	// on harness health rather than on delivery.
	ConfirmedPublishes   int    `json:"confirmed_publishes"`
	UnconfirmedPublishes int    `json:"unconfirmed_publishes"`
	HarnessFault         string `json:"harness_fault,omitempty"`

	// Warmup is the scenario's declared warmup; records intended inside it are
	// excluded from Latency and counted in WarmupExcluded. The zero-loss verdict
	// is NOT windowed — Holes covers the whole run.
	Warmup         string `json:"warmup,omitempty"`
	WarmupExcluded int    `json:"warmup_excluded,omitempty"`

	// HolesSample locates up to the first holesSampleMax holes ("sub/channel seqs a-b")
	// so a failing artifact carries its own evidence — a bare count cannot be triaged.
	HolesSample []string `json:"holes_sample,omitempty"`

	// Phantoms (seq above published — impossible delivery) and Misrouted
	// (record on an unsubscribed channel) are integrity violations; samples
	// mirror HolesSample so a failing artifact carries its own evidence.
	Phantoms        int      `json:"phantoms,omitempty"`
	Misrouted       int      `json:"misrouted,omitempty"`
	PhantomsSample  []string `json:"phantoms_sample,omitempty"`
	MisroutedSample []string `json:"misrouted_sample,omitempty"`

	// Events aggregates every subscriber recovery event by kind (resume_gap,
	// replay_requested, replay_completed, replay_truncated, …) across the run;
	// Subscribers carries the per-subscriber rows the aggregate is built from,
	// so replay outcomes can be partitioned and correlated with holes.
	// RecoveryFault names why the run failed on recovery quality (truncated
	// replays) even when the seq ledger closed — the recovery analogue of
	// HarnessFault.
	Events        map[string]int       `json:"events,omitempty"`
	Subscribers   []SubscriberActivity `json:"subscribers,omitempty"`
	RecoveryFault string               `json:"recovery_fault,omitempty"`

	Recovery []RecoveryStat `json:"recovery,omitempty"`
}

// durations serialize as human-readable strings ("9ms"), not raw nanoseconds,
// so committed results are readable in review.
func (l Latency) MarshalJSON() ([]byte, error) {
	type alias struct {
		Count    int    `json:"count"`
		Negative int    `json:"negative"`
		P50      string `json:"p50"`
		P99      string `json:"p99"`
		P999     string `json:"p999"`
		Max      string `json:"max"`
	}
	return json.Marshal(alias{
		Count: l.Count, Negative: l.Negative,
		P50: l.P50.String(), P99: l.P99.String(), P999: l.P999.String(), Max: l.Max.String(),
	})
}

// Write emits the result as indented JSON.
func Write(w io.Writer, r Result) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}
