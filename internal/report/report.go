// Package report turns a run's receive logs and verdict into the committed
// result artifact: an end-to-end latency distribution (arrival − intended,
// the coordinated-omission-safe measure) and a JSON result with the checker
// verdict and recovery stats.
package report

import (
	"encoding/json"
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

// Result is the committed per-run artifact.
type Result struct {
	RunID    string         `json:"run_id"`
	Scenario string         `json:"scenario"`
	Pass     bool           `json:"pass"`
	Latency  Latency        `json:"latency"`
	Holes    int            `json:"holes"`
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
