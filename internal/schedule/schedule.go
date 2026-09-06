// Package schedule computes open-loop intended send times.
//
// The schedule is pure arithmetic over a piecewise-constant rate profile
// (baseline rate plus burst windows). Intended times are fixed by the plan
// alone — never by how long a send actually took — which is what makes the
// driver coordinated-omission-safe (Gil Tene / wrk2): a slow send makes the
// publisher late relative to the schedule, and the lateness lands in the
// recorded latency instead of silently shifting every later message.
package schedule

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// Burst is a window during which the channel's rate is Multiplier × baseline.
type Burst struct {
	Start      time.Duration // offset from schedule start
	Duration   time.Duration
	Multiplier float64
}

// segment is one piecewise-constant span of the rate profile.
type segment struct {
	start time.Duration // inclusive
	rate  float64       // messages per second within the span
	mass  float64       // cumulative messages scheduled before start
}

// Schedule yields intended send offsets for one channel.
type Schedule struct {
	segments []segment // covers [0, ∞); last segment is unbounded
}

// New builds a schedule from a baseline rate (msg/s) and burst windows.
// Invalid plans (non-positive rate or multiplier, non-positive burst duration,
// overlapping bursts) panic: a benchmark must fail loudly at construction, not
// publish numbers from a plan it silently repaired.
func New(rate float64, bursts []Burst) *Schedule {
	if rate <= 0 {
		panic(fmt.Sprintf("schedule: baseline rate must be positive, got %v", rate))
	}
	sorted := make([]Burst, len(bursts))
	copy(sorted, bursts)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Start < sorted[j].Start })

	var segs []segment
	cursor := time.Duration(0)
	for _, b := range sorted {
		if b.Multiplier <= 0 || b.Duration <= 0 || b.Start < 0 {
			panic(fmt.Sprintf("schedule: invalid burst %+v", b))
		}
		if b.Start < cursor {
			panic(fmt.Sprintf("schedule: burst starting at %v overlaps the previous window", b.Start))
		}
		if b.Start > cursor {
			segs = append(segs, segment{start: cursor, rate: rate})
		}
		segs = append(segs, segment{start: b.Start, rate: rate * b.Multiplier})
		cursor = b.Start + b.Duration
	}
	segs = append(segs, segment{start: cursor, rate: rate}) // unbounded tail

	// Precompute cumulative message mass at each segment boundary.
	for i := 1; i < len(segs); i++ {
		span := segs[i].start - segs[i-1].start
		segs[i].mass = segs[i-1].mass + segs[i-1].rate*span.Seconds()
	}
	return &Schedule{segments: segs}
}

// IntendedOffset returns when message i (0-based) is scheduled, as an offset
// from the schedule start: the time at which cumulative scheduled mass
// reaches i.
func (s *Schedule) IntendedOffset(i int) time.Duration {
	target := float64(i)
	seg := s.segments[len(s.segments)-1]
	for j := 1; j < len(s.segments); j++ {
		if s.segments[j].mass > target {
			seg = s.segments[j-1]
			break
		}
	}
	secs := (target - seg.mass) / seg.rate
	return seg.start + time.Duration(math.Round(secs*float64(time.Second)))
}

// CountThrough returns how many messages are scheduled strictly before offset d.
func (s *Schedule) CountThrough(d time.Duration) int {
	seg := s.segments[len(s.segments)-1]
	for j := 1; j < len(s.segments); j++ {
		if s.segments[j].start > d {
			seg = s.segments[j-1]
			break
		}
	}
	mass := seg.mass + seg.rate*(d-seg.start).Seconds()
	return int(math.Ceil(mass))
}
