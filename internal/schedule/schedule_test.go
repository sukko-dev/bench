package schedule

import (
	"testing"
	"time"
)

// The schedule is pure math: given a baseline rate and burst windows, it yields
// the intended send time of every message on a channel. Open-loop correctness
// (wrk2 / coordinated-omission safety) depends on intended times being fixed by
// the plan alone — never by how long a send actually took.

func TestConstantRateSpacing(t *testing.T) {
	s := New(2.0, nil) // 2 msg/s, no bursts
	tests := []struct {
		i    int
		want time.Duration
	}{
		{0, 0},
		{1, 500 * time.Millisecond},
		{2, 1 * time.Second},
		{10, 5 * time.Second},
	}
	for _, tt := range tests {
		if got := s.IntendedOffset(tt.i); got != tt.want {
			t.Errorf("IntendedOffset(%d) = %v, want %v", tt.i, got, tt.want)
		}
	}
}

func TestBurstWindowCompressesSpacing(t *testing.T) {
	// Baseline 2 msg/s; from t=2s to t=4s the rate is ×4 (8 msg/s).
	s := New(2.0, []Burst{{Start: 2 * time.Second, Duration: 2 * time.Second, Multiplier: 4}})
	// First 4 messages cover [0s, 2s) at 500ms spacing.
	if got := s.IntendedOffset(4); got != 2*time.Second {
		t.Fatalf("message 4 must land at burst start: got %v", got)
	}
	// Inside the burst: 125ms spacing.
	if got := s.IntendedOffset(5); got != 2*time.Second+125*time.Millisecond {
		t.Errorf("message 5 = %v, want 2.125s", got)
	}
	// The burst window [2s,4s) at 8 msg/s carries 16 messages (indices 4..19);
	// index 20 is the first after the burst, back at 500ms spacing.
	if got := s.IntendedOffset(20); got != 4*time.Second {
		t.Errorf("message 20 = %v, want 4s (burst exit)", got)
	}
	if got := s.IntendedOffset(21); got != 4*time.Second+500*time.Millisecond {
		t.Errorf("message 21 = %v, want 4.5s", got)
	}
}

func TestIntendedTimesAreMonotonic(t *testing.T) {
	s := New(3.0, []Burst{
		{Start: 1 * time.Second, Duration: 500 * time.Millisecond, Multiplier: 8},
		{Start: 5 * time.Second, Duration: 3 * time.Second, Multiplier: 2},
	})
	prev := time.Duration(-1)
	for i := range 200 {
		got := s.IntendedOffset(i)
		if got <= prev {
			t.Fatalf("IntendedOffset(%d) = %v not after previous %v", i, got, prev)
		}
		prev = got
	}
}

func TestCountThrough(t *testing.T) {
	// 2 msg/s baseline with a ×4 burst of 2s: 10s of schedule =
	// 8s baseline (16 msgs) + 2s burst (16 msgs) = 32 messages.
	s := New(2.0, []Burst{{Start: 2 * time.Second, Duration: 2 * time.Second, Multiplier: 4}})
	if got := s.CountThrough(10 * time.Second); got != 32 {
		t.Errorf("CountThrough(10s) = %d, want 32", got)
	}
}

func TestRejectsInvalidPlan(t *testing.T) {
	tests := []struct {
		name   string
		rate   float64
		bursts []Burst
	}{
		{"zero rate", 0, nil},
		{"negative rate", -1, nil},
		{"zero multiplier", 2, []Burst{{Start: time.Second, Duration: time.Second, Multiplier: 0}}},
		{"overlapping bursts", 2, []Burst{
			{Start: 1 * time.Second, Duration: 2 * time.Second, Multiplier: 2},
			{Start: 2 * time.Second, Duration: 1 * time.Second, Multiplier: 3},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("New accepted an invalid plan; the schedule must fail loudly at construction")
				}
			}()
			New(tt.rate, tt.bursts)
		})
	}
}
