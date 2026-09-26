package matrix

import (
	"testing"
	"time"
)

func TestFaultDelay(t *testing.T) {
	// odds-burst: 180s run, second burst at 0.66, land 1s in → 118.8s + 1s = 119.8s.
	got := FaultDelay(180*time.Second, 0.66, time.Second)
	want := 118800*time.Millisecond + time.Second
	if got != want {
		t.Errorf("FaultDelay = %v, want %v", got, want)
	}
	// zero offset → exactly the burst start.
	if got := FaultDelay(100*time.Second, 0.5, 0); got != 50*time.Second {
		t.Errorf("FaultDelay zero-offset = %v, want 50s", got)
	}
}

func TestFaultLandsInBurst(t *testing.T) {
	// Burst window [50s, 53s) — a 100s run, second burst at 0.5, 3s wide.
	const start, dur = 50 * time.Second, 3 * time.Second
	cases := []struct {
		name  string
		delay time.Duration
		want  bool
	}{
		{"at burst start (offset 0)", 50 * time.Second, true},
		{"1s into the window (default offset)", 51 * time.Second, true},
		{"just inside the far edge", start + dur - time.Nanosecond, true},
		{"exactly at the far edge is out", start + dur, false},
		{"after the window", 55 * time.Second, false},
		{"before the window", 49 * time.Second, false},
		{"after the run ends entirely", 200 * time.Second, false},
	}
	for _, c := range cases {
		if got := FaultLandsInBurst(c.delay, start, dur); got != c.want {
			t.Errorf("%s: FaultLandsInBurst(%v, %v, %v) = %v, want %v", c.name, c.delay, start, dur, got, c.want)
		}
	}
}
