package report

import (
	"encoding/json"
	"testing"
	"time"
)

// TestNewDriverCPU pins the driver-honesty metric (METHODOLOGY §2: driver CPU
// is measured and reported per run so the load generator cannot silently be the
// bottleneck). PercentWall is (user+sys)/wall × 100; wall==0 must not divide.
func TestNewDriverCPU(t *testing.T) {
	tests := []struct {
		name                     string
		user, sys, wall          time.Duration
		wantUser, wantSys, wantP float64
	}{
		{"quarter of one core", time.Second, 500 * time.Millisecond, 10 * time.Second, 1.0, 0.5, 15.0},
		{"fully busy one core", 3 * time.Second, time.Second, 4 * time.Second, 3.0, 1.0, 100.0},
		{"multi-core >100%", 6 * time.Second, 2 * time.Second, 4 * time.Second, 6.0, 2.0, 200.0},
		{"zero wall no divide", time.Second, 0, 0, 1.0, 0.0, 0.0},
		{"idle", 0, 0, 5 * time.Second, 0.0, 0.0, 0.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDriverCPU(tt.user, tt.sys, tt.wall)
			if d.UserSeconds != tt.wantUser {
				t.Errorf("UserSeconds = %v, want %v", d.UserSeconds, tt.wantUser)
			}
			if d.SysSeconds != tt.wantSys {
				t.Errorf("SysSeconds = %v, want %v", d.SysSeconds, tt.wantSys)
			}
			if d.PercentWall != tt.wantP {
				t.Errorf("PercentWall = %v, want %v", d.PercentWall, tt.wantP)
			}
		})
	}
}

// TestDriverCPU_SerializesOnResult asserts the field is present in the committed
// artifact (the whole point — a run's driver load must be visible in result.json).
func TestDriverCPU_SerializesOnResult(t *testing.T) {
	r := Result{DriverCPU: NewDriverCPU(2*time.Second, time.Second, 6*time.Second)}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back map[string]any
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	dc, ok := back["driver_cpu"].(map[string]any)
	if !ok {
		t.Fatalf("driver_cpu missing or wrong type in result JSON: %s", b)
	}
	for _, k := range []string{"user_s", "sys_s", "percent_wall"} {
		if _, ok := dc[k]; !ok {
			t.Errorf("driver_cpu.%s missing in %v", k, dc)
		}
	}
}
