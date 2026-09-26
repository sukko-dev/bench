package report

import (
	"encoding/json"
	"testing"
	"time"
)

// TestLatencyRoundTrip pins the MarshalJSON/UnmarshalJSON symmetry: the fault-matrix runner reads
// result.json back into a Result, so the string-duration on-disk form must decode to the same
// durations it was written from.
func TestLatencyRoundTrip(t *testing.T) {
	in := Latency{
		Count: 3900, Negative: 2,
		P50:  28*time.Millisecond + 500*time.Microsecond,
		P99:  50 * time.Millisecond,
		P999: 56*time.Millisecond + 500*time.Microsecond,
		Max:  120 * time.Millisecond,
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out Latency
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out != in {
		t.Errorf("round-trip mismatch:\n in=%+v\nout=%+v\njson=%s", in, out, b)
	}
}

// TestLatencyUnmarshalEmpty: absent/empty duration fields decode to 0, not an error.
func TestLatencyUnmarshalEmpty(t *testing.T) {
	var out Latency
	if err := json.Unmarshal([]byte(`{"count":0,"negative":0}`), &out); err != nil {
		t.Fatalf("unmarshal empty: %v", err)
	}
	if out.P50 != 0 || out.P99 != 0 || out.Max != 0 {
		t.Errorf("empty latency did not decode to zeros: %+v", out)
	}
}
