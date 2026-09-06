package wire

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPayloadRoundTrip(t *testing.T) {
	p := Payload{Run: "r1", Channel: "acme.md-042", Seq: 981, IntendedUnixNano: 1757000000123456789}
	raw, err := Encode(p, 300)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != p {
		t.Errorf("round trip = %+v, want %+v", got, p)
	}
}

func TestPayloadPaddedToExactSize(t *testing.T) {
	for _, size := range []int{200, 300, 1024} {
		raw, err := Encode(Payload{Run: "r", Channel: "t.c", Seq: 1, IntendedUnixNano: 1}, size)
		if err != nil {
			t.Fatalf("Encode(size=%d): %v", size, err)
		}
		if len(raw) != size {
			t.Errorf("size %d: got %d bytes", size, len(raw))
		}
		if !json.Valid(raw) {
			t.Errorf("size %d: padded payload is not valid JSON", size)
		}
	}
}

func TestPayloadSizeTooSmallErrors(t *testing.T) {
	p := Payload{Run: "run-id", Channel: strings.Repeat("c", 50), Seq: 1 << 60, IntendedUnixNano: 1 << 62}
	if _, err := Encode(p, 40); err == nil {
		t.Fatal("Encode accepted a target size smaller than the payload itself")
	}
}

func TestDecodeRejectsForeignPayload(t *testing.T) {
	if _, err := Decode([]byte(`{"price": 1.91}`)); err == nil {
		t.Fatal("Decode accepted a payload without bench fields — the checker must never count foreign messages")
	}
}
