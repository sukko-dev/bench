// Package wire defines the bench message payload: the fields the checker and
// latency analysis need, padded to a target size so payload weight is a
// disclosed workload parameter rather than an accident of JSON field lengths.
package wire

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Payload is what the publisher puts in every message's data field.
type Payload struct {
	Run              string `json:"bench_run"` // run ID — the checker ignores foreign runs
	Channel          string `json:"bench_ch"`  // publisher-side channel (defense against misrouting)
	Seq              uint64 `json:"bench_seq"` // per-channel publisher sequence, 1-based
	IntendedUnixNano int64  `json:"bench_t"`   // open-loop intended send time (latency baseline)
	Pad              string `json:"bench_pad,omitempty"`
}

// Encode marshals p padded to exactly size bytes.
func Encode(p Payload, size int) ([]byte, error) {
	p.Pad = ""
	base, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("encode payload: %w", err)
	}
	// Adding the pad field costs len(`,"bench_pad":""`) plus the pad itself.
	const padOverhead = len(`,"bench_pad":""`)
	need := size - len(base) - padOverhead
	if need < 0 {
		return nil, fmt.Errorf("payload needs %d bytes, target size is %d", len(base)+padOverhead, size)
	}
	p.Pad = strings.Repeat("x", need)
	out, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("encode padded payload: %w", err)
	}
	if len(out) != size {
		return nil, fmt.Errorf("padded payload is %d bytes, want %d", len(out), size)
	}
	return out, nil
}

// Decode parses a payload and rejects anything that is not a bench message —
// the checker must never count foreign traffic.
func Decode(raw []byte) (Payload, error) {
	var p Payload
	if err := json.Unmarshal(raw, &p); err != nil {
		return Payload{}, fmt.Errorf("decode payload: %w", err)
	}
	if p.Run == "" || p.Channel == "" || p.Seq == 0 || p.IntendedUnixNano == 0 {
		return Payload{}, errors.New("decode payload: missing bench fields")
	}
	p.Pad = ""
	return p, nil
}
