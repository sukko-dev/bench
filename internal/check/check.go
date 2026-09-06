// Package check proves (or refutes) the zero-loss claim from the full receive
// history — the Jepsen move: completeness is verified from data, not asserted.
//
// Invariants, per subscriber × subscribed channel:
//   - after mid-dedupe, the distinct seqs received are exactly 1..Published[ch]
//     (a gap anywhere, including the tail, is a Hole → FAIL);
//   - a seq above Published[ch] is a Phantom → FAIL (it would mean the
//     checker's own publisher accounting is untrustworthy);
//   - a record on a channel the subscriber never subscribed to is Misrouted →
//     FAIL (cross-channel delivery).
//
// Counted but never failing (at-least-once semantics, disclosed):
//   - RedeliveredMids: the same message (same mid) delivered more than once;
//   - DuplicatePublishes: the same seq under different mids (publisher retry
//     after an ambiguous failure).
package check

import "github.com/sukko-dev/bench/internal/rlog"

// Manifest is the publisher's account of what was published.
type Manifest struct {
	Run       string
	Published map[string]uint64 // channel → final seq (1-based count)
}

// SubscriberLog is one subscriber's full receive history plus its subscription set.
type SubscriberLog struct {
	ID       string
	Channels []string
	Records  []rlog.Record
}

// Hole is a contiguous range of published-but-never-received seqs.
type Hole struct {
	Subscriber string
	Channel    string
	FromSeq    uint64
	ToSeq      uint64
}

// Delivery locates a single offending record.
type Delivery struct {
	Subscriber string
	Channel    string
	Seq        uint64
}

// Report is the checker's verdict and evidence.
type Report struct {
	Delivered          int // records counted after mid-dedupe
	RedeliveredMids    int
	DuplicatePublishes int
	Holes              []Hole
	Phantoms           []Delivery
	Misrouted          []Delivery
}

// Pass reports whether the run satisfies the zero-loss invariants.
func (r Report) Pass() bool {
	return len(r.Holes) == 0 && len(r.Phantoms) == 0 && len(r.Misrouted) == 0
}

// Run checks every subscriber log against the manifest.
func Run(m Manifest, subs []SubscriberLog) Report {
	var rep Report
	for _, sub := range subs {
		subscribed := make(map[string]bool, len(sub.Channels))
		for _, ch := range sub.Channels {
			subscribed[ch] = true
		}

		// Per channel: mids seen (redelivery dedupe) and seqs seen (coverage).
		seenMid := make(map[string]map[string]bool)
		seenSeq := make(map[string]map[uint64]bool)
		for _, rec := range sub.Records {
			if !subscribed[rec.Channel] {
				rep.Misrouted = append(rep.Misrouted, Delivery{sub.ID, rec.Channel, rec.Seq})
				continue
			}
			if seenMid[rec.Channel] == nil {
				seenMid[rec.Channel] = make(map[string]bool)
				seenSeq[rec.Channel] = make(map[uint64]bool)
			}
			if seenMid[rec.Channel][rec.Mid] {
				rep.RedeliveredMids++
				continue
			}
			seenMid[rec.Channel][rec.Mid] = true
			rep.Delivered++

			if rec.Seq > m.Published[rec.Channel] {
				rep.Phantoms = append(rep.Phantoms, Delivery{sub.ID, rec.Channel, rec.Seq})
				continue
			}
			if seenSeq[rec.Channel][rec.Seq] {
				rep.DuplicatePublishes++
				continue
			}
			seenSeq[rec.Channel][rec.Seq] = true
		}

		// Coverage: every subscribed channel must have received 1..Published[ch].
		for _, ch := range sub.Channels {
			total := m.Published[ch]
			var holeStart uint64
			inHole := false
			for s := uint64(1); s <= total; s++ {
				missing := !seenSeq[ch][s]
				switch {
				case missing && !inHole:
					holeStart, inHole = s, true
				case !missing && inHole:
					rep.Holes = append(rep.Holes, Hole{sub.ID, ch, holeStart, s - 1})
					inHole = false
				}
			}
			if inHole {
				rep.Holes = append(rep.Holes, Hole{sub.ID, ch, holeStart, total})
			}
		}
	}
	return rep
}
