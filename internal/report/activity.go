package report

import (
	"fmt"
	"strings"
	"time"

	"github.com/sukko-dev/bench/internal/check"
	"github.com/sukko-dev/bench/internal/sub"
)

// Per-subscriber detail caps: the counts (Events, Holes) are NEVER capped —
// only the human-readable logs are, so the artifact stays bounded (worst case
// ~subscribers × eventLogMax short lines) while every number stays exact.
const (
	eventLogMax    = 20
	holesDetailMax = 5
)

// SubEvents is one subscriber's identity and recovery events, as handed over
// by the scenario runner.
type SubEvents struct {
	ID     string
	Events []sub.Event
}

// SubscriberActivity is one subscriber's row in the artifact: its recovery
// events and its holes side by side, so a triage can partition subscribers by
// replay outcome (e.g. replay_completed vs replay_unrecoverable) and compare
// hole patterns across the groups directly from result.json.
type SubscriberActivity struct {
	ID            string         `json:"id"`
	Events        map[string]int `json:"events,omitempty"`         // event kind → count (uncapped)
	EventLog      []string       `json:"event_log,omitempty"`      // "+<offset>s <kind> [channel] [detail]", first eventLogMax
	EventsOmitted int            `json:"events_omitted,omitempty"` // events beyond the EventLog cap
	Holes         int            `json:"holes,omitempty"`          // this subscriber's hole count (uncapped)
	HolesDetail   []string       `json:"holes_detail,omitempty"`   // "channel seqs a-b", first holesDetailMax
	HolesOmitted  int            `json:"holes_omitted,omitempty"`  // holes beyond the HolesDetail cap
}

// BuildActivity aggregates recovery events into run-wide counts by kind and
// per-subscriber rows. Rows exist only for subscribers with at least one
// event or one hole — a healthy run contributes nothing — and keep the
// runner's deterministic input order. Event log offsets are relative to t0 so
// recovery timing can be correlated with hole timing.
func BuildActivity(subs []SubEvents, holes []check.Hole, t0 time.Time) (map[string]int, []SubscriberActivity) {
	holesBySub := make(map[string][]check.Hole)
	for _, h := range holes {
		holesBySub[h.Subscriber] = append(holesBySub[h.Subscriber], h)
	}

	var counts map[string]int
	var rows []SubscriberActivity
	for _, s := range subs {
		subHoles := holesBySub[s.ID]
		if len(s.Events) == 0 && len(subHoles) == 0 {
			continue
		}
		row := SubscriberActivity{ID: s.ID, Holes: len(subHoles)}
		if len(s.Events) > 0 {
			row.Events = make(map[string]int, 4)
		}
		for i, ev := range s.Events {
			if counts == nil {
				counts = make(map[string]int, 8)
			}
			counts[string(ev.Kind)]++
			row.Events[string(ev.Kind)]++
			if i < eventLogMax {
				row.EventLog = append(row.EventLog, eventLine(ev, t0))
			}
		}
		if n := len(s.Events) - eventLogMax; n > 0 {
			row.EventsOmitted = n
		}
		for i, h := range subHoles {
			if i >= holesDetailMax {
				row.HolesOmitted = len(subHoles) - holesDetailMax
				break
			}
			row.HolesDetail = append(row.HolesDetail, fmt.Sprintf("%s seqs %d-%d", h.Channel, h.FromSeq, h.ToSeq))
		}
		rows = append(rows, row)
	}
	return counts, rows
}

// eventLine renders one event as "+<offset>s <kind> [channel] [detail]".
func eventLine(ev sub.Event, t0 time.Time) string {
	parts := []string{fmt.Sprintf("+%.1fs", ev.At.Sub(t0).Seconds()), string(ev.Kind)}
	if ev.Channel != "" {
		parts = append(parts, ev.Channel)
	}
	if ev.Detail != "" {
		parts = append(parts, ev.Detail)
	}
	return strings.Join(parts, " ")
}

// RecoverySound reports whether the run's recovery behavior supports a clean
// verdict, with a human-readable reason when it does not. A truncated replay
// means the hole was only PARTLY filled — the run is not a clean-recovery run
// even if the seq ledger happens to close, the same vacuous-green class
// HarnessSound exists to kill. Unrecoverable replays are deliberately not a
// fault here: the messages they failed to recover are real holes the checker
// already fails on.
func RecoverySound(events map[string]int) (bool, string) {
	if n := events[string(sub.EventReplayTruncated)]; n > 0 {
		return false, fmt.Sprintf("%d replay(s) completed truncated — recovery fell short, so holes may under-report loss; not a clean-recovery run", n)
	}
	return true, ""
}
