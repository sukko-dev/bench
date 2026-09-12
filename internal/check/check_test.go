package check

import (
	"fmt"
	"testing"

	"github.com/sukko-dev/bench/internal/rlog"
)

// buildLog returns records for one subscriber that received channel ch
// completely: seqs 1..n, each under a unique mid.
func completeLog(ch string, n uint64) []rlog.Record {
	var rs []rlog.Record
	for s := uint64(1); s <= n; s++ {
		rs = append(rs, rlog.Record{
			ArrivalUnixNano: int64(s) * 1000, IntendedUnixNano: int64(s) * 900,
			Channel: ch, Seq: s, Mid: fmt.Sprintf("%s-mid-%d", ch, s),
		})
	}
	return rs
}

func manifest() Manifest {
	return Manifest{Run: "r1", Published: map[string]uint64{"t.a": 5, "t.b": 3}}
}

func TestCleanRunPasses(t *testing.T) {
	subs := []SubscriberLog{
		{ID: "sub-0", Channels: []string{"t.a", "t.b"}, Records: append(completeLog("t.a", 5), completeLog("t.b", 3)...)},
		{ID: "sub-1", Channels: []string{"t.a"}, Records: completeLog("t.a", 5)},
	}
	rep := Run(manifest(), subs)
	if !rep.Pass() {
		t.Fatalf("clean run failed: %+v", rep)
	}
	if rep.Delivered != 13 {
		t.Errorf("Delivered = %d, want 13", rep.Delivered)
	}
}

func TestHoleFailsAndIsLocated(t *testing.T) {
	recs := completeLog("t.a", 5)
	recs = append(recs[:2], recs[3:]...) // drop seq 3
	rep := Run(manifest(), []SubscriberLog{
		{ID: "sub-0", Channels: []string{"t.a", "t.b"}, Records: append(recs, completeLog("t.b", 3)...)},
	})
	if rep.Pass() {
		t.Fatal("run with a missing message passed")
	}
	if len(rep.Holes) != 1 {
		t.Fatalf("holes = %+v, want exactly one", rep.Holes)
	}
	h := rep.Holes[0]
	if h.Subscriber != "sub-0" || h.Channel != "t.a" || h.FromSeq != 3 || h.ToSeq != 3 {
		t.Errorf("hole = %+v, want sub-0/t.a seq 3", h)
	}
}

func TestRedeliveredMidIsDedupedNotFailed(t *testing.T) {
	// At-least-once: the same message (same mid) delivered twice is counted,
	// never a failure — mid-dedupe is the disclosed client contract.
	recs := completeLog("t.a", 5)
	recs = append(recs, recs[1]) // redeliver seq 2 with the same mid
	rep := Run(manifest(), []SubscriberLog{
		{ID: "sub-0", Channels: []string{"t.a", "t.b"}, Records: append(recs, completeLog("t.b", 3)...)},
	})
	if !rep.Pass() {
		t.Fatalf("redelivery failed the run: %+v", rep)
	}
	if rep.RedeliveredMids != 1 {
		t.Errorf("RedeliveredMids = %d, want 1", rep.RedeliveredMids)
	}
}

func TestSameSeqDifferentMidCountsAsDuplicatePublish(t *testing.T) {
	// A publisher retry after an ambiguous failure creates a second message
	// carrying the same bench seq under a new mid. Completeness holds; the
	// effect is counted separately from transport redelivery.
	recs := completeLog("t.a", 5)
	dup := recs[1]
	dup.Mid = "t.a-mid-2-retry"
	recs = append(recs, dup)
	rep := Run(manifest(), []SubscriberLog{
		{ID: "sub-0", Channels: []string{"t.a", "t.b"}, Records: append(recs, completeLog("t.b", 3)...)},
	})
	if !rep.Pass() {
		t.Fatalf("publish retry failed the run: %+v", rep)
	}
	if rep.DuplicatePublishes != 1 {
		t.Errorf("DuplicatePublishes = %d, want 1", rep.DuplicatePublishes)
	}
}

func TestPhantomSeqFails(t *testing.T) {
	recs := append(completeLog("t.a", 5), rlog.Record{Channel: "t.a", Seq: 6, Mid: "phantom"})
	rep := Run(manifest(), []SubscriberLog{
		{ID: "sub-0", Channels: []string{"t.a", "t.b"}, Records: append(recs, completeLog("t.b", 3)...)},
	})
	if rep.Pass() {
		t.Fatal("a seq beyond the published manifest passed — the checker cannot trust its own publisher accounting")
	}
	if len(rep.Phantoms) != 1 || rep.Phantoms[0].Seq != 6 {
		t.Errorf("Phantoms = %+v, want one at seq 6", rep.Phantoms)
	}
}

func TestUnsubscribedChannelDeliveryFails(t *testing.T) {
	// sub-1 never subscribed to t.b; receiving it is misrouting — the
	// cross-channel isolation failure a delivery benchmark must never shrug at.
	rep := Run(manifest(), []SubscriberLog{
		{ID: "sub-0", Channels: []string{"t.a", "t.b"}, Records: append(completeLog("t.a", 5), completeLog("t.b", 3)...)},
		{ID: "sub-1", Channels: []string{"t.a"}, Records: append(completeLog("t.a", 5), completeLog("t.b", 1)...)},
	})
	if rep.Pass() {
		t.Fatal("misrouted delivery passed")
	}
	if len(rep.Misrouted) != 1 || rep.Misrouted[0].Subscriber != "sub-1" || rep.Misrouted[0].Channel != "t.b" {
		t.Errorf("Misrouted = %+v, want sub-1/t.b", rep.Misrouted)
	}
}

func TestTrailingHoleIsDetected(t *testing.T) {
	// Missing the LAST messages (the classic kill-window loss) must fail:
	// contiguity from 1 is not enough, the range must reach Published[ch].
	rep := Run(manifest(), []SubscriberLog{
		{ID: "sub-0", Channels: []string{"t.a", "t.b"}, Records: append(completeLog("t.a", 3), completeLog("t.b", 3)...)},
	})
	if rep.Pass() {
		t.Fatal("run missing the tail of a channel passed")
	}
	h := rep.Holes[0]
	if h.FromSeq != 4 || h.ToSeq != 5 {
		t.Errorf("trailing hole = %+v, want seqs 4-5", h)
	}
}

// TestUnconfirmedSeqIsNotAHole pins the loss-attribution rule: the checker may claim
// loss only for messages the system ACKNOWLEDGED accepting. Seqs the publisher recorded
// as unconfirmed (e.g. rate-limited at admission) were never accepted, so their absence
// at subscribers is the harness's doing, not the platform's — demanding them convicted
// the platform of "losing" messages it had refused, one hole per subscriber.
func TestUnconfirmedSeqIsNotAHole(t *testing.T) {
	m := Manifest{
		Run:         "r1",
		Published:   map[string]uint64{"t.a": 5},
		Unconfirmed: map[string][]uint64{"t.a": {2, 4}},
	}
	// The subscriber received exactly the confirmed seqs: 1, 3, 5.
	recs := []rlog.Record{}
	for _, s := range []uint64{1, 3, 5} {
		recs = append(recs, rlog.Record{Channel: "t.a", Seq: s, Mid: fmt.Sprintf("m-%d", s)})
	}
	rep := Run(m, []SubscriberLog{{ID: "sub-0", Channels: []string{"t.a"}, Records: recs}})

	if len(rep.Holes) != 0 {
		t.Errorf("Holes = %+v, want none (2 and 4 were never accepted)", rep.Holes)
	}
	if !rep.Pass() {
		t.Errorf("Pass() = false, want true")
	}
	if rep.ConfirmedPublished != 3 {
		t.Errorf("ConfirmedPublished = %d, want 3", rep.ConfirmedPublished)
	}
	if rep.UnconfirmedPublished != 2 {
		t.Errorf("UnconfirmedPublished = %d, want 2", rep.UnconfirmedPublished)
	}
}

// TestUnconfirmedSeqArrivingIsTolerated: "unconfirmed" means the ACK was lost, not
// necessarily the message — a timeout after the gateway processed it is still a real
// publish. Its arrival must not fail the run in either direction.
func TestUnconfirmedSeqArrivingIsTolerated(t *testing.T) {
	m := Manifest{
		Run:         "r1",
		Published:   map[string]uint64{"t.a": 3},
		Unconfirmed: map[string][]uint64{"t.a": {2}},
	}
	rep := Run(m, []SubscriberLog{{ID: "sub-0", Channels: []string{"t.a"}, Records: completeLog("t.a", 3)}})

	if !rep.Pass() {
		t.Errorf("Pass() = false, want true (rep: %+v)", rep)
	}
	if rep.Delivered != 3 {
		t.Errorf("Delivered = %d, want 3 (the ambiguous seq still counts when it arrives)", rep.Delivered)
	}
}

// TestConfirmedButMissingIsStillAHole: the exclusion must be surgical. A seq that IS
// confirmed and does not arrive remains a hole — otherwise the unconfirmed set becomes
// a place to hide real loss.
func TestConfirmedButMissingIsStillAHole(t *testing.T) {
	m := Manifest{
		Run:         "r1",
		Published:   map[string]uint64{"t.a": 3},
		Unconfirmed: map[string][]uint64{"t.a": {2}},
	}
	// Received 1 only — 3 is confirmed and missing.
	rep := Run(m, []SubscriberLog{{ID: "sub-0", Channels: []string{"t.a"},
		Records: []rlog.Record{{Channel: "t.a", Seq: 1, Mid: "m-1"}}}})

	if len(rep.Holes) != 1 || rep.Holes[0].FromSeq != 3 || rep.Holes[0].ToSeq != 3 {
		t.Errorf("Holes = %+v, want exactly the confirmed-and-missing seq 3", rep.Holes)
	}
	if rep.Pass() {
		t.Error("Pass() = true, want false")
	}
}
