package scenario

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sukko-dev/bench/internal/check"
	"github.com/sukko-dev/bench/internal/pub"
	"github.com/sukko-dev/bench/internal/rlog"
	"github.com/sukko-dev/bench/internal/sub"
)

// fakeSub stands in for a live subscriber: it records the channels it was asked
// to serve and returns a scripted receive log at Stop.
type fakeSub struct {
	id       string
	channels []string
	records  []rlog.Record
	events   []sub.Event
	stopped  bool
	onStop   func()
}

func (f *fakeSub) Stop() error {
	f.stopped = true
	if f.onStop != nil {
		f.onStop()
	}
	return nil
}
func (f *fakeSub) Events() []sub.Event             { return f.events }
func (f *fakeSub) Records() ([]rlog.Record, error) { return f.records, nil }
func (f *fakeSub) ID() string                      { return f.id }
func (f *fakeSub) Channels() []string              { return f.channels }

func cfg() Config {
	return Config{
		RunID: "r1", URL: "ws://x", Token: "t",
		Channels: []string{"t.a", "t.b"}, SubsPerChannel: 2,
		BaselineRate: 2.0, Duration: 2 * time.Second, PayloadSize: 200,
		T0: time.Unix(1_757_000_000, 0),
	}
}

func fullRecords(run, ch string, n uint64) []rlog.Record {
	var rs []rlog.Record
	for s := uint64(1); s <= n; s++ {
		rs = append(rs, rlog.Record{Channel: ch, Seq: s, Mid: run + ch + string(rune('0'+s)), IntendedUnixNano: int64(s), ArrivalUnixNano: int64(s) + 5})
	}
	return rs
}

func TestRunStartsSubscriberPerChannelSlotAndPasses(t *testing.T) {
	var started []*fakeSub
	deps := Deps{
		StartSub: func(ctx context.Context, id string, channels []string) (Subscriber, error) {
			fs := &fakeSub{id: id, channels: channels}
			for _, ch := range channels {
				fs.records = append(fs.records, fullRecords("r1", ch, 4)...)
			}
			started = append(started, fs)
			return fs, nil
		},
		RunPub: func(ctx context.Context, plans []pub.ChannelPlan) (pub.Manifest, error) {
			m := pub.Manifest{Run: "r1", Published: map[string]uint64{}}
			for _, p := range plans {
				m.Published[p.Channel] = 4
			}
			return m, nil
		},
	}
	res, err := Run(context.Background(), cfg(), deps)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 2 channels × 2 subs/channel = 4 subscribers, each on exactly ONE channel.
	if len(started) != 4 {
		t.Fatalf("started %d subscribers, want 4", len(started))
	}
	perChannel := map[string]int{}
	for _, s := range started {
		if len(s.channels) != 1 {
			t.Errorf("sub %s subscribed to %v, want exactly one channel", s.id, s.channels)
		} else {
			perChannel[s.channels[0]]++
		}
		if !s.stopped {
			t.Errorf("sub %s not stopped", s.id)
		}
	}
	for _, ch := range []string{"t.a", "t.b"} {
		if perChannel[ch] != 2 {
			t.Errorf("channel %s has %d subscribers, want 2", ch, perChannel[ch])
		}
	}
	if !res.Check.Pass() {
		t.Fatalf("checker failed a clean run: %+v", res.Check)
	}
	if res.Manifest.Published["t.a"] != 4 {
		t.Errorf("manifest not captured: %+v", res.Manifest)
	}
}

func TestRunSurfacesCheckerFailureWithoutErroring(t *testing.T) {
	// A lost message is a run RESULT (Check.Pass() == false), not an
	// operational error — the artifact publishes failed runs.
	deps := Deps{
		StartSub: func(ctx context.Context, id string, channels []string) (Subscriber, error) {
			fs := &fakeSub{id: id, channels: channels}
			for _, ch := range channels {
				recs := fullRecords("r1", ch, 4)
				fs.records = append(fs.records, recs[:2]...) // drop seqs 3,4
			}
			return fs, nil
		},
		RunPub: func(ctx context.Context, plans []pub.ChannelPlan) (pub.Manifest, error) {
			m := pub.Manifest{Run: "r1", Published: map[string]uint64{}}
			for _, p := range plans {
				m.Published[p.Channel] = 4
			}
			return m, nil
		},
	}
	res, err := Run(context.Background(), cfg(), deps)
	if err != nil {
		t.Fatalf("Run errored on a lossy run (should be a verdict, not an error): %v", err)
	}
	if res.Check.Pass() {
		t.Fatal("checker passed a run missing messages")
	}
	if len(res.Check.Holes) == 0 {
		t.Error("holes not reported")
	}
}

func TestUnconfirmedPublishesRelaxCheckerExpectations(t *testing.T) {
	// A publish the publisher never confirmed is not expected at subscribers;
	// its seq must be excluded from the manifest the checker uses, else every
	// run with a single publish failure would false-FAIL.
	deps := Deps{
		StartSub: func(ctx context.Context, id string, channels []string) (Subscriber, error) {
			fs := &fakeSub{id: id, channels: channels}
			for _, ch := range channels {
				recs := fullRecords("r1", ch, 4)
				fs.records = append(fs.records, recs[:3]...) // seq 4 never arrived
			}
			return fs, nil
		},
		RunPub: func(ctx context.Context, plans []pub.ChannelPlan) (pub.Manifest, error) {
			m := pub.Manifest{Run: "r1", Published: map[string]uint64{}, Unconfirmed: map[string][]uint64{}}
			for _, p := range plans {
				m.Published[p.Channel] = 4
				m.Unconfirmed[p.Channel] = []uint64{4} // publisher gave up on seq 4
			}
			return m, nil
		},
	}
	res, err := Run(context.Background(), cfg(), deps)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Check.Pass() {
		t.Fatalf("unconfirmed publish false-failed the run: %+v", res.Check)
	}
}

func TestSubscriberStartFailureAbortsRun(t *testing.T) {
	deps := Deps{
		StartSub: func(ctx context.Context, id string, channels []string) (Subscriber, error) {
			return nil, errors.New("dial refused")
		},
		RunPub: func(ctx context.Context, plans []pub.ChannelPlan) (pub.Manifest, error) {
			t.Fatal("publisher must not run if subscribers did not come up")
			return pub.Manifest{}, nil
		},
	}
	if _, err := Run(context.Background(), cfg(), deps); err == nil {
		t.Fatal("Run ignored a subscriber that failed to start")
	}
}

func TestChannelPlansShareBurstProfile(t *testing.T) {
	c := cfg()
	c.Bursts = []Burst{{StartFraction: 0.5, Duration: time.Second, Multiplier: 8}}
	var gotPlans []pub.ChannelPlan
	deps := Deps{
		StartSub: func(ctx context.Context, id string, channels []string) (Subscriber, error) {
			return &fakeSub{id: id, channels: channels}, nil
		},
		RunPub: func(ctx context.Context, plans []pub.ChannelPlan) (pub.Manifest, error) {
			gotPlans = plans
			return pub.Manifest{Run: "r1", Published: map[string]uint64{}}, nil
		},
	}
	if _, err := Run(context.Background(), c, deps); err != nil {
		t.Fatal(err)
	}
	if len(gotPlans) != 2 {
		t.Fatalf("plans = %d, want one per channel", len(gotPlans))
	}
	// Each plan's schedule includes the burst: count through the window exceeds
	// the flat-rate count.
	flat := int(c.BaselineRate * c.Duration.Seconds())
	for _, p := range gotPlans {
		if got := p.Schedule.CountThrough(c.Duration); got <= flat {
			t.Errorf("plan %s count %d does not reflect the burst (flat=%d)", p.Channel, got, flat)
		}
	}
}

var _ Subscriber = (*fakeSub)(nil)
var _ = check.Report{}

// TestDrainRunsBetweenPublisherAndStop pins the teardown ordering that closes the
// final-message race: the last publish is CONFIRMED at the gateway while its fan-out is
// still in flight, so stopping subscribers immediately after RunPub loses the tail seq
// (observed live as every channel's final seq missing at every subscriber) and convicts
// the platform of losing a message the harness refused to wait for.
func TestDrainRunsBetweenPublisherAndStop(t *testing.T) {
	var order []string
	var mu sync.Mutex
	note := func(ev string) {
		mu.Lock()
		order = append(order, ev)
		mu.Unlock()
	}

	deps := Deps{
		StartSub: func(_ context.Context, id string, channels []string) (Subscriber, error) {
			return &fakeSub{id: id, channels: channels, onStop: func() { note("stop") }}, nil
		},
		RunPub: func(context.Context, []pub.ChannelPlan) (pub.Manifest, error) {
			note("pub-done")
			return pub.Manifest{Published: map[string]uint64{}}, nil
		},
		Drain: func(context.Context) { note("drain") },
	}
	_, err := Run(context.Background(), Config{
		RunID: "r1", Channels: []string{"t.a"}, SubsPerChannel: 2,
		BaselineRate: 1, Duration: time.Second, PayloadSize: 100, T0: time.Unix(0, 0),
	}, deps)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{"pub-done", "drain", "stop", "stop"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v (drain must sit between publisher completion and every Stop)", order, want)
		}
	}
}
