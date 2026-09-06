// Package scenario orchestrates one benchmark run: it starts a subscriber per
// channel-slot, runs the open-loop publisher across all channels, stops the
// subscribers, and proves the result with the checker. Provisioning and auth
// are the harness's job (compose seeds a bench tenant); the runner takes a
// gateway URL, a token, and the channel set as inputs.
package scenario

import (
	"context"
	"fmt"
	"time"

	"github.com/sukko-dev/bench/internal/check"
	"github.com/sukko-dev/bench/internal/pub"
	"github.com/sukko-dev/bench/internal/rlog"
	"github.com/sukko-dev/bench/internal/schedule"
	"github.com/sukko-dev/bench/internal/sub"
)

// Subscriber is the run's view of a live subscriber (satisfied by *sub.Subscriber
// through a thin adapter; faked in tests).
type Subscriber interface {
	Stop() error
	Events() []sub.Event
	Records() ([]rlog.Record, error)
	ID() string
	Channels() []string
}

// Deps are the injected seams that touch the network.
type Deps struct {
	StartSub func(ctx context.Context, id string, channels []string) (Subscriber, error)
	RunPub   func(ctx context.Context, plans []pub.ChannelPlan) (pub.Manifest, error)
}

// Burst is a burst window expressed as a fraction of the run duration, so the
// same profile applies regardless of Duration.
type Burst struct {
	StartFraction float64
	Duration      time.Duration
	Multiplier    float64
}

// Config parameterizes one run.
type Config struct {
	RunID          string
	URL            string
	Token          string
	Channels       []string
	SubsPerChannel int
	BaselineRate   float64 // msg/s per channel
	Bursts         []Burst
	Duration       time.Duration
	PayloadSize    int
	T0             time.Time
}

// SubReport is one subscriber's recovery events.
type SubReport struct {
	ID     string
	Events []sub.Event
}

// Result is everything a run produces.
type Result struct {
	Manifest pub.Manifest
	Subs     []SubReport
	Check    check.Report
}

// Run executes the scenario and returns its Result. A checker failure is part
// of the Result (Check.Pass() == false), never an error — the artifact
// publishes failed runs. Operational failures (a subscriber that can't start,
// a publisher that can't run) are errors.
func Run(ctx context.Context, cfg Config, deps Deps) (Result, error) {
	bursts := make([]schedule.Burst, len(cfg.Bursts))
	for i, b := range cfg.Bursts {
		bursts[i] = schedule.Burst{
			Start:      time.Duration(b.StartFraction * float64(cfg.Duration)),
			Duration:   b.Duration,
			Multiplier: b.Multiplier,
		}
	}

	// Odds shape: each channel gets SubsPerChannel observers, each on that one
	// channel (a market-data client watches its markets, not all of them).
	// Total connections = len(Channels) × SubsPerChannel.
	var subs []Subscriber
	stopAll := func() {
		for _, s := range subs {
			_ = s.Stop() // best-effort teardown; the first start error is what we return
		}
	}
	id := 0
	for _, ch := range cfg.Channels {
		for range cfg.SubsPerChannel {
			name := fmt.Sprintf("bench-sub-%d", id)
			id++
			s, err := deps.StartSub(ctx, name, []string{ch})
			if err != nil {
				stopAll()
				return Result{}, fmt.Errorf("start subscriber %s: %w", name, err)
			}
			subs = append(subs, s)
		}
	}

	plans := make([]pub.ChannelPlan, len(cfg.Channels))
	for i, ch := range cfg.Channels {
		plans[i] = pub.ChannelPlan{Channel: ch, Schedule: schedule.New(cfg.BaselineRate, bursts)}
	}
	manifest, err := deps.RunPub(ctx, plans)
	if err != nil {
		stopAll()
		return Result{}, fmt.Errorf("run publisher: %w", err)
	}

	res := Result{Manifest: manifest}
	var subLogs []check.SubscriberLog
	for _, s := range subs {
		// Stop before reading: a live subscriber's receive log is buffered and
		// only complete once Stop has flushed it.
		if stopErr := s.Stop(); stopErr != nil {
			return Result{}, fmt.Errorf("stop subscriber %s: %w", s.ID(), stopErr)
		}
		recs, rerr := s.Records()
		if rerr != nil {
			return Result{}, fmt.Errorf("read subscriber %s log: %w", s.ID(), rerr)
		}
		res.Subs = append(res.Subs, SubReport{ID: s.ID(), Events: s.Events()})
		subLogs = append(subLogs, check.SubscriberLog{ID: s.ID(), Channels: s.Channels(), Records: recs})
	}

	res.Check = check.Run(checkManifest(cfg.RunID, manifest), subLogs)
	return res, nil
}

// checkManifest builds the checker's expectation: what was published MINUS the
// seqs the publisher never confirmed (unconfirmed publishes were never expected
// at subscribers, so counting them would false-FAIL the run). An unconfirmed
// seq that is the channel's tail lowers the expected count; an interior
// unconfirmed seq is left in place (its absence would be a real hole) — but the
// publisher's unconfirmed set is rare and the common case is a trailing gap, so
// we lower Published to the highest contiguous confirmed seq.
func checkManifest(runID string, m pub.Manifest) check.Manifest {
	published := make(map[string]uint64, len(m.Published))
	for ch, last := range m.Published {
		published[ch] = highestConfirmed(last, m.Unconfirmed[ch])
	}
	return check.Manifest{Run: runID, Published: published}
}

// highestConfirmed returns the largest seq ≤ last with no unconfirmed seq at or
// below it left un-accounted — i.e. it trims a trailing run of unconfirmed seqs.
func highestConfirmed(last uint64, unconfirmed []uint64) uint64 {
	un := make(map[uint64]bool, len(unconfirmed))
	for _, s := range unconfirmed {
		un[s] = true
	}
	for last > 0 && un[last] {
		last--
	}
	return last
}
