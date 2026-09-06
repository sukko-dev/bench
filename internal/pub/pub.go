// Package pub is the open-loop publisher: it dispatches every scheduled
// message at its intended time regardless of how long earlier sends take
// (coordinated-omission safety — a stalled send never delays the dispatcher
// or shifts later intended stamps), and accounts for what was actually
// confirmed so the checker's expectations are honest.
package pub

import (
	"container/heap"
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sukko-dev/bench/internal/schedule"
	"github.com/sukko-dev/bench/internal/wire"
)

// SendFunc delivers one encoded payload to a channel (real impl: REST publish).
// An error means this attempt did not confirm; the publisher retries up to
// MaxAttempts, then records the seq as unconfirmed.
type SendFunc func(ctx context.Context, channel string, payload []byte) error

// SleepFunc waits until an absolute time or ctx is done (injected for tests).
type SleepFunc func(ctx context.Context, until time.Time) error

// ChannelPlan pairs a channel with its rate schedule.
type ChannelPlan struct {
	Channel  string
	Schedule *schedule.Schedule
}

// Config parameterizes one publisher run.
type Config struct {
	RunID       string
	T0          time.Time
	Duration    time.Duration
	PayloadSize int
	Plans       []ChannelPlan
	Send        SendFunc
	SleepUntil  SleepFunc
	MaxAttempts int // per message; default 3
	// MaxInFlight bounds concurrent sends; default 256. The dispatcher never
	// waits on a send, only on this admission cap.
	MaxInFlight int
}

// Manifest is the publisher's account of the run.
type Manifest struct {
	Run         string
	Published   map[string]uint64   // channel → final seq issued (1-based count)
	Unconfirmed map[string][]uint64 // channel → seqs that never confirmed (excluded from loss checks, reported)
	Retries     int                 // total failed attempts that were retried
}

// heap of per-channel cursors ordered by next intended offset.
type cursor struct {
	plan    ChannelPlan
	nextIdx int
	nextAt  time.Duration
}

type cursorHeap []*cursor

func (h cursorHeap) Len() int           { return len(h) }
func (h cursorHeap) Less(i, j int) bool { return h[i].nextAt < h[j].nextAt }
func (h cursorHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *cursorHeap) Push(x any)        { *h = append(*h, x.(*cursor)) }
func (h *cursorHeap) Pop() any          { old := *h; n := len(old); c := old[n-1]; *h = old[:n-1]; return c }

// Run publishes every scheduled message across all plans for Duration.
func Run(ctx context.Context, cfg Config) (Manifest, error) {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 256
	}
	m := Manifest{
		Run:         cfg.RunID,
		Published:   make(map[string]uint64, len(cfg.Plans)),
		Unconfirmed: make(map[string][]uint64),
	}

	h := make(cursorHeap, 0, len(cfg.Plans))
	for _, p := range cfg.Plans {
		h = append(h, &cursor{plan: p, nextAt: p.Schedule.IntendedOffset(0)})
	}
	heap.Init(&h)

	var (
		wg       sync.WaitGroup
		sem      = make(chan struct{}, cfg.MaxInFlight)
		mu       sync.Mutex // guards m.Unconfirmed
		retries  atomic.Int64
		firstErr error
	)

	for h.Len() > 0 {
		c := heap.Pop(&h).(*cursor)
		if c.nextAt >= cfg.Duration {
			continue // this channel's schedule is exhausted for the run window
		}
		intended := cfg.T0.Add(c.nextAt)
		if err := cfg.SleepUntil(ctx, intended); err != nil {
			firstErr = fmt.Errorf("publisher stopped: %w", err)
			break
		}

		seq := uint64(c.nextIdx + 1)
		payload, err := wire.Encode(wire.Payload{
			Run: cfg.RunID, Channel: c.plan.Channel, Seq: seq, IntendedUnixNano: intended.UnixNano(),
		}, cfg.PayloadSize)
		if err != nil {
			firstErr = fmt.Errorf("encode %s seq %d: %w", c.plan.Channel, seq, err)
			break
		}
		m.Published[c.plan.Channel] = seq

		// Dispatch without waiting for completion — only the admission cap can
		// hold the dispatcher back, never an individual slow send.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			firstErr = fmt.Errorf("publisher stopped: %w", ctx.Err())
		}
		if firstErr != nil {
			break
		}
		wg.Add(1)
		go func(channel string, seq uint64, payload []byte) {
			defer wg.Done()
			defer func() { <-sem }()
			for attempt := 1; ; attempt++ {
				err := cfg.Send(ctx, channel, payload)
				if err == nil {
					return
				}
				if attempt >= cfg.MaxAttempts {
					mu.Lock()
					m.Unconfirmed[channel] = append(m.Unconfirmed[channel], seq)
					mu.Unlock()
					return
				}
				retries.Add(1)
			}
		}(c.plan.Channel, seq, payload)

		c.nextIdx++
		c.nextAt = c.plan.Schedule.IntendedOffset(c.nextIdx)
		heap.Push(&h, c)
	}

	wg.Wait()
	m.Retries = int(retries.Load())
	for ch := range m.Unconfirmed {
		sort.Slice(m.Unconfirmed[ch], func(i, j int) bool { return m.Unconfirmed[ch][i] < m.Unconfirmed[ch][j] })
	}
	return m, firstErr
}
