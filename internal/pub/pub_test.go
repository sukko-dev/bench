package pub

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sukko-dev/bench/internal/schedule"
	"github.com/sukko-dev/bench/internal/wire"
)

// instantSleep never waits — virtual time for tests.
func instantSleep(ctx context.Context, _ time.Time) error { return ctx.Err() }

type sentMsg struct {
	channel string
	payload wire.Payload
}

// capture collects sends thread-safely.
type capture struct {
	mu   sync.Mutex
	msgs []sentMsg
}

func (c *capture) fn(_ context.Context, channel string, raw []byte) error {
	p, err := wire.Decode(raw)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, sentMsg{channel, p})
	return nil
}

func plan(rate float64) []ChannelPlan {
	return []ChannelPlan{
		{Channel: "t.a", Schedule: schedule.New(rate, nil)},
		{Channel: "t.b", Schedule: schedule.New(rate, nil)},
	}
}

func TestPublishesScheduledMessagesWithSeqAndIntendedTime(t *testing.T) {
	c := &capture{}
	t0 := time.Unix(1_757_000_000, 0)
	m, err := Run(context.Background(), Config{
		RunID: "r1", T0: t0, Duration: 2 * time.Second, PayloadSize: 200,
		Plans: plan(2.0), Send: c.fn, SleepUntil: instantSleep,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 2 msg/s × 2s = 4 per channel (offsets 0, .5, 1, 1.5).
	if m.Published["t.a"] != 4 || m.Published["t.b"] != 4 {
		t.Fatalf("manifest = %+v, want 4 per channel", m.Published)
	}
	if len(c.msgs) != 8 {
		t.Fatalf("sent %d messages, want 8", len(c.msgs))
	}
	// Sends complete concurrently, so capture order is not send order; the
	// invariant is that each channel was issued exactly seqs 1..4.
	seqs := map[string]map[uint64]bool{}
	for _, s := range c.msgs {
		if s.payload.Run != "r1" || s.payload.Channel != s.channel {
			t.Errorf("payload %+v inconsistent with channel %s", s.payload, s.channel)
		}
		if seqs[s.channel] == nil {
			seqs[s.channel] = map[uint64]bool{}
		}
		seqs[s.channel][s.payload.Seq] = true
	}
	for ch, got := range seqs {
		for s := uint64(1); s <= 4; s++ {
			if !got[s] {
				t.Errorf("%s missing seq %d: %v", ch, s, got)
			}
		}
		if len(got) != 4 {
			t.Errorf("%s has %d distinct seqs, want 4", ch, len(got))
		}
	}
	// Intended stamps must equal t0 + the pure schedule offsets.
	want := t0.UnixNano() + (500 * time.Millisecond).Nanoseconds()
	var found bool
	for _, s := range c.msgs {
		if s.channel == "t.a" && s.payload.Seq == 2 {
			found = s.payload.IntendedUnixNano == want
		}
	}
	if !found {
		t.Errorf("t.a seq 2 does not carry intended time t0+500ms")
	}
}

func TestStalledSendDoesNotShiftLaterIntendedTimes(t *testing.T) {
	// Coordinated-omission safety: one send blocking must neither stall the
	// dispatcher nor shift any later message's intended stamp.
	release := make(chan struct{})
	c := &capture{}
	var once sync.Once
	blockingFirst := func(ctx context.Context, channel string, raw []byte) error {
		var blocked bool
		once.Do(func() { blocked = true })
		if blocked {
			<-release // first send stalls until released
		}
		return c.fn(ctx, channel, raw)
	}
	t0 := time.Unix(1_757_000_000, 0)
	done := make(chan struct{})
	var m Manifest
	var runErr error
	go func() {
		defer close(done)
		m, runErr = Run(context.Background(), Config{
			RunID: "r1", T0: t0, Duration: 2 * time.Second, PayloadSize: 200,
			Plans: []ChannelPlan{{Channel: "t.a", Schedule: schedule.New(4.0, nil)}},
			Send:  blockingFirst, SleepUntil: instantSleep,
		})
	}()
	// The dispatcher must finish issuing all sends while the first is blocked.
	deadline := time.After(2 * time.Second)
	for {
		c.mu.Lock()
		n := len(c.msgs)
		c.mu.Unlock()
		if n >= 7 { // 8 scheduled; 7 unblocked ones complete
			break
		}
		select {
		case <-deadline:
			t.Fatal("dispatcher stalled behind a blocked send — closed-loop behavior")
		case <-time.After(5 * time.Millisecond):
		}
	}
	close(release)
	<-done
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
	if m.Published["t.a"] != 8 {
		t.Fatalf("Published = %d, want 8", m.Published["t.a"])
	}
	// Every intended stamp still equals the pure schedule — no shift.
	s := schedule.New(4.0, nil)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, msg := range c.msgs {
		i := int(msg.payload.Seq - 1)
		want := t0.Add(s.IntendedOffset(i)).UnixNano()
		if msg.payload.IntendedUnixNano != want {
			t.Errorf("seq %d intended = %d, want %d (schedule shifted)", msg.payload.Seq, msg.payload.IntendedUnixNano, want)
		}
	}
}

func TestFailedSendRetriesThenRecordsUnconfirmed(t *testing.T) {
	fails := map[uint64]int{2: 999, 3: 2} // seq2 always fails; seq3 fails twice then succeeds
	c := &capture{}
	var mu sync.Mutex
	flaky := func(ctx context.Context, channel string, raw []byte) error {
		p, err := wire.Decode(raw)
		if err != nil {
			return err
		}
		mu.Lock()
		left := fails[p.Seq]
		if left > 0 {
			fails[p.Seq]--
		}
		mu.Unlock()
		if left > 0 {
			return context.DeadlineExceeded
		}
		return c.fn(ctx, channel, raw)
	}
	m, err := Run(context.Background(), Config{
		RunID: "r1", T0: time.Unix(1_757_000_000, 0), Duration: 2 * time.Second, PayloadSize: 200,
		Plans: []ChannelPlan{{Channel: "t.a", Schedule: schedule.New(2.0, nil)}},
		Send:  flaky, SleepUntil: instantSleep, MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if m.Published["t.a"] != 4 {
		t.Fatalf("Published = %d, want 4 (unconfirmed still occupy seq space)", m.Published["t.a"])
	}
	if len(m.Unconfirmed["t.a"]) != 1 || m.Unconfirmed["t.a"][0] != 2 {
		t.Fatalf("Unconfirmed = %+v, want [2] for t.a", m.Unconfirmed)
	}
	if m.Retries != 4 {
		t.Errorf("Retries = %d, want 4 (two per failing seq before give-up/success)", m.Retries)
	}
}

func TestContextCancelStopsRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Run(ctx, Config{
		RunID: "r1", T0: time.Unix(1_757_000_000, 0), Duration: time.Hour, PayloadSize: 200,
		Plans: plan(1000.0), Send: (&capture{}).fn, SleepUntil: instantSleep,
	})
	if err == nil {
		t.Fatal("Run ignored a cancelled context")
	}
}

// hintedErr fakes a rate-limit rejection carrying the server's Retry-After.
type hintedErr struct{ after time.Duration }

func (e *hintedErr) Error() string             { return "rate limited (test)" }
func (e *hintedErr) RetryAfter() time.Duration { return e.after }

// backoffRecorder captures the delays the retry loop waits between attempts.
type backoffRecorder struct {
	mu     sync.Mutex
	delays []time.Duration
}

func (b *backoffRecorder) fn(ctx context.Context, d time.Duration) error {
	b.mu.Lock()
	b.delays = append(b.delays, d)
	b.mu.Unlock()
	return ctx.Err()
}

// TestRetryHonorsRetryAfterHint pins the §IX handshake from the client side: when a send
// error carries the server's Retry-After, the retry MUST wait exactly that long. Retrying
// sooner lands in the same empty token bucket — it cannot succeed, and it triples the load
// on the endpoint that just asked for less.
func TestRetryHonorsRetryAfterHint(t *testing.T) {
	c := &capture{}
	var mu sync.Mutex
	failures := 2
	rateLimited := func(ctx context.Context, channel string, raw []byte) error {
		mu.Lock()
		left := failures
		if left > 0 {
			failures--
		}
		mu.Unlock()
		if left > 0 {
			return &hintedErr{after: 2 * time.Second}
		}
		return c.fn(ctx, channel, raw)
	}
	rec := &backoffRecorder{}
	m, err := Run(context.Background(), Config{
		RunID: "r1", T0: time.Unix(1_757_000_000, 0), Duration: time.Second, PayloadSize: 200,
		Plans: []ChannelPlan{{Channel: "t.a", Schedule: schedule.New(1.0, nil)}},
		Send:  rateLimited, SleepUntil: instantSleep, Backoff: rec.fn, MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(m.Unconfirmed["t.a"]) != 0 {
		t.Fatalf("Unconfirmed = %+v, want none (third attempt succeeds)", m.Unconfirmed)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.delays) != 2 {
		t.Fatalf("recorded %d backoffs (%v), want 2 (one before each retry)", len(rec.delays), rec.delays)
	}
	for i, d := range rec.delays {
		if d != 2*time.Second {
			t.Errorf("backoff[%d] = %v, want 2s (the server's Retry-After)", i, d)
		}
	}
}

// TestRetryBacksOffWithoutHint: an unhinted failure must still wait — growing delays,
// never an immediate retry — but bounded, so a dead endpoint doesn't stall the run.
func TestRetryBacksOffWithoutHint(t *testing.T) {
	alwaysFail := func(context.Context, string, []byte) error { return context.DeadlineExceeded }
	rec := &backoffRecorder{}
	m, err := Run(context.Background(), Config{
		RunID: "r1", T0: time.Unix(1_757_000_000, 0), Duration: time.Second, PayloadSize: 200,
		Plans: []ChannelPlan{{Channel: "t.a", Schedule: schedule.New(1.0, nil)}},
		Send:  alwaysFail, SleepUntil: instantSleep, Backoff: rec.fn, MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(m.Unconfirmed["t.a"]) != 1 {
		t.Fatalf("Unconfirmed = %+v, want the one seq after MaxAttempts", m.Unconfirmed)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.delays) != 2 {
		t.Fatalf("recorded %d backoffs (%v), want 2", len(rec.delays), rec.delays)
	}
	if rec.delays[0] <= 0 || rec.delays[1] <= rec.delays[0] {
		t.Errorf("delays = %v, want positive and growing (exponential backoff)", rec.delays)
	}
}
