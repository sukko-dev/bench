package sub

// This file is the SSE counterpart to the raw-WebSocket subscriber in sub.go.
// SSE is receive-only and its recovery is server-driven: on reconnect the client
// re-sends the opaque Last-Event-ID cursor it last saw, and the ws-server replays
// the gap inside the Subscribe stream (ADR-0030). There is no client→server gap or
// replay frame — so this subscriber is far simpler than the WS one: read events,
// log records, and on stream loss redial echoing the cursor verbatim.
//
// It writes the SAME rlog.Record stream the WS subscriber does (seq/mid/channel
// from the decoded bench payload), so the transport-agnostic checker (internal/check)
// proves SSE zero-loss with no changes. It satisfies the same scenario.Subscriber
// interface (Stop/Events/Records/ID/Channels).

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sukko-dev/bench/internal/rlog"
	"github.com/sukko-dev/bench/internal/wire"
)

// SSESubscriber is one running SSE bench subscriber connection.
type SSESubscriber struct {
	cfg      Config
	cancel   context.CancelFunc
	done     chan struct{}
	log      *rlog.Writer
	received atomic.Int64

	mu     sync.Mutex
	events []Event
	cursor string    // last opaque id: seen; echoed verbatim on redial (never decoded)
	body   io.Closer // current stream body; Stop closes it to unblock the read
}

// StartSSE opens the SSE stream, confirms the 200, and begins logging. The first
// connection is synchronous — a subscriber that cannot establish is a setup error,
// not a silent zero-delivery result (mirrors sub.Start).
func StartSSE(ctx context.Context, cfg Config) (*SSESubscriber, error) {
	if cfg.RedialDelay <= 0 {
		cfg.RedialDelay = 100 * time.Millisecond
	}
	w, err := rlog.Create(cfg.LogPath)
	if err != nil {
		return nil, fmt.Errorf("sse subscriber log: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	s := &SSESubscriber{cfg: cfg, cancel: cancel, done: make(chan struct{}), log: w}

	resp, err := s.connect(ctx)
	if err != nil {
		cancel()
		_ = w.Close() // best-effort: the connect error is the failure that matters
		return nil, err
	}
	go s.run(ctx, resp)
	return s, nil
}

// connect issues GET {base}/sse?channels=...&token=..., echoing the opaque
// Last-Event-ID cursor when one is held. Returns the streaming response on 200.
func (s *SSESubscriber) connect(ctx context.Context) (*http.Response, error) {
	base, err := url.Parse(s.cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("sse subscriber url: %w", err)
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/sse"
	q := base.Query()
	q.Set("channels", strings.Join(s.cfg.Channels, ","))
	if s.cfg.Token != "" {
		q.Set("token", s.cfg.Token)
	}
	base.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("sse subscriber request: %w", err)
	}
	s.mu.Lock()
	cursor := s.cursor
	s.mu.Unlock()
	if cursor != "" {
		req.Header.Set("Last-Event-ID", cursor) // opaque — echoed verbatim, never decoded
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sse subscriber dial: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("sse subscriber dial: http %d (channels=%s) — SSE is Pro-gated, is the stack licensed Pro?",
			resp.StatusCode, strings.Join(s.cfg.Channels, ","))
	}
	return resp, nil
}

// run reads the SSE stream, logging each delivery record. On stream loss (the
// ws-server was killed and its Subscribe stream ended) it redials, echoing the
// cursor; the server replays the gap. Redial failures are retried until the run's
// context is canceled — a subscriber that never recovers surfaces as holes in the
// checker, which is the honest outcome, not a harness fault.
func (s *SSESubscriber) run(ctx context.Context, resp *http.Response) {
	defer close(s.done)
	s.setBody(resp.Body)
	for {
		s.readStream(ctx, resp.Body)
		if ctx.Err() != nil {
			s.closeCurrentBody()
			return // Stop() canceled us
		}
		s.addEvent(EventDisconnected)
		next, ok := s.redial(ctx)
		if !ok {
			s.closeCurrentBody()
			return // run ended while redialing — under-recovery shows as holes
		}
		s.addEvent(EventResubscribed)
		s.closeCurrentBody() // close the dead stream before swapping in the new one (no leak, no ErrTooLong redial loop)
		resp = next
		s.setBody(resp.Body)
	}
}

// readStream consumes one connection's event stream until it ends (EOF, error, or
// ctx cancel). Each dispatched `event: message` frame is decoded and logged; the
// latest `id:` is captured as the resume cursor.
func (s *SSESubscriber) readStream(ctx context.Context, body io.ReadCloser) {
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var data strings.Builder
	var lastEventID string // the block's id: value, held until the block dispatches
	hasSeenID := false     // an id: appeared on this stream (guards against clobbering the cursor with "")
	hasData := false
	for sc.Scan() {
		if ctx.Err() != nil {
			return
		}
		line := sc.Text()
		switch {
		case line == "":
			// Dispatch. WHATWG EventSource commits the last-event-ID at the blank line
			// (even for a data-less block — e.g. the gateway's bare `id:\n\n`
			// keepalive-flush), NOT when the id: line is parsed. So a block truncated
			// before its terminating blank line (an id: line delivered, the event body
			// or blank line lost — reachable once a gateway-kill/partition fault exists)
			// never advances the resume cursor past an event this subscriber never
			// dispatched, which would otherwise skip it on redial and read as a hole.
			if hasSeenID {
				s.mu.Lock()
				s.cursor = lastEventID // opaque, verbatim
				s.mu.Unlock()
			}
			if hasData {
				s.dispatch(data.String())
			}
			data.Reset()
			hasData = false
		case strings.HasPrefix(line, ":"):
			// keepalive comment — ignore
		case strings.HasPrefix(line, "id:"):
			lastEventID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
			hasSeenID = true
		case strings.HasPrefix(line, "data:"):
			if hasData {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			hasData = true
		}
	}
}

// dispatch decodes one SSE event payload (the full broadcast envelope) and logs it
// if it is a delivery for this run. Replayed records arrive as ordinary "message"
// frames on the SSE stream, so a single case covers live and replayed delivery.
func (s *SSESubscriber) dispatch(payload string) {
	var env envelope
	if err := json.Unmarshal([]byte(payload), &env); err != nil {
		return
	}
	if env.Type != "message" {
		return
	}
	arrival := time.Now().UnixNano()
	p, err := wire.Decode(env.Data)
	if err != nil || p.Run != s.cfg.RunID {
		return // foreign traffic is never logged
	}
	rec := rlog.Record{
		ArrivalUnixNano:  arrival,
		IntendedUnixNano: p.IntendedUnixNano,
		Channel:          p.Channel,
		Seq:              p.Seq,
		Mid:              env.Mid,
	}
	s.mu.Lock()
	err = s.log.Append(rec)
	s.mu.Unlock()
	if err != nil {
		return // log error surfaces at Stop via flush; arrival loss is visible to the checker
	}
	s.received.Add(1)
}

// redial retries connect with RedialDelay backoff until it succeeds or ctx is
// canceled. Returns (resp, true) on success, (nil, false) if the run ended first.
func (s *SSESubscriber) redial(ctx context.Context) (*http.Response, bool) {
	for {
		resp, err := s.connect(ctx)
		if err == nil {
			return resp, true
		}
		if ctx.Err() != nil {
			return nil, false
		}
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(s.cfg.RedialDelay):
		}
	}
}

func (s *SSESubscriber) setBody(b io.Closer) {
	s.mu.Lock()
	s.body = b
	s.mu.Unlock()
}

func (s *SSESubscriber) closeCurrentBody() {
	s.mu.Lock()
	b := s.body
	s.body = nil
	s.mu.Unlock()
	if b != nil {
		_ = b.Close() // best-effort: unblocking the read is what matters
	}
}

func (s *SSESubscriber) addEvent(kind EventKind) {
	s.mu.Lock()
	s.events = append(s.events, Event{Kind: kind, At: time.Now()})
	s.mu.Unlock()
}

// Stop cancels the run, closes the current stream to unblock the read, waits for
// the loop to exit, and flushes the log.
func (s *SSESubscriber) Stop() error {
	s.cancel()
	s.closeCurrentBody()
	<-s.done
	return s.log.Close()
}

// Events returns a copy of the recorded recovery moments.
func (s *SSESubscriber) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

// Records reads back the full receive log.
func (s *SSESubscriber) Records() ([]rlog.Record, error) { return rlog.ReadAll(s.cfg.LogPath) }

// Received reports the count of logged delivery records.
func (s *SSESubscriber) Received() int { return int(s.received.Load()) }

// ID returns the subscriber's client identity.
func (s *SSESubscriber) ID() string { return s.cfg.ClientID }

// Channels returns the subscribed channels.
func (s *SSESubscriber) Channels() []string { return s.cfg.Channels }
