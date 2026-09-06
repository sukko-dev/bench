// Package sub is the raw-WebSocket bench subscriber: it speaks the Sukko
// client contract directly (no SDK — the benchmark measures the platform, not
// a client library's delivery queue), appends every bench message to an rlog
// with an arrival stamp, tracks per-channel replay cursors, and on disconnect
// resumes per the contract: reconnect{client_id, last_pos} first, then
// subscribe. Recovery-relevant moments (disconnect, resubscribed) are recorded
// as timestamped events for the analysis stage.
package sub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/sukko-dev/bench/internal/rlog"
	"github.com/sukko-dev/bench/internal/wire"
)

// Config parameterizes one subscriber connection.
type Config struct {
	URL      string // ws:// or wss:// gateway endpoint
	Token    string // JWT, sent as ?token=
	RunID    string // bench run — foreign payloads are not logged
	ClientID string // persistent identity for reconnect
	Channels []string
	LogPath  string
	// AckTimeout bounds the wait for subscription_ack; default 5s.
	AckTimeout time.Duration
	// RedialDelay between reconnect attempts; default 100ms.
	RedialDelay time.Duration
}

// EventKind labels a recovery-relevant moment.
type EventKind string

const (
	EventDisconnected EventKind = "disconnected"
	EventResubscribed EventKind = "resubscribed"
)

// Event is one timestamped recovery moment.
type Event struct {
	Kind EventKind
	At   time.Time
}

// Subscriber is one running bench subscriber connection.
type Subscriber struct {
	cfg    Config
	cancel context.CancelFunc
	done   chan struct{}

	log      *rlog.Writer
	received atomic.Int64

	mu      sync.Mutex
	conn    *websocket.Conn // current connection; Stop closes it to unblock the read
	stopped bool            // set by Stop; a conn swapped in afterward is closed at once
	lastPos map[string]string
	events  []Event
	runErr  error
}

// envelope is the subset of server frames the subscriber reads.
type envelope struct {
	Type    string          `json:"type"`
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
	Pos     string          `json:"pos"`
	Mid     string          `json:"mid"`
}

// Start dials, subscribes, waits for the subscription ack, and begins logging.
// The first connection is synchronous — a subscriber that cannot establish
// itself must fail Start, not limp — while later disconnects are handled by
// the internal resume loop.
func Start(ctx context.Context, cfg Config) (*Subscriber, error) {
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 5 * time.Second
	}
	if cfg.RedialDelay <= 0 {
		cfg.RedialDelay = 100 * time.Millisecond
	}
	w, err := rlog.Create(cfg.LogPath)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	s := &Subscriber{cfg: cfg, cancel: cancel, done: make(chan struct{}), log: w, lastPos: make(map[string]string)}

	conn, err := s.establish(runCtx, false)
	if err != nil {
		cancel()
		s.log.Close() // best-effort; the establish error is the failure that matters
		return nil, err
	}
	go s.run(runCtx, conn)
	return s, nil
}

// establish dials and performs the resume handshake: reconnect (when resuming)
// MUST lead subscribe on the wire, then the subscription ack is awaited.
func (s *Subscriber) establish(ctx context.Context, resuming bool) (*websocket.Conn, error) {
	u, err := url.Parse(s.cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("subscriber url: %w", err)
	}
	q := u.Query()
	q.Set("token", s.cfg.Token)
	u.RawQuery = q.Encode()

	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("subscriber dial: %w (http %d)", err, resp.StatusCode)
		}
		return nil, fmt.Errorf("subscriber dial: %w", err)
	}

	if resuming {
		s.mu.Lock()
		lastPos := make(map[string]string, len(s.lastPos))
		for ch, pos := range s.lastPos {
			lastPos[ch] = pos
		}
		s.mu.Unlock()
		reconnect := map[string]any{
			"type": "reconnect",
			"data": map[string]any{"client_id": s.cfg.ClientID, "last_pos": lastPos},
		}
		if err := conn.WriteJSON(reconnect); err != nil {
			conn.Close() // best-effort; the write error is the failure that matters
			return nil, fmt.Errorf("subscriber reconnect frame: %w", err)
		}
	}

	subscribe := map[string]any{"type": "subscribe", "data": map[string]any{"channels": s.cfg.Channels}}
	if err := conn.WriteJSON(subscribe); err != nil {
		conn.Close() // best-effort; the write error is the failure that matters
		return nil, fmt.Errorf("subscriber subscribe frame: %w", err)
	}

	// Read until the subscription ack; broadcast frames may arrive first on a
	// resume (replay can start immediately) — they are logged, not skipped.
	deadline := time.Now().Add(s.cfg.AckTimeout)
	for {
		if err := conn.SetReadDeadline(deadline); err != nil {
			conn.Close() // best-effort; the deadline error is the failure that matters
			return nil, fmt.Errorf("subscriber ack deadline: %w", err)
		}
		var env envelope
		if err := conn.ReadJSON(&env); err != nil {
			conn.Close() // best-effort; the read error is the failure that matters
			return nil, fmt.Errorf("subscriber awaiting subscription_ack: %w", err)
		}
		if env.Type == "subscription_ack" {
			if err := conn.SetReadDeadline(time.Time{}); err != nil {
				conn.Close() // best-effort; the deadline error is the failure that matters
				return nil, fmt.Errorf("subscriber clear deadline: %w", err)
			}
			return conn, nil
		}
		s.handleFrame(env)
	}
}

// run is the read loop with resume-on-disconnect.
func (s *Subscriber) run(ctx context.Context, conn *websocket.Conn) {
	defer close(s.done)
	defer s.closeCurrentConn() // close whichever conn is current at exit
	s.setConn(conn)
	for {
		var env envelope
		if err := conn.ReadJSON(&env); err != nil {
			if ctx.Err() != nil {
				return // Stop() closed us
			}
			s.addEvent(EventDisconnected)
			next, rerr := s.resume(ctx)
			if rerr != nil {
				s.mu.Lock()
				s.runErr = rerr
				s.mu.Unlock()
				return
			}
			s.addEvent(EventResubscribed)
			conn = next
			s.setConn(conn)
			continue
		}
		s.handleFrame(env)
	}
}

// setConn swaps in the current connection. If Stop already ran, the new conn
// is closed immediately (and not stored) so the read loop's next ReadJSON
// fails at once — this closes the race where a resume completes concurrently
// with Stop and would otherwise leave a live conn nobody closes.
func (s *Subscriber) setConn(c *websocket.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		_ = c.Close() // ignored: sole purpose is to make the next read fail
		return
	}
	s.conn = c
}

// closeCurrentConn marks the subscriber stopped and closes the current
// connection to unblock a parked read; gorilla's Close on an already-closed
// conn errors harmlessly and is ignored.
func (s *Subscriber) closeCurrentConn() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
	if s.conn != nil {
		_ = s.conn.Close() // ignored: closing an already-dead conn errors and that is fine
	}
}

// resume redials until it succeeds or ctx ends.
func (s *Subscriber) resume(ctx context.Context) (*websocket.Conn, error) {
	for {
		conn, err := s.establish(ctx, true)
		if err == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("subscriber stopped during resume: %w", ctx.Err())
		}
		select {
		case <-time.After(s.cfg.RedialDelay):
		case <-ctx.Done():
			return nil, fmt.Errorf("subscriber stopped during resume: %w", ctx.Err())
		}
	}
}

// handleFrame logs bench messages and tracks replay cursors.
func (s *Subscriber) handleFrame(env envelope) {
	if env.Type != "message" && env.Type != "replay_message" {
		return
	}
	arrival := time.Now().UnixNano()
	if env.Pos != "" && env.Channel != "" {
		s.mu.Lock()
		s.lastPos[env.Channel] = env.Pos
		s.mu.Unlock()
	}
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
		return // the log error surfaces at Stop via the flush; arrival loss is visible to the checker
	}
	s.received.Add(1)
}

// Received reports how many bench records were logged so far.
func (s *Subscriber) Received() int { return int(s.received.Load()) }

// Events returns the recovery events recorded so far.
func (s *Subscriber) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

// LastPos returns a copy of the per-channel replay cursors.
func (s *Subscriber) LastPos() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.lastPos))
	for ch, pos := range s.lastPos {
		out[ch] = pos
	}
	return out
}

func (s *Subscriber) addEvent(kind EventKind) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, Event{Kind: kind, At: time.Now()})
}

// Stop ends the read loop and flushes the receive log.
func (s *Subscriber) Stop() error {
	s.cancel()
	s.closeCurrentConn() // a context cancel cannot unblock a parked read; closing the conn does
	<-s.done

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.log.Close(); err != nil {
		return err
	}
	return s.runErr
}
