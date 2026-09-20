// Package sub is the raw-WebSocket bench subscriber: it speaks the Sukko
// client contract directly (no SDK — the benchmark measures the platform, not
// a client library's delivery queue), appends every bench message to an rlog
// with an arrival stamp, tracks per-channel replay cursors, and on disconnect
// resumes per the contract: reconnect{client_id, last_pos} first, then
// subscribe. Mid-session, server gap frames are answered with replay
// requests: reason "overflow" replays from the frame's last_pos, and reason
// "resume" (a broadcast-bus lapse of unknown extent) replays from the newest
// per-connection (ts, pos) checkpoint strictly before the lapse start — never
// the latest received pos, which sits after the hole because upstream-lost
// messages consume no per-connection seqs. Replays are serialized per channel
// on replay_complete (gaps landing mid-flight coalesce to the oldest anchor),
// truncated completions surface distinctly from clean ones, and terminal
// replay errors distinctly from transient ones. Recovery-relevant moments
// (disconnect, resubscribed, gaps, replay requests/queues/skips/completions/
// errors) are recorded as timestamped events for the analysis stage.
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
	// EventResumeGap: the server reported a broadcast-bus lapse of unknown
	// extent (gap frame, reason "resume").
	EventResumeGap EventKind = "resume_gap"
	// EventOverflowGap: the server dropped live messages for this slow
	// client (gap frame, reason "overflow").
	EventOverflowGap EventKind = "overflow_gap"
	// EventReplayRequested: a replay frame was sent; Detail is the from_pos.
	EventReplayRequested EventKind = "replay_requested"
	// EventReplaySkipped: a gap could not be replayed (no valid baseline);
	// the loss stays visible to the checker instead of being guessed at.
	EventReplaySkipped EventKind = "replay_skipped"
	// EventReplayQueued: a gap arrived while a replay was already in flight
	// for the channel; its anchor was coalesced into the single pending
	// replay slot. Detail is the pending from_pos after coalescing.
	EventReplayQueued EventKind = "replay_queued"
	// EventReplayCompleted: the server signalled a clean end of replay
	// (replay_complete, truncated absent/false); Detail carries the
	// messages_replayed count.
	EventReplayCompleted EventKind = "replay_completed"
	// EventReplayTruncated: replay_complete arrived with truncated=true —
	// delivery was cut short, the hole is only PARTLY filled. Distinct from
	// EventReplayCompleted so under-recovery can never be mistaken for full
	// recovery in the run artifact.
	EventReplayTruncated EventKind = "replay_truncated"
	// EventReplayError: a transient replay rejection or failure (replay in
	// progress, rate limited, replay failed) — retrying can help; Detail is
	// the code and server message. Not fatal.
	EventReplayError EventKind = "replay_error"
	// EventReplayUnrecoverable: a terminal replay rejection (e.g.
	// offset_out_of_range — the anchor predates retention): recovery is
	// impossible and retrying cannot help. Kept distinct from
	// EventReplayError so an unrecoverable run is not mistaken for a busy
	// one.
	EventReplayUnrecoverable EventKind = "replay_unrecoverable"
)

// terminalReplayCodes are replay error codes for which retrying the same
// request cannot help: the anchor is gone from retention, or the request
// itself is unacceptable to the server.
var terminalReplayCodes = map[string]bool{
	"offset_out_of_range": true,
	"invalid_request":     true,
	"not_subscribed":      true,
	"not_available":       true,
}

// replaySpecificCodes are error codes that can only refer to a replay, so an
// "error" envelope carrying one is surfaced even when the harness has no
// replay in flight for the channel (it may be out of sync with the server —
// visibility wins). Generic codes with no replay in flight are ignored: they
// are not attributable to replay.
var replaySpecificCodes = map[string]bool{
	"replay_in_progress":  true,
	"replay_rate_limited": true,
	"replay_failed":       true,
	"offset_out_of_range": true,
}

// Event is one timestamped recovery moment. Channel and Detail are set for
// gap/replay events so the run artifact shows what was recovered from where.
type Event struct {
	Kind    EventKind
	At      time.Time
	Channel string `json:",omitempty"`
	Detail  string `json:",omitempty"`
}

// checkpoint is one (envelope ts, pos) pair recorded for resume-gap
// anchoring. Checkpoints are per-connection: server timestamps are only
// comparable to gap timestamps from the same connection.
type checkpoint struct {
	ts  int64
	pos string
}

// pendingReplay is the single coalesced follow-up replay for a channel while
// another replay is in flight. anchorTs orders candidates: the OLDEST anchor
// wins, because a replay from pos P re-delivers everything from P forward —
// one replay from the oldest anchor covers every gap that queued behind it.
// Caveat: a resume candidate's anchorTs is a checkpoint stamp (pre-lapse)
// while an overflow candidate's is the gap's own stamp (lapse start); both
// come from the same server clock so the comparison is sound, but mixing the
// two kinds on one channel can pick a slightly newer position than ideal.
type pendingReplay struct {
	fromPos  string
	anchorTs int64
}

// checkpointCap bounds per-channel checkpoint memory. When full, the list is
// thinned by dropping every other entry — old checkpoints get sparser but the
// oldest is never evicted, so an anchor strictly before the gap ts is found
// whenever any pre-gap message was received on this connection. A sparser
// anchor only re-delivers more, which is safe: the checker dedupes by mid.
const checkpointCap = 512

// Subscriber is one running bench subscriber connection.
type Subscriber struct {
	cfg    Config
	cancel context.CancelFunc
	done   chan struct{}

	log      *rlog.Writer
	received atomic.Int64

	mu       sync.Mutex
	conn     *websocket.Conn // current connection; Stop closes it to unblock the read
	stopped  bool            // set by Stop; a conn swapped in afterward is closed at once
	lastPos  map[string]string
	ckpts    map[string][]checkpoint  // per-channel (ts, pos) history, ts ascending; reset per connection
	inFlight map[string]bool          // channels with a replay awaiting replay_complete/error; reset per connection
	pending  map[string]pendingReplay // coalesced follow-up replay per in-flight channel; reset per connection
	events   []Event
}

// envelope is the subset of server frames the subscriber reads.
type envelope struct {
	Type    string          `json:"type"`
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
	Pos     string          `json:"pos"`
	Mid     string          `json:"mid"`
	Ts      int64           `json:"ts"`       // server stamp; gap frames carry the lapse start
	Reason  string          `json:"reason"`   // gap frames: "overflow" or "resume"
	LastPos string          `json:"last_pos"` // gap frames: replay anchor for "overflow"; empty for "resume"
	Code    string          `json:"code"`     // error frames: rejection/failure code
	Message string          `json:"message"`  // error frames: human-readable detail
	// replay_complete fields
	MessagesReplayed int  `json:"messages_replayed"` // count of replay_message envelopes delivered
	Truncated        bool `json:"truncated"`         // true = delivery cut short; absent on the wire when false
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
	s := &Subscriber{cfg: cfg, cancel: cancel, done: make(chan struct{}), log: w, lastPos: make(map[string]string), ckpts: make(map[string][]checkpoint), inFlight: make(map[string]bool), pending: make(map[string]pendingReplay)}

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

	// Checkpoints are per-connection: a new connection may be a different
	// server with a different clock, so stamps recorded on the old one are
	// not comparable to gap timestamps on this one. The lastPos cursor is
	// broker state and survives (it drives the reconnect handshake below).
	// Replay serialization state is per-connection too: the server tracks
	// in-flight replays per connection, so an unresolved replay on a dead
	// connection must not queue gaps on this one.
	s.mu.Lock()
	clear(s.ckpts)
	clear(s.inFlight)
	clear(s.pending)
	s.mu.Unlock()

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
		s.handleFrame(conn, env)
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
				// resume only returns an error on ctx cancellation (all other
				// redial failures are retried), i.e. the run ended while this
				// subscriber was still redialing. That is a graceful stop, not a
				// harness fault: the subscriber simply did not recover in time,
				// which the checker already reflects as holes (under-recovery).
				// Recording it as a fatal error would wrongly abort the whole
				// run and discard the result.
				return
			}
			s.addEvent(EventResubscribed)
			conn = next
			s.setConn(conn)
			continue
		}
		s.handleFrame(conn, env)
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

// handleFrame logs bench messages, tracks replay cursors and resume-gap
// checkpoints, and answers gap frames with replay requests. conn is the
// connection the frame arrived on — replies go there, never to s.conn, which
// may still point at a previous connection while establish is mid-handshake.
// All frame handling runs on the single read goroutine, so conn writes here
// never race with the handshake writes in establish.
func (s *Subscriber) handleFrame(conn *websocket.Conn, env envelope) {
	switch env.Type {
	case "message", "replay_message":
		// fall through to logging below
	case "gap":
		s.handleGap(conn, env)
		return
	case "replay_complete":
		s.handleReplayComplete(conn, env)
		return
	case "error":
		// Replay rejections and failures arrive as the generic error
		// envelope — there is no dedicated replay_error frame type.
		s.handleReplayError(conn, env)
		return
	default:
		return
	}
	arrival := time.Now().UnixNano()
	if env.Pos != "" && env.Channel != "" {
		s.mu.Lock()
		s.lastPos[env.Channel] = env.Pos
		s.recordCheckpoint(env.Channel, env.Ts, env.Pos)
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

// recordCheckpoint appends a (ts, pos) pair to the channel's checkpoint
// history. Caller must hold s.mu. The list is kept ts-ascending: an equal
// stamp updates the tail pos (newer pos, same validity); an out-of-order
// stamp is dropped — the worst case is a slightly older anchor, which only
// re-delivers more.
func (s *Subscriber) recordCheckpoint(channel string, ts int64, pos string) {
	if ts <= 0 {
		return
	}
	cks := s.ckpts[channel]
	if n := len(cks); n > 0 {
		if ts < cks[n-1].ts {
			return
		}
		if ts == cks[n-1].ts {
			cks[n-1].pos = pos
			return
		}
	}
	if len(cks) >= checkpointCap {
		cks = thinCheckpoints(cks)
	}
	s.ckpts[channel] = append(cks, checkpoint{ts: ts, pos: pos})
}

// thinCheckpoints halves the list by dropping every other entry, always
// keeping the oldest (index 0) and the newest (tail). Repeated thinning
// leaves old history exponentially spaced and recent history dense — exactly
// the shape resume-gap anchoring needs.
func thinCheckpoints(cks []checkpoint) []checkpoint {
	out := cks[:0]
	for i := 0; i < len(cks); i += 2 {
		out = append(out, cks[i])
	}
	if len(cks)%2 == 0 {
		out = append(out, cks[len(cks)-1])
	}
	return out
}

// anchorBefore returns the newest recorded (pos, ts) whose envelope ts is
// strictly before gapTs, or ok=false when no such baseline exists on this
// connection. Caller must hold s.mu.
func (s *Subscriber) anchorBefore(channel string, gapTs int64) (pos string, ts int64, ok bool) {
	cks := s.ckpts[channel]
	for i := len(cks) - 1; i >= 0; i-- {
		if cks[i].ts < gapTs {
			return cks[i].pos, cks[i].ts, true
		}
	}
	return "", 0, false
}

// handleGap reacts to a server gap frame. reason "overflow" carries an
// explicit replay anchor (last_pos). reason "resume" reports a lapse of
// unknown extent starting at env.Ts — the replay anchors at the newest
// checkpoint strictly before that stamp, NEVER the latest received pos:
// per-connection seqs stay contiguous across an upstream hole, so the latest
// pos sits after the loss and would skip exactly what must be recovered.
func (s *Subscriber) handleGap(conn *websocket.Conn, env envelope) {
	switch env.Reason {
	case "overflow":
		s.addGapEvent(EventOverflowGap, env.Channel, env.LastPos)
		if env.LastPos == "" {
			s.addGapEvent(EventReplaySkipped, env.Channel, "overflow gap without last_pos")
			return
		}
		s.startOrQueueReplay(conn, env.Channel, env.LastPos, env.Ts)
	case "resume":
		s.addGapEvent(EventResumeGap, env.Channel, fmt.Sprintf("gap ts=%d", env.Ts))
		s.mu.Lock()
		fromPos, anchorTs, ok := s.anchorBefore(env.Channel, env.Ts)
		s.mu.Unlock()
		if !ok {
			// No message with ts < gap ts was received on this connection
			// (e.g. subscribed during the lapse): there is no valid
			// baseline — skip rather than guess (never queue a guess
			// either). The loss stays visible.
			s.addGapEvent(EventReplaySkipped, env.Channel, "no baseline before gap ts")
			return
		}
		s.startOrQueueReplay(conn, env.Channel, fromPos, anchorTs)
	}
	// Unknown reasons are ignored: without defined semantics any replay
	// anchor would be a guess, and a wrong from_pos is a protocol error.
}

// startOrQueueReplay serializes replays per channel: the server allows one
// replay at a time, so instead of provoking a replay_in_progress rejection,
// a gap that lands mid-flight coalesces into the channel's single pending
// slot (oldest anchor wins — see pendingReplay) and fires when the in-flight
// replay resolves. The server remains the authority: if the harness's view
// is ever out of sync, the rejection path in handleReplayError still catches
// it.
func (s *Subscriber) startOrQueueReplay(conn *websocket.Conn, channel, fromPos string, anchorTs int64) {
	s.mu.Lock()
	if s.inFlight[channel] {
		if p, ok := s.pending[channel]; !ok || anchorTs < p.anchorTs {
			s.pending[channel] = pendingReplay{fromPos: fromPos, anchorTs: anchorTs}
		}
		queued := s.pending[channel].fromPos
		s.mu.Unlock()
		s.addGapEvent(EventReplayQueued, channel, queued)
		return
	}
	s.inFlight[channel] = true
	s.mu.Unlock()
	s.sendReplayInFlight(conn, channel, fromPos)
}

// sendReplayInFlight sends a replay whose in-flight slot is already claimed,
// releasing the slot if the write fails (the connection is dying and
// establish will reset all per-connection state anyway).
func (s *Subscriber) sendReplayInFlight(conn *websocket.Conn, channel, fromPos string) {
	if err := s.sendReplay(conn, channel, fromPos); err != nil {
		s.mu.Lock()
		delete(s.inFlight, channel)
		s.mu.Unlock()
	}
}

// resolveInFlight closes the channel's in-flight slot and pops its pending
// replay, if any, re-claiming the slot for it under the same lock. The
// caller sends the returned pending replay after recording its event.
func (s *Subscriber) resolveInFlight(channel string) (p pendingReplay, hasPending bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inFlight, channel)
	p, hasPending = s.pending[channel]
	delete(s.pending, channel)
	if hasPending {
		s.inFlight[channel] = true
	}
	return p, hasPending
}

// handleReplayComplete closes the in-flight replay. truncated=true means
// delivery was cut short — the hole is only partly filled — and is surfaced
// as a DISTINCT event so under-recovery can never look like full recovery.
// Either way a pending coalesced replay fires next.
func (s *Subscriber) handleReplayComplete(conn *websocket.Conn, env envelope) {
	p, hasPending := s.resolveInFlight(env.Channel)
	if env.Truncated {
		s.addGapEvent(EventReplayTruncated, env.Channel, fmt.Sprintf("messages_replayed=%d truncated", env.MessagesReplayed))
	} else {
		s.addGapEvent(EventReplayCompleted, env.Channel, fmt.Sprintf("messages_replayed=%d", env.MessagesReplayed))
	}
	if hasPending {
		s.sendReplayInFlight(conn, env.Channel, p.fromPos)
	}
}

// handleReplayError reacts to the generic "error" envelope. With a replay in
// flight for the channel it is that replay's outcome; otherwise only
// replay-specific codes are attributable (and still surfaced — the harness
// may be out of sync with the server). Terminal codes (retry cannot help)
// surface distinctly from transient ones. A pending replay still fires: only
// the server knows whether ITS anchor is acceptable, and one extra rejection
// is cheaper — and more visible — than silently dropping queued coverage.
// Replay errors never abort the run: the checker's loss count stays ground
// truth.
func (s *Subscriber) handleReplayError(conn *websocket.Conn, env envelope) {
	s.mu.Lock()
	wasInFlight := s.inFlight[env.Channel]
	s.mu.Unlock()
	if !wasInFlight && !replaySpecificCodes[env.Code] {
		return // generic error, no replay in flight: not attributable to replay
	}
	p, hasPending := s.resolveInFlight(env.Channel)
	detail := env.Code
	if env.Message != "" {
		detail += ": " + env.Message
	}
	kind := EventReplayError
	if terminalReplayCodes[env.Code] {
		kind = EventReplayUnrecoverable
	}
	s.addGapEvent(kind, env.Channel, detail)
	if hasPending {
		s.sendReplayInFlight(conn, env.Channel, p.fromPos)
	}
}

// sendReplay issues a mid-session replay request. fromPos is inclusive and
// is always echoed verbatim from a received frame — never synthesized. The
// write happens outside any lock (single-writer read goroutine).
func (s *Subscriber) sendReplay(conn *websocket.Conn, channel, fromPos string) error {
	replay := map[string]any{
		"type": "replay",
		"data": map[string]any{"channel": channel, "from_pos": fromPos},
	}
	if err := conn.WriteJSON(replay); err != nil {
		// The connection is dying; the read loop will notice and resume.
		// No event: the request never reached the wire.
		return err
	}
	s.addGapEvent(EventReplayRequested, channel, fromPos)
	return nil
}

func (s *Subscriber) addGapEvent(kind EventKind, channel, detail string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, Event{Kind: kind, At: time.Now(), Channel: channel, Detail: detail})
}

// Received reports how many bench records were logged so far.
func (s *Subscriber) Received() int { return int(s.received.Load()) }

// ID returns the subscriber's persistent client identity.
func (s *Subscriber) ID() string { return s.cfg.ClientID }

// Channels returns the channels this subscriber holds.
func (s *Subscriber) Channels() []string { return s.cfg.Channels }

// Records reads the flushed receive log. Only valid after Stop — the log is
// buffered while the subscriber runs.
func (s *Subscriber) Records() ([]rlog.Record, error) {
	return rlog.ReadAll(s.cfg.LogPath)
}

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
	return s.log.Close()
}
