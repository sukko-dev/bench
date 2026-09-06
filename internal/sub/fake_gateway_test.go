package sub

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
)

// fakeGateway is an in-process WebSocket server speaking just enough of the
// Sukko client contract for subscriber tests: token auth via query param,
// subscribe → subscription_ack, reconnect frame capture, and scripted
// message/replay delivery. It is the test's instrument — assertions run
// against what the REAL subscriber sent and logged.
type fakeGateway struct {
	t        *testing.T
	srv      *httptest.Server
	upgrader websocket.Upgrader

	mu       sync.Mutex
	conns    []*fakeConn
	newConnC chan *fakeConn
}

type fakeConn struct {
	ws *websocket.Conn
	// frames the client sent, in order, as raw JSON envelopes
	mu     sync.Mutex
	frames []map[string]any
	gotSub chan struct{} // closed when a subscribe frame arrives
}

func newFakeGateway(t *testing.T) *fakeGateway {
	g := &fakeGateway{t: t, newConnC: make(chan *fakeConn, 8)}
	g.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("token") == "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		ws, err := g.upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		fc := &fakeConn{ws: ws, gotSub: make(chan struct{})}
		g.mu.Lock()
		g.conns = append(g.conns, fc)
		g.mu.Unlock()
		g.newConnC <- fc
		go fc.readLoop()
	}))
	t.Cleanup(g.srv.Close)
	return g
}

func (g *fakeGateway) wsURL() string {
	return "ws" + strings.TrimPrefix(g.srv.URL, "http")
}

func (fc *fakeConn) readLoop() {
	var subOnce sync.Once
	for {
		_, raw, err := fc.ws.ReadMessage()
		if err != nil {
			return
		}
		var env map[string]any
		if err := json.Unmarshal(raw, &env); err != nil {
			continue
		}
		fc.mu.Lock()
		fc.frames = append(fc.frames, env)
		fc.mu.Unlock()
		if env["type"] == "subscribe" {
			// Ack every requested channel.
			data, _ := env["data"].(map[string]any)
			chans, _ := data["channels"].([]any)
			ack := map[string]any{"type": "subscription_ack", "subscribed": chans, "count": len(chans)}
			subOnce.Do(func() { close(fc.gotSub) })
			fc.send(ack)
		}
	}
}

func (fc *fakeConn) send(v any) {
	raw, _ := json.Marshal(v)
	fc.mu.Lock()
	defer fc.mu.Unlock()
	_ = fc.ws.WriteMessage(websocket.TextMessage, raw)
}

// sendMessage delivers one broadcast message frame carrying a bench payload.
func (fc *fakeConn) sendMessage(seq int, channel string, data json.RawMessage, pos, mid string, history bool) {
	frame := map[string]any{
		"type": "message", "seq": seq, "ts": 1_757_000_000_000,
		"channel": channel, "data": data, "mid": mid,
	}
	if pos != "" {
		frame["pos"] = pos
	}
	if history {
		frame["history"] = true
	}
	fc.send(frame)
}

// frameTypes returns the ordered type field of every frame the client sent.
func (fc *fakeConn) frameTypes() []string {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	var out []string
	for _, f := range fc.frames {
		if s, ok := f["type"].(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// frameOfType returns the first frame with the given type, or nil.
func (fc *fakeConn) frameOfType(typ string) map[string]any {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	for _, f := range fc.frames {
		if f["type"] == typ {
			return f
		}
	}
	return nil
}
