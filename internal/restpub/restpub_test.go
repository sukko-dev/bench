package restpub

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sukko-dev/bench/internal/wire"
)

func TestPublishPostsChannelAndDataWithBearer(t *testing.T) {
	var gotAuth, gotChannel, gotCT string
	var gotData json.RawMessage
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Channel string          `json:"channel"`
			Data    json.RawMessage `json:"data"`
		}
		_ = json.Unmarshal(body, &req)
		gotChannel, gotData = req.Channel, req.Data
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"status":"accepted","mid":"abc-1"}`))
	}))
	defer srv.Close()

	p := New(srv.URL, "jwt-xyz")
	payload, _ := wire.Encode(wire.Payload{Run: "r1", Channel: "t.a", Seq: 1, IntendedUnixNano: 42}, 200)
	if err := p.Send(context.Background(), "t.a", payload); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if gotAuth != "Bearer jwt-xyz" {
		t.Errorf("Authorization = %q, want Bearer jwt-xyz", gotAuth)
	}
	if !strings.HasPrefix(gotCT, "application/json") {
		t.Errorf("Content-Type = %q", gotCT)
	}
	if gotChannel != "t.a" {
		t.Errorf("channel = %q, want t.a", gotChannel)
	}
	// The bench payload is nested under data as-is, decodable at the far end.
	if got, err := wire.Decode(gotData); err != nil || got.Seq != 1 {
		t.Errorf("data round-trip failed: %+v err=%v", got, err)
	}
}

func TestPublishNon200IsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		_, _ = w.Write([]byte(`{"code":"RATE_LIMITED","message":"slow down"}`))
	}))
	defer srv.Close()

	p := New(srv.URL, "jwt")
	err := p.Send(context.Background(), "t.a", []byte(`{"bench_run":"r","bench_ch":"t.a","bench_seq":1,"bench_t":1}`))
	if err == nil {
		t.Fatal("Send accepted a 429 response")
	}
	if !strings.Contains(err.Error(), "429") {
		t.Errorf("error should name the status: %v", err)
	}
}

func TestPublishContextCancelPropagates(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := New(srv.URL, "jwt")
	if err := p.Send(ctx, "t.a", []byte(`{}`)); err == nil {
		t.Fatal("Send ignored a cancelled context")
	}
	if calls.Load() != 0 {
		t.Errorf("request issued despite cancelled context")
	}
}

func TestPublishRejectsNonBenchPayload(t *testing.T) {
	// Guard against sending a payload the far end can't attribute to this run —
	// a bug in the caller would otherwise pollute another run's channel.
	p := New("http://unused", "jwt")
	if err := p.Send(context.Background(), "t.a", []byte(`{"price":1.9}`)); err == nil {
		t.Fatal("Send accepted a non-bench payload")
	}
}
