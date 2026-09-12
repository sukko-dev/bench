// Package restpub implements the publisher's SendFunc over the gateway's REST
// publish endpoint (POST /api/v1/publish, Community — no edition gate). It
// wraps the bench payload as the message `data` and carries the JWT as a
// bearer token.
package restpub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sukko-dev/bench/internal/wire"
)

// Publisher posts bench messages to a gateway.
type Publisher struct {
	endpoint string
	token    string
	client   *http.Client
}

// New builds a Publisher for the gateway base URL (scheme://host, no path).
func New(baseURL, token string) *Publisher {
	return &Publisher{
		endpoint: strings.TrimRight(baseURL, "/") + "/api/v1/publish",
		token:    token,
		client:   &http.Client{},
	}
}

// RateLimitedError is returned when the gateway answers 429. After carries the parsed
// Retry-After header (zero when absent or unparseable) so the retry loop can wait exactly
// as long as the server asked — an immediate retry lands in the same empty token bucket.
type RateLimitedError struct {
	Channel string
	After   time.Duration
}

func (e *RateLimitedError) Error() string {
	return fmt.Sprintf("restpub: publish %s rate limited (HTTP 429, retry after %s)", e.Channel, e.After)
}

// RetryAfter reports the server's requested wait; zero means "no hint, use your own backoff".
func (e *RateLimitedError) RetryAfter() time.Duration { return e.After }

type publishRequest struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

// Send posts one bench payload to channel. It is the pub.SendFunc: an error
// means this attempt did not confirm. The payload must be a bench payload —
// sending anything else would put unattributable data on the channel.
func (p *Publisher) Send(ctx context.Context, channel string, payload []byte) error {
	if _, err := wire.Decode(payload); err != nil {
		return fmt.Errorf("restpub: refusing to send a non-bench payload: %w", err)
	}
	body, err := json.Marshal(publishRequest{Channel: channel, Data: json.RawMessage(payload)})
	if err != nil {
		return fmt.Errorf("restpub: marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("restpub: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.token)

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("restpub: publish %s: %w", channel, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		_, _ = io.Copy(io.Discard, resp.Body) // drain for connection reuse
		var after time.Duration
		if secs, perr := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); perr == nil && secs > 0 {
			after = time.Duration(secs) * time.Second
		}
		return &RateLimitedError{Channel: channel, After: after}
	}
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("restpub: publish %s got HTTP %d: %s", channel, resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	_, _ = io.Copy(io.Discard, resp.Body) // drain for connection reuse
	return nil
}
