package pagerless

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

const (
	backoffBase = time.Second
	backoffMax  = 5 * time.Minute
	userAgent   = "pagerless-go/0.1"
)

// sendResult classifies one POST.
type sendResult int

const (
	sentOK    sendResult = iota
	retryable            // network error, 408, 429, 5xx: keep the batch, back off
	permanent            // other 4xx: the batch can never succeed, drop it loudly
)

// sendPending posts buffered events in batches of up to maxBatch. It returns more=true with the
// time to wait when events remain (backing off or more to send after a failure).
func (c *Client) sendPending(ctx context.Context) (wait time.Duration, more bool) {
	for {
		c.mu.Lock()
		if len(c.queue) == 0 {
			c.mu.Unlock()
			return 0, false
		}
		if now := c.cfg.now(); now.Before(c.nextAttempt) {
			wait = c.nextAttempt.Sub(now)
			c.mu.Unlock()
			return wait, true
		}
		n := min(len(c.queue), maxBatch)
		batch := make([]wireEvent, n)
		copy(batch, c.queue[:n])
		c.mu.Unlock()

		res, retryAfter, err := c.post(ctx, batch)

		c.mu.Lock()
		switch res {
		case sentOK, permanent:
			// The queue may have dropped from its head while we were sending; remove only what
			// is still ours (matched by ID).
			c.queue = removeSent(c.queue, batch)
			c.failures = 0
			c.nextAttempt = time.Time{}
			if res == permanent {
				c.cfg.logger.Error("pagerless: ingest rejected batch, dropped", "events", n, "err", err)
			}
			c.mu.Unlock()
		case retryable:
			c.failures++
			d := c.backoff(c.failures)
			if retryAfter > d {
				d = min(retryAfter, backoffMax)
			}
			c.nextAttempt = c.cfg.now().Add(d)
			c.cfg.logger.Warn("pagerless: send failed, will retry", "events", n, "retry_in", d, "err", err)
			c.mu.Unlock()
			return d, true
		}
	}
}

// removeSent drops the leading events of q that belong to batch.
func removeSent(q, batch []wireEvent) []wireEvent {
	if len(batch) == 0 {
		return q
	}
	ids := make(map[string]struct{}, len(batch))
	for _, e := range batch {
		ids[e.ID] = struct{}{}
	}
	i := 0
	for i < len(q) {
		if _, ok := ids[q[i].ID]; !ok {
			break
		}
		i++
	}
	return append(q[:0], q[i:]...)
}

// backoff is exponential from 1 s, capped at 5 min, with jitter so a fleet doesn't retry in step.
func (c *Client) backoff(failures int) time.Duration {
	d := backoffBase << min(failures-1, 16)
	if d <= 0 || d > backoffMax {
		d = backoffMax
	}
	return c.cfg.jitter(d)
}

// equalJitter returns a duration in [d/2, d).
func equalJitter(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(half)
}

func (c *Client) post(ctx context.Context, batch []wireEvent) (sendResult, time.Duration, error) {
	body, err := json.Marshal(wireBatch{Events: batch})
	if err != nil {
		return permanent, 0, fmt.Errorf("encode batch: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.endpoint, bytes.NewReader(body))
	if err != nil {
		return permanent, 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.cfg.httpClient.Do(req)
	if err != nil {
		return retryable, 0, err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)) // drain for connection reuse
	_ = resp.Body.Close()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return sentOK, 0, nil
	case resp.StatusCode == http.StatusRequestTimeout, resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode >= 500:
		return retryable, parseRetryAfter(resp.Header.Get("Retry-After")), fmt.Errorf("ingest returned %d", resp.StatusCode)
	default:
		return permanent, 0, fmt.Errorf("ingest returned %d", resp.StatusCode)
	}
}

func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if s, err := strconv.Atoi(v); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}
