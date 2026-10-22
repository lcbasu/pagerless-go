package pagerless

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// Scheduled-job check-ins: a job reports each run, and Pagerless tells (or wakes) you once when a
// run is missed or fails.
//
//	err := pagerless.Monitor(ctx, "nightly-backup", pagerless.Schedule{Cron: "30 3 * * *"}, backup)

// Schedule says when a job should report. Set exactly one of Every or Cron.
type Schedule struct {
	// Name is the plain-words name for alerts ("Nightly backup"); default from the slug.
	Name string
	// Every is the time between runs (at least a minute).
	Every time.Duration
	// Cron is a 5-field cron expression ("30 3 * * *") or a macro (@daily).
	Cron string
	// TZ is the IANA zone for Cron; default the account's time zone, else UTC.
	TZ string
	// Grace is how late a run may be before it counts as missed; default a tenth of the gap,
	// 5 minutes to 1 hour.
	Grace time.Duration
	// Level is Tell (default) or Wake. Log is treated as Tell: a job nobody hears about isn't watched.
	Level Severity
}

// CheckInStatus is the outcome of one run.
type CheckInStatus string

const (
	// CheckInOK is a run that finished fine.
	CheckInOK CheckInStatus = "ok"
	// CheckInFailed is a run that failed; Pagerless opens a problem at once.
	CheckInFailed CheckInStatus = "failed"
)

// ErrBadSlug is returned for a job name that isn't 1-64 letters, digits, '.', '_' or '-'.
var ErrBadSlug = errors.New("pagerless: check-in name must be 1-64 letters, digits, '.', '_' or '-'")

var slugRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

type wireSchedule struct {
	Name               string `json:"name,omitempty"`
	ExpectEverySeconds int64  `json:"expectEverySeconds,omitempty"`
	Cron               string `json:"cron,omitempty"`
	TZ                 string `json:"tz,omitempty"`
	GraceSeconds       *int64 `json:"graceSeconds,omitempty"`
	Level              string `json:"level,omitempty"`
}

// checkInBody is the check-in request body of the ingest API.
type checkInBody struct {
	Status     CheckInStatus `json:"status"`
	DurationMs *int64        `json:"durationMs,omitempty"`
	Message    string        `json:"message,omitempty"`
	Schedule   *wireSchedule `json:"schedule,omitempty"`
	ID         string        `json:"id,omitempty"`
	Env        string        `json:"env,omitempty"`
}

// CheckInOption sets a field of one check-in.
type CheckInOption func(*checkInBody)

// WithSchedule creates the check-in on first use and keeps it in step (unless edited in the app).
func WithSchedule(s Schedule) CheckInOption {
	return func(b *checkInBody) { b.Schedule = s.wire() }
}

// Failed marks the run failed, with the reason (scrubbed before sending).
func Failed(reason string) CheckInOption {
	return func(b *checkInBody) {
		b.Status = CheckInFailed
		b.Message = truncate(Scrub(reason), 500)
	}
}

// Took records how long the run took.
func Took(d time.Duration) CheckInOption {
	return func(b *checkInBody) {
		ms := max(d.Milliseconds(), 0)
		b.DurationMs = &ms
	}
}

// RunID is an idempotency key for this run: a retry with the same id counts once.
func RunID(id string) CheckInOption { return func(b *checkInBody) { b.ID = truncate(id, maxID) } }

func (s Schedule) wire() *wireSchedule {
	w := &wireSchedule{Name: truncate(s.Name, 100), Cron: strings.TrimSpace(s.Cron), TZ: s.TZ}
	if w.Cron == "" && s.Every > 0 {
		w.ExpectEverySeconds = max(int64(s.Every/time.Second), 60)
	}
	if s.Grace > 0 {
		g := int64(s.Grace / time.Second)
		w.GraceSeconds = &g
	}
	if s.Level == Wake {
		w.Level = string(Wake)
	} else {
		w.Level = string(Tell)
	}
	return w
}

// checkInURL turns the events URL into the check-in URL on the same host and prefix:
// ".../v1/events" → ".../v1/checkins/<slug>".
func checkInURL(eventsURL, slug string) string {
	base := strings.TrimSuffix(strings.TrimSuffix(eventsURL, "/"), "/v1/events")
	return strings.TrimSuffix(base, "/") + "/v1/checkins/" + url.PathEscape(slug)
}

// CheckIn reports one run of a scheduled job, at once (not batched: jobs often exit right after).
// It returns ErrNoKey when reporting is off and never panics.
func (c *Client) CheckIn(ctx context.Context, slug string, opts ...CheckInOption) (err error) {
	if !c.active() {
		return ErrNoKey
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("pagerless: check-in: %v", r)
		}
	}()
	name := strings.ToLower(strings.TrimSpace(slug))
	if !slugRE.MatchString(name) {
		return ErrBadSlug
	}
	b := checkInBody{Status: CheckInOK, Env: c.cfg.env}
	for _, o := range opts {
		if o != nil {
			o(&b)
		}
	}
	body, err := json.Marshal(b)
	if err != nil {
		return fmt.Errorf("pagerless: encode check-in: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, checkInURL(c.cfg.endpoint, name), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("pagerless: build check-in: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.cfg.httpClient.Do(req)
	if err != nil {
		c.cfg.logger.Warn("pagerless: check-in not sent", "job", name, "error", err)
		return fmt.Errorf("pagerless: check-in: %w", err)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		c.cfg.logger.Warn("pagerless: check-in refused", "job", name, "status", resp.StatusCode)
		return fmt.Errorf("pagerless: check-in returned %d", resp.StatusCode)
	}
	return nil
}

// Monitor runs fn and reports the run: ok with its run time, or failed with fn's error (or panic,
// which is re-raised after reporting). It returns fn's error; a check-in that couldn't be sent is
// logged, never returned, so it can't hide the job's own result.
func (c *Client) Monitor(ctx context.Context, slug string, s Schedule, fn func(context.Context) error) error {
	now := time.Now
	if c != nil && c.cfg.now != nil {
		now = c.cfg.now
	}
	start := now()
	done := false
	defer func() {
		if done {
			return
		}
		if r := recover(); r != nil {
			_ = c.CheckIn(context.WithoutCancel(ctx), slug, WithSchedule(s), Took(now().Sub(start)), Failed(fmt.Sprintf("panic: %v", r)))
			panic(r)
		}
	}()
	runErr := fn(ctx)
	done = true
	opts := []CheckInOption{WithSchedule(s), Took(now().Sub(start))}
	if runErr != nil {
		opts = append(opts, Failed(runErr.Error()))
	}
	// The job's context may be done (it timed out); the report must still go out.
	_ = c.CheckIn(context.WithoutCancel(ctx), slug, opts...)
	return runErr
}

// CheckIn reports one run on the default client.
func CheckIn(ctx context.Context, slug string, opts ...CheckInOption) error {
	return std.Load().CheckIn(ctx, slug, opts...)
}

// Monitor runs fn and reports the run on the default client.
func Monitor(ctx context.Context, slug string, s Schedule, fn func(context.Context) error) error {
	return std.Load().Monitor(ctx, slug, s, fn)
}
