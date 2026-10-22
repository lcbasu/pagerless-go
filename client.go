// Package pagerless reports errors, request health, heartbeats and deploys from a Go service to
// Pagerless.
//
//	func main() {
//		pagerless.Init("") // reads PAGERLESS_KEY
//		defer pagerless.Close(context.Background())
//		mux := http.NewServeMux()
//		mux.HandleFunc("GET /orders/{id}", getOrder)
//		http.ListenAndServe(":8080", pagerless.Middleware(mux))
//	}
//
// The SDK groups repeats in-process (a new problem is sent at once, repeats are counted per
// 10 s window), scrubs secrets and personal data before anything leaves the process, never
// blocks the caller, and never panics into user code. When the endpoint is down it backs off and
// drops the oldest data first once its bounded buffer is full.
package pagerless

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultEndpoint is the ingest event API.
const DefaultEndpoint = "https://events.pagerless.dev/v1/events" // the ingest API's public address

// Defaults, the same as the JS SDK.
// Errors captured within startupWindow of the client starting carry the tag phase=startup (unless
// the app set its own phase). Pagerless counts repeated startup errors across restarts as a crash
// loop. Same window as the JS SDK (STARTUP_WINDOW_MS).
const (
	startupWindow = 30 * time.Second
	tagPhase      = "phase"
	phaseStartup  = "startup"
)

const (
	defaultWindow    = 10 * time.Second
	defaultHeartbeat = 60 * time.Second
	defaultTimeout   = 10 * time.Second
	defaultMaxQueue  = 2_000
)

// ErrNoKey is returned by Init and New when no key is given and PAGERLESS_KEY is unset.
// The returned client is still usable; it records nothing.
var ErrNoKey = errors.New("pagerless: no key (pass one to Init or set PAGERLESS_KEY); reporting is off")

type config struct {
	key        string
	endpoint   string
	service    string
	env        string
	release    string
	httpClient *http.Client
	window     time.Duration
	heartbeat  time.Duration
	maxQueue   int
	logger     *slog.Logger
	now        func() time.Time
	newID      func() string
	jitter     func(time.Duration) time.Duration
	background bool // run the sender goroutine; tests turn it off to drive the loop by hand
}

// Option configures Init / New.
type Option func(*config)

// WithEndpoint overrides the ingest URL (default PAGERLESS_ENDPOINT, then DefaultEndpoint).
func WithEndpoint(url string) Option { return func(c *config) { c.endpoint = url } }

// WithService names the service (default PAGERLESS_SERVICE, then the executable name).
func WithService(name string) Option { return func(c *config) { c.service = name } }

// WithEnv sets the environment (default PAGERLESS_ENV, then "production").
func WithEnv(env string) Option { return func(c *config) { c.env = env } }

// WithRelease sets the release (default PAGERLESS_RELEASE, then the VCS revision in the build info).
// A known release is sent once as a deploy marker at Init.
func WithRelease(release string) Option { return func(c *config) { c.release = release } }

// WithHTTPClient sets the HTTP client used to send batches.
func WithHTTPClient(hc *http.Client) Option { return func(c *config) { c.httpClient = hc } }

// WithWindow sets the grouping window (default 10 s).
func WithWindow(d time.Duration) Option { return func(c *config) { c.window = d } }

// WithHeartbeat sets the heartbeat interval (default 60 s).
func WithHeartbeat(d time.Duration) Option { return func(c *config) { c.heartbeat = d } }

// WithMaxQueue bounds how many unsent events are buffered (default 2000).
func WithMaxQueue(n int) Option { return func(c *config) { c.maxQueue = n } }

// WithLogger receives SDK diagnostics (send failures, drops). Default: discarded.
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// Client reports to one Pagerless project. Use the package-level functions unless you need more
// than one client. All methods are safe for concurrent use and on a nil *Client.
type Client struct {
	cfg       config
	enabled   bool
	startedAt time.Time

	mu          sync.Mutex
	agg         *aggregator
	queue       []wireEvent
	dropped     int // events dropped since the last heartbeat
	failures    int
	nextAttempt time.Time
	closed      bool

	wake    chan struct{}
	done    chan struct{}
	exited  chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
	closeMu sync.Once
}

// New builds a client. With no key it returns a disabled client and ErrNoKey.
func New(key string, opts ...Option) (*Client, error) {
	cfg := config{
		key:        key,
		httpClient: &http.Client{Timeout: defaultTimeout},
		window:     defaultWindow,
		heartbeat:  defaultHeartbeat,
		maxQueue:   defaultMaxQueue,
		now:        time.Now,
		newID:      randomID,
		jitter:     equalJitter,
		background: true,
	}
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	applyEnv(&cfg)

	start := cfg.now()
	c := &Client{
		cfg:       cfg,
		startedAt: start,
		agg:       newAggregator(start),
		wake:      make(chan struct{}, 1),
		done:      make(chan struct{}),
		exited:    make(chan struct{}),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	if cfg.key == "" {
		close(c.exited)
		return c, ErrNoKey
	}
	c.enabled = true
	c.enqueueHeartbeat()
	if cfg.release != "" {
		c.Deploy(cfg.release)
	}
	if cfg.background {
		go c.run()
	} else {
		close(c.exited)
	}
	return c, nil
}

func applyEnv(c *config) {
	if c.key == "" {
		c.key = os.Getenv("PAGERLESS_KEY")
	}
	c.endpoint = firstNonEmpty(c.endpoint, os.Getenv("PAGERLESS_ENDPOINT"), DefaultEndpoint)
	c.service = firstNonEmpty(c.service, os.Getenv("PAGERLESS_SERVICE"), exeName(), "app")
	c.env = firstNonEmpty(c.env, os.Getenv("PAGERLESS_ENV"), "production")
	c.release = firstNonEmpty(c.release, os.Getenv("PAGERLESS_RELEASE"), vcsRevision())
	if c.httpClient == nil {
		c.httpClient = &http.Client{Timeout: defaultTimeout}
	}
	if c.window <= 0 {
		c.window = defaultWindow
	}
	if c.heartbeat <= 0 {
		c.heartbeat = defaultHeartbeat
	}
	if c.maxQueue <= 0 {
		c.maxQueue = defaultMaxQueue
	}
	if c.logger == nil {
		c.logger = slog.New(slog.DiscardHandler)
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func exeName() string {
	p, err := os.Executable()
	if err != nil {
		return ""
	}
	return filepath.Base(p)
}

func vcsRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" && len(s.Value) >= 12 {
			return s.Value[:12]
		}
	}
	return ""
}

// guard keeps SDK bugs out of user code: any internal panic is swallowed and logged.
func (c *Client) guard() {
	if r := recover(); r != nil && c != nil && c.cfg.logger != nil {
		c.cfg.logger.Error("pagerless: internal error, event dropped", "panic", fmt.Sprint(r))
	}
}

func (c *Client) active() bool { return c != nil && c.enabled }

// stamp fills the envelope fields, scrubs free text and enforces wire limits.
func (c *Client) stamp(ev *wireEvent) {
	ev.V = 1
	if ev.ID == "" {
		ev.ID = c.cfg.newID()
	}
	if ev.TS == 0 {
		ev.TS = c.cfg.now().UnixMilli()
	}
	ev.Service = c.cfg.service
	ev.Env = c.cfg.env
	ev.Release = c.cfg.release
	ev.Route = Scrub(ev.Route)
	ev.Title = Scrub(ev.Title)
	if ev.Error != nil {
		e := *ev.Error
		e.Message = Scrub(e.Message)
		e.Stack = Scrub(e.Stack)
		ev.Error = &e
	}
	ev.clamp()
}

// CaptureOption adds context to Capture.
type CaptureOption func(*wireEvent)

// Tags attaches tags (scrubbed; values under secret-looking keys are redacted).
func Tags(tags map[string]string) CaptureOption {
	return func(e *wireEvent) { e.Tags = cleanTags(tags) }
}

// Route sets the route the error happened on, e.g. "POST /checkout".
func Route(route string) CaptureOption { return func(e *wireEvent) { e.Route = route } }

// Status sets the HTTP status the error produced.
func Status(code int) CaptureOption { return func(e *wireEvent) { e.Status = code } }

// Capture reports err. Repeats of the same problem (type + call site + route) are counted, not
// re-sent. Safe to call with a nil error (no-op).
func (c *Client) Capture(err error, opts ...CaptureOption) {
	if !c.active() || err == nil {
		return
	}
	defer c.guard()
	c.captureError(errType(err), err.Error(), callers(1), opts)
}

func (c *Client) captureError(typ, msg string, frames []frame, opts []CaptureOption) {
	ev := wireEvent{
		Type:  typeError,
		Count: 1,
		Error: &wireError{Type: typ, Message: msg, Stack: formatStack(frames)},
	}
	for _, o := range opts {
		if o != nil {
			o(&ev)
		}
	}
	if _, set := ev.Tags[tagPhase]; !set && c.cfg.now().Sub(c.startedAt) < startupWindow {
		tags := make(map[string]string, len(ev.Tags)+1)
		maps.Copy(tags, ev.Tags)
		tags[tagPhase] = phaseStartup
		ev.Tags = tags
	}
	ev.Fingerprint = fingerprint(typeError, typ, ev.Route, frames)
	c.record(ev)
}

// Event reports a custom event. severity is wake, tell or log; an invalid severity is sent as
// tell (unsure means tell, never log).
func (c *Client) Event(title string, severity Severity, tags map[string]string) {
	if !c.active() {
		return
	}
	defer c.guard()
	if !severity.valid() {
		severity = Tell
	}
	ev := wireEvent{Type: typeCustom, Count: 1, Title: title, Severity: severity, Tags: cleanTags(tags)}
	ev.Fingerprint = fingerprint(typeCustom, string(severity), Scrub(title), nil)
	c.record(ev)
}

// Deploy sends a deploy marker for release. The ID is derived from service, env and release, so
// every replica and restart of one release collapses to a single marker at ingest.
func (c *Client) Deploy(release string) {
	if !c.active() || release == "" {
		return
	}
	defer c.guard()
	ev := wireEvent{
		ID:    stableID("dep_", c.cfg.service, c.cfg.env, release),
		Type:  typeDeploy,
		Count: 1,
		Title: truncate("Deployed "+release, maxTitle),
	}
	c.stamp(&ev)
	ev.Release = truncate(release, maxRelease)
	c.mu.Lock()
	c.enqueueLocked(ev)
	c.mu.Unlock()
	c.signal()
}

// record routes an error/custom event through the aggregator.
func (c *Client) record(ev wireEvent) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	sendNow := c.agg.addOccurrence(ev)
	if sendNow {
		c.stamp(&ev)
		c.enqueueLocked(ev)
	}
	c.mu.Unlock()
	if sendNow {
		c.signal()
	}
}

// recordRequest feeds one finished HTTP request into the window.
func (c *Client) recordRequest(route string, status int, latency time.Duration) {
	if !c.active() {
		return
	}
	defer c.guard()
	c.mu.Lock()
	if !c.closed {
		c.agg.addRequest(route, status, float64(latency.Microseconds())/1000)
	}
	c.mu.Unlock()
}

// enqueueLocked appends ev to the send buffer, dropping the oldest event when full: under a long
// outage the most recent state of the app is what the person needs.
func (c *Client) enqueueLocked(ev wireEvent) {
	if len(c.queue) >= c.cfg.maxQueue {
		over := len(c.queue) - c.cfg.maxQueue + 1
		c.queue = append(c.queue[:0], c.queue[over:]...)
		c.dropped += over
	}
	c.queue = append(c.queue, ev)
}

func (c *Client) enqueueHeartbeat() {
	c.mu.Lock()
	defer c.mu.Unlock()
	ev := wireEvent{Type: typeHeartbeat, Count: 1}
	dropped := c.dropped + c.agg.takeOverflow()
	if dropped > 0 {
		ev.Tags = map[string]string{"dropped": fmt.Sprint(dropped)}
		c.dropped = 0
	}
	c.stamp(&ev)
	c.enqueueLocked(ev)
}

// flushWindow moves the window's aggregated groups into the send buffer.
func (c *Client) flushWindow() {
	c.mu.Lock()
	evs := c.agg.flush(c.cfg.now())
	for i := range evs {
		c.stamp(&evs[i])
		c.enqueueLocked(evs[i])
	}
	c.mu.Unlock()
}

func (c *Client) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// Close flushes the current window and tries to deliver everything buffered until ctx ends.
// Events recorded after Close are ignored.
func (c *Client) Close(ctx context.Context) error {
	if !c.active() {
		return nil
	}
	defer c.guard()
	var err error
	c.closeMu.Do(func() {
		close(c.done)
		select {
		case <-c.exited:
		case <-ctx.Done():
			c.cancel()
			<-c.exited
		}
		c.flushWindow()
		c.mu.Lock()
		c.closed = true
		c.nextAttempt = time.Time{} // last chance: try now even if backing off
		c.mu.Unlock()
		err = c.drain(ctx)
		c.cancel()
	})
	return err
}

// drain sends until the buffer is empty, a permanent error clears it, or ctx ends.
func (c *Client) drain(ctx context.Context) error {
	for {
		wait, more := c.sendPending(ctx)
		if !more {
			return nil
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			c.mu.Lock()
			n := len(c.queue)
			c.mu.Unlock()
			return fmt.Errorf("pagerless: close: %d events not delivered: %w", n, ctx.Err())
		case <-t.C:
		}
	}
}

// run is the single sender goroutine.
func (c *Client) run() {
	defer close(c.exited)
	window := time.NewTicker(c.cfg.window)
	defer window.Stop()
	hb := time.NewTicker(c.cfg.heartbeat)
	defer hb.Stop()
	retry := time.NewTimer(time.Hour)
	retry.Stop()
	defer retry.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-c.wake:
		case <-retry.C:
		case <-window.C:
			c.flushWindow()
		case <-hb.C:
			c.enqueueHeartbeat()
		}
		if wait, more := c.sendPending(c.ctx); more {
			retry.Reset(wait)
		}
	}
}

// --- package-level default client ---

var std atomic.Pointer[Client]

// Init starts the default client. key falls back to PAGERLESS_KEY and the endpoint to
// PAGERLESS_ENDPOINT. Without a key, reporting is off and ErrNoKey is returned; the rest of the
// API keeps working as a no-op. Calling Init again replaces (and closes) the previous client.
func Init(key string, opts ...Option) error {
	c, err := New(key, opts...)
	if old := std.Swap(c); old != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = old.Close(ctx)
		}()
	}
	return err
}

// Close flushes and stops the default client.
func Close(ctx context.Context) error { return std.Load().Close(ctx) }

// Capture reports err on the default client.
func Capture(err error, opts ...CaptureOption) {
	c := std.Load()
	if !c.active() || err == nil {
		return
	}
	defer c.guard()
	c.captureError(errType(err), err.Error(), callers(1), opts)
}

// Event reports a custom event on the default client.
func Event(title string, severity Severity, tags map[string]string) {
	std.Load().Event(title, severity, tags)
}

// Deploy sends a deploy marker on the default client.
func Deploy(release string) { std.Load().Deploy(release) }
