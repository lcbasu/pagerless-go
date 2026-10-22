package pagerless

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"unicode/utf8"
)

// Wire contract: the ingest API's event batch (IngestEvent, IngestBatch). Field names, limits and
// optionality here must match that file exactly; the golden test pins the JSON shape.

// Event types (wire EventType).
const (
	typeError     = "error"
	typeMetric    = "metric"
	typeHeartbeat = "heartbeat"
	typeDeploy    = "deploy"
	typeCustom    = "custom"
)

// Severity is how loud a custom event is (wire Level).
type Severity string

const (
	// Wake pages a person now.
	Wake Severity = "wake"
	// Tell notifies without waking anyone.
	Tell Severity = "tell"
	// Log records the event only.
	Log Severity = "log"
)

func (s Severity) valid() bool { return s == Wake || s == Tell || s == Log }

// Wire limits from IngestEvent.
const (
	maxID          = 64
	maxService     = 100
	maxEnv         = 40
	maxRelease     = 100
	maxFingerprint = 128
	maxRoute       = 300
	maxErrType     = 200
	maxErrMessage  = 2000
	maxStack       = 16000
	maxMetricName  = 100
	maxMetricUnit  = 20
	maxTagKey      = 64
	maxTagValue    = 256
	maxTitle       = 200
	maxWindowS     = 3600
	maxBatch       = 500
	maxTags        = 50
)

type wireError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Stack   string `json:"stack,omitempty"`
}

type wireMetric struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
	Unit  string  `json:"unit,omitempty"`
}

// wireEvent is one IngestEvent. Field order follows wire.ts.
type wireEvent struct {
	V           int               `json:"v"`
	ID          string            `json:"id"`
	Type        string            `json:"type"`
	TS          int64             `json:"ts"`
	Service     string            `json:"service"`
	Env         string            `json:"env"`
	Release     string            `json:"release,omitempty"`
	Fingerprint string            `json:"fingerprint,omitempty"`
	Route       string            `json:"route,omitempty"`
	Count       int               `json:"count"`
	WindowS     int               `json:"window_s,omitempty"`
	Status      int               `json:"status,omitempty"`
	Error       *wireError        `json:"error,omitempty"`
	Metric      *wireMetric       `json:"metric,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	Severity    Severity          `json:"severity,omitempty"`
	Title       string            `json:"title,omitempty"`
}

type wireBatch struct {
	Events []wireEvent `json:"events"`
}

// clamp enforces every wire limit so a valid SDK never sends a batch ingest rejects.
func (e *wireEvent) clamp() {
	e.ID = truncate(e.ID, maxID)
	e.Service = truncate(e.Service, maxService)
	e.Env = truncate(e.Env, maxEnv)
	e.Release = truncate(e.Release, maxRelease)
	e.Fingerprint = truncate(e.Fingerprint, maxFingerprint)
	e.Route = truncate(e.Route, maxRoute)
	e.Title = truncate(e.Title, maxTitle)
	if e.Count < 1 {
		e.Count = 1
	}
	if e.WindowS > maxWindowS {
		e.WindowS = maxWindowS
	}
	if e.Status != 0 && (e.Status < 100 || e.Status > 599) {
		e.Status = 0
	}
	if e.Error != nil {
		e.Error.Type = truncate(e.Error.Type, maxErrType)
		e.Error.Message = truncate(e.Error.Message, maxErrMessage)
		e.Error.Stack = truncate(e.Error.Stack, maxStack)
	}
	if e.Metric != nil {
		e.Metric.Name = truncate(e.Metric.Name, maxMetricName)
		e.Metric.Unit = truncate(e.Metric.Unit, maxMetricUnit)
	}
	if !e.Severity.valid() {
		e.Severity = ""
	}
}

// cleanTags scrubs and bounds caller tags. Values under secret-looking keys are redacted.
func cleanTags(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, min(len(in), maxTags))
	for k, v := range in {
		if len(out) >= maxTags {
			break
		}
		if k == "" {
			continue
		}
		k = truncate(k, maxTagKey)
		if secretKey.MatchString(k) {
			out[k] = redacted
			continue
		}
		out[k] = truncate(Scrub(v), maxTagValue)
	}
	return out
}

// truncate cuts s to at most n bytes without splitting a UTF-8 sequence.
// Zod's max() counts UTF-16 units, which is never more than UTF-8 bytes, so this is safe.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	s = s[:n]
	for len(s) > 0 && !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}

// randomID returns a 32-char hex idempotency key.
func randomID() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error on supported platforms (Go 1.24+ panics instead of erroring).
	return hex.EncodeToString(b[:])
}

// stableID derives a deterministic idempotency key so repeated sends (restarts, replicas)
// of the same logical event dedupe at ingest.
func stableID(prefix string, parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return prefix + hex.EncodeToString(h.Sum(nil))[:32]
}
