// Package pagerlesstest is a fake Pagerless ingest for tests: point a client at it, run the code
// under test, close the client, and read what would have been sent.
//
//	srv := pagerlesstest.NewServer(t)
//	c, _ := pagerless.New("pl_test", pagerless.WithEndpoint(srv.URL()))
//	… exercise the app with c …
//	_ = c.Close(ctx)
//	errs := srv.Errors()
package pagerlesstest

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// Event is one event as ingest receives it (the fields tests usually look at).
type Event struct {
	ID          string            `json:"id"`
	Type        string            `json:"type"`
	Count       int               `json:"count"`
	Route       string            `json:"route"`
	Status      int               `json:"status"`
	Title       string            `json:"title"`
	Fingerprint string            `json:"fingerprint"`
	Tags        map[string]string `json:"tags"`
	Error       *struct {
		Type    string `json:"type"`
		Message string `json:"message"`
		Stack   string `json:"stack"`
	} `json:"error"`
	Metric *struct {
		Name  string  `json:"name"`
		Value float64 `json:"value"`
	} `json:"metric"`
}

// CheckIn is one check-in request: the job name from the path and the JSON body.
type CheckIn struct {
	Slug       string `json:"-"`
	Status     string `json:"status"`
	DurationMs *int64 `json:"durationMs"`
	Message    string `json:"message"`
	ID         string `json:"id"`
	Schedule   *struct {
		Cron               string `json:"cron"`
		TZ                 string `json:"tz"`
		ExpectEverySeconds int64  `json:"expectEverySeconds"`
	} `json:"schedule"`
}

// Server records event batches (POST …/v1/events) and check-ins (POST …/v1/checkins/<job>).
type Server struct {
	srv      *httptest.Server
	mu       sync.Mutex
	events   []Event
	checkIns []CheckIn
}

// NewServer starts a fake ingest that is closed when the test ends.
func NewServer(t testing.TB) *Server {
	s := &Server{}
	s.srv = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.srv.Close)
	return s
}

// URL is the events endpoint to pass to pagerless.WithEndpoint.
func (s *Server) URL() string { return s.srv.URL + "/v1/events" }

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	defer s.mu.Unlock()
	if slug, ok := strings.CutPrefix(r.URL.Path, "/v1/checkins/"); ok {
		var ci CheckIn
		if err := json.Unmarshal(body, &ci); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ci.Slug = slug
		s.checkIns = append(s.checkIns, ci)
		w.WriteHeader(http.StatusAccepted)
		return
	}
	var batch struct {
		Events []Event `json:"events"`
	}
	if err := json.Unmarshal(body, &batch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.events = append(s.events, batch.Events...)
	w.WriteHeader(http.StatusAccepted)
}

// Events returns every event received so far.
func (s *Server) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events...)
}

// Errors returns the error events received so far.
func (s *Server) Errors() []Event { return s.ofType("error") }

// Requests returns the per-route request summaries (route, status, count) received so far.
func (s *Server) Requests() []Event {
	var out []Event
	for _, e := range s.ofType("metric") {
		if e.Metric != nil && e.Metric.Name == "http.server.duration" {
			out = append(out, e)
		}
	}
	return out
}

func (s *Server) ofType(t string) []Event {
	var out []Event
	for _, e := range s.Events() {
		if e.Type == t {
			out = append(out, e)
		}
	}
	return out
}

// CheckIns returns the check-ins received so far.
func (s *Server) CheckIns() []CheckIn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]CheckIn(nil), s.checkIns...)
}
