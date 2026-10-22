package pagerlesschi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	pagerless "github.com/lcbasu/pagerless-go"
	"github.com/lcbasu/pagerless-go/pagerlesstest"
)

func do(h http.Handler, method, path string) int {
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(method, path, nil))
	return rr.Code
}

func refund(w http.ResponseWriter, r *http.Request) {
	var m map[string]int
	m[chi.URLParam(r, "id")]++ // nil map write: a real panic
}

func TestChiRoutesAndPanics(t *testing.T) {
	srv := pagerlesstest.NewServer(t)
	pc, err := pagerless.New("pl_test", pagerless.WithEndpoint(srv.URL()))
	if err != nil {
		t.Fatal(err)
	}
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(WithClient(pc))
	r.Post("/refunds/{id}", refund)
	r.Route("/v1", func(r chi.Router) {
		r.Get("/orders/{id}", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) })
	})

	if code := do(r, "POST", "/refunds/3"); code != 500 {
		t.Fatalf("Recoverer still answers 500, got %d", code)
	}
	do(r, "GET", "/v1/orders/1")
	do(r, "GET", "/v1/orders/2")
	do(r, "GET", "/nope/77")
	_ = pc.Close(context.Background())

	errs := srv.Errors()
	if len(errs) != 1 || errs[0].Route != "POST /refunds/{id}" || !strings.Contains(errs[0].Error.Stack, "pagerless-go/chi.refund") {
		t.Fatalf("panic: %+v", errs)
	}
	got := map[string]int{}
	for _, ev := range srv.Requests() {
		got[ev.Route+" "+http.StatusText(ev.Status)] += ev.Count
	}
	for k, n := range map[string]int{
		"POST /refunds/{id} Internal Server Error": 1,
		"GET /v1/orders/{id} OK":                   2,
		"GET (unmatched) Not Found":                1,
	} {
		if got[k] != n {
			t.Errorf("%s: got %d want %d (all %v)", k, got[k], n, got)
		}
	}
}
