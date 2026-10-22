package pagerlessmux

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	pagerless "github.com/lcbasu/pagerless-go"
	"github.com/lcbasu/pagerless-go/pagerlesstest"
)

func do(h http.Handler, method, path string) {
	defer func() { _ = recover() }() // no recovery middleware here: the panic reaches the test
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, path, nil))
}

func refund(w http.ResponseWriter, r *http.Request) {
	var m map[string]int
	m[mux.Vars(r)["id"]]++ // nil map write: a real panic
}

func TestMuxRoutesAndPanics(t *testing.T) {
	srv := pagerlesstest.NewServer(t)
	pc, err := pagerless.New("pl_test", pagerless.WithEndpoint(srv.URL()))
	if err != nil {
		t.Fatal(err)
	}
	r := mux.NewRouter()
	r.Use(WithClient(pc))
	r.HandleFunc("/refunds/{id}", refund).Methods("POST")
	r.HandleFunc("/orders/{id:[0-9]+}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}).Methods("GET")

	do(r, "POST", "/refunds/3")
	do(r, "GET", "/orders/1")
	do(r, "GET", "/orders/2")
	_ = pc.Close(context.Background())

	errs := srv.Errors()
	if len(errs) != 1 || errs[0].Route != "POST /refunds/{id}" || !strings.Contains(errs[0].Error.Stack, "pagerless-go/gorillamux.refund") {
		t.Fatalf("panic: %+v", errs)
	}
	got := map[string]int{}
	for _, ev := range srv.Requests() {
		got[ev.Route+" "+http.StatusText(ev.Status)] += ev.Count
	}
	if got["GET /orders/{id:[0-9]+} Accepted"] != 2 || got["POST /refunds/{id} Internal Server Error"] != 1 {
		t.Fatalf("routes: %v", got)
	}
}
