package pagerlessgrpc

import (
	"context"
	"net"
	"strconv"
	"strings"
	"testing"

	pagerless "github.com/lcbasu/pagerless-go"
	"github.com/lcbasu/pagerless-go/pagerlesstest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// shop answers Check by the requested service name, so one RPC exercises every outcome.
type shop struct {
	healthpb.UnimplementedHealthServer
}

func chargeCard(service string) {
	var m map[string]int
	m[service]++ // nil map write: a real panic
}

func (shop) Check(_ context.Context, r *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	switch r.Service {
	case "panic":
		chargeCard(r.Service)
	case "internal":
		return nil, status.Error(codes.Internal, "ledger write failed")
	case "notfound":
		return nil, status.Error(codes.NotFound, "no such shop")
	case "down":
		return nil, status.Error(codes.Unavailable, "warehouse offline")
	}
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (shop) Watch(_ *healthpb.HealthCheckRequest, _ healthpb.Health_WatchServer) error {
	return status.Error(codes.DataLoss, "stream torn")
}

// recovery stands in for the app's own recovery interceptor, outside ours.
func recovery(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if recover() != nil {
			err = status.Error(codes.Internal, "internal")
		}
	}()
	return h(ctx, req)
}

func TestServerAndClientInterceptors(t *testing.T) {
	ingest := pagerlesstest.NewServer(t)
	pc, err := pagerless.New("pl_test", pagerless.WithEndpoint(ingest.URL()))
	if err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(append([]grpc.ServerOption{grpc.UnaryInterceptor(recovery)}, ServerOptions(WithClient(pc))...)...)
	healthpb.RegisterHealthServer(srv, shop{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dial := append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, DialOptions(WithClient(pc))...)
	conn, err := grpc.Dial("passthrough:///bufnet", dial...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	hc := healthpb.NewHealthClient(conn)
	ctx := context.Background()
	for _, svc := range []string{"", "", "panic", "internal", "notfound", "down"} {
		_, _ = hc.Check(ctx, &healthpb.HealthCheckRequest{Service: svc})
	}
	if st, err := hc.Watch(ctx, &healthpb.HealthCheckRequest{}); err == nil {
		_, _ = st.Recv()
	}
	_ = pc.Close(ctx)

	const check = "/grpc.health.v1.Health/Check"
	byKey := map[string]pagerlesstest.Event{}
	for _, e := range ingest.Errors() {
		byKey[e.Route+" "+e.Error.Type] = e
	}
	p, ok := byKey["gRPC "+check+" panic: runtime.plainError"]
	if !ok {
		t.Fatalf("panic not reported: %v", keys(byKey))
	}
	if !strings.Contains(p.Error.Stack, "pagerless-go/grpc.chargeCard") || p.Status != 500 {
		t.Fatalf("panic event: %+v", p)
	}
	for _, k := range []string{
		"gRPC " + check + " grpc.Internal",           // server fault
		"gRPC client " + check + " grpc.Unavailable", // outbound failure
		"gRPC client " + check + " grpc.Internal",    // the panic and the Internal, as the caller saw them
	} {
		if _, ok := byKey[k]; !ok {
			t.Errorf("missing %q in %v", k, keys(byKey))
		}
	}
	for k := range byKey {
		if strings.Contains(k, "NotFound") || strings.HasPrefix(k, "gRPC "+check+" grpc.Unavailable") {
			t.Errorf("not a fault to capture: %s", k)
		}
	}
	if tgt := byKey["gRPC client "+check+" grpc.Unavailable"].Tags["grpc.target"]; tgt != "passthrough:///bufnet" {
		t.Errorf("target tag %q", tgt)
	}
	got := map[string]int{}
	for _, e := range ingest.Requests() {
		got[e.Route+" "+itoa(e.Status)] += e.Count
	}
	for k, n := range map[string]int{
		"gRPC " + check + " 200":                2,
		"gRPC " + check + " 500":                2, // the panic and the Internal
		"gRPC " + check + " 404":                1,
		"gRPC " + check + " 503":                1,
		"gRPC /grpc.health.v1.Health/Watch 500": 1,
	} {
		if got[k] != n {
			t.Errorf("%s: got %d want %d (all %v)", k, got[k], n, got)
		}
	}
}

func keys(m map[string]pagerlesstest.Event) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func itoa(n int) string { return strconv.Itoa(n) }
func TestHTTPStatus(t *testing.T) {
	for c, want := range map[codes.Code]int{codes.OK: 200, codes.Internal: 500, codes.Unavailable: 503, codes.NotFound: 404, codes.Canceled: 499} {
		if got := HTTPStatus(c); got != want {
			t.Errorf("%v → %d, want %d", c, got, want)
		}
	}
}
