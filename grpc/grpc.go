// Package pagerlessgrpc reports gRPC server failures, panics and per-method health, and failed
// outbound calls, to Pagerless.
//
//	srv := grpc.NewServer(pagerlessgrpc.ServerOptions()...)
//	conn, err := grpc.NewClient(target, append(opts, pagerlessgrpc.DialOptions()...)...)
//
// Each method is a route ("gRPC /shop.Orders/Get"). Server status codes are recorded as their
// HTTP equivalents (Internal → 500, Unavailable → 503…), so 5xx rates work as for HTTP. A panic
// is reported and re-panicked, so a recovery interceptor outside this one still handles it.
package pagerlessgrpc

import (
	"context"
	"net/http"
	"time"

	pagerless "github.com/lcbasu/pagerless-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Option configures the interceptors.
type Option func(*config)

type config struct{ client func() *pagerless.Client }

// WithClient reports to c instead of the default client.
func WithClient(c *pagerless.Client) Option {
	return func(cfg *config) { cfg.client = func() *pagerless.Client { return c } }
}

func newConfig(opts []Option) config {
	cfg := config{client: pagerless.Default}
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	return cfg
}

// ServerOptions chains the unary and stream server interceptors.
func ServerOptions(opts ...Option) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(UnaryServerInterceptor(opts...)),
		grpc.ChainStreamInterceptor(StreamServerInterceptor(opts...)),
	}
}

// DialOptions chains the unary and stream client interceptors.
func DialOptions(opts ...Option) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(UnaryClientInterceptor(opts...)),
		grpc.WithChainStreamInterceptor(StreamClientInterceptor(opts...)),
	}
}

// UnaryServerInterceptor records each call and reports panics and server-fault errors
// (Unknown, Internal, DataLoss): the codes that mean a bug, not a bad request.
func UnaryServerInterceptor(opts ...Option) grpc.UnaryServerInterceptor {
	cfg := newConfig(opts)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		pc := cfg.client()
		route := "gRPC " + info.FullMethod
		start := time.Now()
		defer repanic(pc, route, start)
		resp, err := handler(ctx, req)
		observe(pc, route, err, start)
		return resp, err
	}
}

// StreamServerInterceptor is UnaryServerInterceptor for streaming methods (one record per stream).
func StreamServerInterceptor(opts ...Option) grpc.StreamServerInterceptor {
	cfg := newConfig(opts)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		pc := cfg.client()
		route := "gRPC " + info.FullMethod
		start := time.Now()
		defer repanic(pc, route, start)
		err := handler(srv, ss)
		observe(pc, route, err, start)
		return err
	}
}

func repanic(pc *pagerless.Client, route string, start time.Time) {
	v := recover()
	if v == nil {
		return
	}
	pc.CapturePanic(v, pagerless.Route(route), pagerless.Status(http.StatusInternalServerError))
	pc.RecordRequest(route, http.StatusInternalServerError, time.Since(start))
	panic(v)
}

func observe(pc *pagerless.Client, route string, err error, start time.Time) {
	code := status.Code(err)
	httpStatus := HTTPStatus(code)
	if serverFault(code) {
		pc.Capture(err, captureOpts(err, route, httpStatus)...)
	}
	pc.RecordRequest(route, httpStatus, time.Since(start))
}

// UnaryClientInterceptor reports outbound calls that failed for reasons on the far side or the
// network (Unavailable, DeadlineExceeded, Unknown, Internal, DataLoss, Unimplemented), not
// answers like NotFound or InvalidArgument, nor calls this process cancelled.
func UnaryClientInterceptor(opts ...Option) grpc.UnaryClientInterceptor {
	cfg := newConfig(opts)
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, callOpts...)
		reportOutbound(cfg.client(), method, cc, err)
		return err
	}
}

// StreamClientInterceptor reports streams that failed to open (see UnaryClientInterceptor).
func StreamClientInterceptor(opts ...Option) grpc.StreamClientInterceptor {
	cfg := newConfig(opts)
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, callOpts ...grpc.CallOption) (grpc.ClientStream, error) {
		s, err := streamer(ctx, desc, cc, method, callOpts...)
		reportOutbound(cfg.client(), method, cc, err)
		return s, err
	}
}

func reportOutbound(pc *pagerless.Client, method string, cc *grpc.ClientConn, err error) {
	if err == nil || !outboundFailure(status.Code(err)) {
		return
	}
	opts := captureOpts(err, "gRPC client "+method, 0)
	if cc != nil {
		opts = append(opts, pagerless.Tags(map[string]string{"grpc.target": cc.Target()}))
	}
	pc.Capture(err, opts...)
}

// captureOpts names a status error by its code ("grpc.Internal"): the Go type of a status error
// says nothing, the code does. A handler's own error type is kept.
func captureOpts(err error, route string, httpStatus int) []pagerless.CaptureOption {
	opts := []pagerless.CaptureOption{pagerless.Route(route)}
	if httpStatus != 0 {
		opts = append(opts, pagerless.Status(httpStatus))
	}
	if _, ok := err.(interface{ GRPCStatus() *status.Status }); ok {
		opts = append(opts, pagerless.ErrorType("grpc."+status.Code(err).String()))
	}
	return opts
}

func serverFault(c codes.Code) bool {
	return c == codes.Unknown || c == codes.Internal || c == codes.DataLoss
}

func outboundFailure(c codes.Code) bool {
	switch c {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Unknown, codes.Internal, codes.DataLoss, codes.Unimplemented:
		return true
	}
	return false
}

// HTTPStatus maps a gRPC code to its HTTP equivalent (the grpc-gateway mapping).
func HTTPStatus(c codes.Code) int {
	switch c {
	case codes.OK:
		return http.StatusOK
	case codes.Canceled:
		return 499 // client closed the request
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return http.StatusBadRequest
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists, codes.Aborted:
		return http.StatusConflict
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default: // Unknown, Internal, DataLoss
		return http.StatusInternalServerError
	}
}
