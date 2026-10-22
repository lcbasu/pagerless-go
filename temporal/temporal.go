// Package pagerlesstemporal reports Temporal activity and workflow failures to Pagerless and
// turns every Temporal Schedule into a check-in: each scheduled run pings
// /v1/checkins/<schedule-id> when it ends (ok, or failed with the reason), so a schedule that
// stops firing or keeps failing pages you once.
//
// Install it on the worker, passing the client so each schedule's timing can be read:
//
//	w := worker.New(c, "orders", worker.Options{
//		Interceptors: []interceptor.WorkerInterceptor{pagerlesstemporal.New(c)},
//	})
//
// or on the client, which covers every worker made from it and learns the timing of each
// schedule this process creates (CreateSchedule):
//
//	c, err := client.Dial(client.Options{
//		Interceptors: []interceptor.ClientInterceptor{pagerlesstemporal.New(nil)},
//	})
//
// Not both: Temporal would run it twice.
package pagerlesstemporal

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	pagerless "github.com/lcbasu/pagerless-go"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ScheduledByID is the search attribute Temporal sets on a workflow a Schedule started.
var ScheduledByID = temporal.NewSearchAttributeKeyKeyword("TemporalScheduledById")

// Option configures New.
type Option func(*Interceptor)

// WithClient reports to c instead of the default client.
func WithClient(c *pagerless.Client) Option {
	return func(i *Interceptor) { i.client = func() *pagerless.Client { return c } }
}

// SkipSchedules leaves schedules whose ID starts with any of prefixes without a check-in: ones
// the app creates per customer ("agent-run-<id>"), which come and go and shouldn't be watched.
func SkipSchedules(prefixes ...string) Option {
	return func(i *Interceptor) { i.skip = append(i.skip, prefixes...) }
}

// NoCheckIns turns schedule check-ins off; failures are still reported.
func NoCheckIns() Option { return func(i *Interceptor) { i.checkIns = false } }

// Interceptor is the worker (and client) interceptor New returns.
type Interceptor struct {
	interceptor.InterceptorBase
	client   func() *pagerless.Client
	describe func(ctx context.Context, id string) (*client.ScheduleSpec, error)
	skip     []string
	checkIns bool

	mu        sync.Mutex
	schedules map[string]cachedSchedule
	pending   sync.WaitGroup // check-ins in flight (tests wait on it)
}

type cachedSchedule struct {
	s       pagerless.Schedule
	ok      bool
	expires time.Time // zero: learned from CreateSchedule, kept
}

// How long a schedule's timing is trusted before it is read again (a schedule can be edited).
const (
	scheduleTTL     = time.Hour
	scheduleMissTTL = 10 * time.Minute
	checkInTimeout  = 30 * time.Second
)

// New returns the interceptor. c reads each schedule's timing (Describe); nil is fine, and as a
// client interceptor it learns the timing of schedules created through that client instead.
func New(c client.Client, opts ...Option) *Interceptor {
	i := &Interceptor{client: pagerless.Default, checkIns: true, schedules: map[string]cachedSchedule{}}
	if c != nil {
		i.describe = func(ctx context.Context, id string) (*client.ScheduleSpec, error) {
			d, err := c.ScheduleClient().GetHandle(ctx, id).Describe(ctx)
			if err != nil {
				return nil, err
			}
			return d.Schedule.Spec, nil
		}
	}
	for _, o := range opts {
		if o != nil {
			o(i)
		}
	}
	return i
}

// InterceptClient implements interceptor.ClientInterceptor: it notes the spec of every schedule
// created through the client, so check-ins carry its timing without a Describe call.
func (i *Interceptor) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	return &clientOutbound{ClientOutboundInterceptorBase: interceptor.ClientOutboundInterceptorBase{Next: next}, root: i}
}

type clientOutbound struct {
	interceptor.ClientOutboundInterceptorBase
	root *Interceptor
}

func (o *clientOutbound) CreateSchedule(ctx context.Context, in *interceptor.ScheduleClientCreateInput) (client.ScheduleHandle, error) {
	if in != nil && in.Options != nil && in.Options.ID != "" {
		// Learned even when the schedule already exists: the code's spec is what runs.
		s, ok := ScheduleOf(in.Options.Spec)
		o.root.mu.Lock()
		o.root.schedules[in.Options.ID] = cachedSchedule{s: s, ok: ok}
		o.root.mu.Unlock()
	}
	return o.Next.CreateSchedule(ctx, in)
}

// InterceptActivity implements interceptor.WorkerInterceptor.
func (i *Interceptor) InterceptActivity(ctx context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &activityInbound{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}, root: i}
}

// InterceptWorkflow implements interceptor.WorkerInterceptor.
func (i *Interceptor) InterceptWorkflow(ctx workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &workflowInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, root: i}
}

type activityInbound struct {
	interceptor.ActivityInboundInterceptorBase
	root *Interceptor
}

func (a *activityInbound) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (any, error) {
	info := activity.GetInfo(ctx)
	pc := a.root.client()
	opts := []pagerless.CaptureOption{
		pagerless.Route("activity " + info.ActivityType.Name),
		pagerless.Tags(map[string]string{
			"workflow":   info.WorkflowType.Name,
			"task_queue": info.TaskQueue,
			"attempt":    strconv.Itoa(int(info.Attempt)),
		}),
	}
	defer func() {
		if v := recover(); v != nil {
			pc.CapturePanic(v, opts...)
			panic(v) // Temporal turns it into the activity's failure, as before
		}
	}()
	res, err := a.Next.ExecuteActivity(ctx, in)
	if err != nil && !benign(err) {
		pc.Capture(err, append(opts, errorType(err))...)
	}
	return res, err
}

type workflowInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	root *Interceptor
}

func (w *workflowInbound) ExecuteWorkflow(ctx workflow.Context, in *interceptor.ExecuteWorkflowInput) (any, error) {
	info := workflow.GetInfo(ctx)
	pc := w.root.client()
	opts := []pagerless.CaptureOption{
		pagerless.Route("workflow " + info.WorkflowType.Name),
		pagerless.Tags(map[string]string{"task_queue": info.TaskQueueName}),
	}
	defer func() {
		if v := recover(); v != nil {
			// A workflow panic blocks the run (its task retries until the code is fixed): report
			// it each time it happens for real, never while replaying history.
			if !workflow.IsReplaying(ctx) {
				pc.CapturePanic(v, opts...)
			}
			panic(v)
		}
	}()
	res, err := w.Next.ExecuteWorkflow(ctx, in)
	if workflow.IsReplaying(ctx) || continuedAsNew(err) {
		return res, err
	}
	if err != nil && !benign(err) {
		pc.Capture(err, append(opts, errorType(err))...)
	}
	if id, ok := workflow.GetTypedSearchAttributes(ctx).GetKeyword(ScheduledByID); ok && id != "" && w.root.watches(id) && !canceled(err) {
		took := workflow.Now(ctx).Sub(info.WorkflowStartTime)
		w.root.pending.Add(1)
		// Off the workflow's goroutine: the check-in is network I/O, and workflow code must not
		// block. It doesn't touch the workflow's history.
		go w.root.checkIn(pc, id, info.WorkflowExecution.RunID, took, err)
	}
	return res, err
}

func (i *Interceptor) watches(scheduleID string) bool {
	if !i.checkIns {
		return false
	}
	for _, p := range i.skip {
		if strings.HasPrefix(scheduleID, p) {
			return false
		}
	}
	return true
}

func (i *Interceptor) checkIn(pc *pagerless.Client, scheduleID, runID string, took time.Duration, runErr error) {
	defer i.pending.Done()
	ctx, cancel := context.WithTimeout(context.Background(), checkInTimeout)
	defer cancel()
	opts := []pagerless.CheckInOption{pagerless.Took(took), pagerless.RunID(runID)}
	if s, ok := i.schedule(ctx, scheduleID); ok {
		opts = append(opts, pagerless.WithSchedule(s))
	}
	if runErr != nil {
		opts = append(opts, pagerless.Failed(runErr.Error()))
	}
	_ = pc.CheckIn(ctx, pagerless.CheckInSlug(scheduleID), opts...) // logged by the client; never fails the run
}

// schedule is the schedule's timing as a check-in schedule, read once an hour at most.
func (i *Interceptor) schedule(ctx context.Context, id string) (pagerless.Schedule, bool) {
	now := time.Now()
	i.mu.Lock()
	c, hit := i.schedules[id]
	i.mu.Unlock()
	if hit && (c.expires.IsZero() || now.Before(c.expires)) {
		return c.s, c.ok
	}
	if i.describe == nil {
		return pagerless.Schedule{}, false
	}
	spec, err := i.describe(ctx, id)
	c = cachedSchedule{expires: now.Add(scheduleMissTTL)}
	if err == nil && spec != nil {
		if c.s, c.ok = ScheduleOf(*spec); c.ok {
			c.expires = now.Add(scheduleTTL)
		}
	}
	i.mu.Lock()
	i.schedules[id] = c
	i.mu.Unlock()
	return c.s, c.ok
}

// wait blocks until every check-in started so far has been sent (tests).
func (i *Interceptor) wait() { i.pending.Wait() }

func benign(err error) bool {
	return canceled(err) || errors.Is(err, activity.ErrResultPending)
}

func canceled(err error) bool {
	return err != nil && (errors.Is(err, context.Canceled) || temporal.IsCanceledError(err))
}

func continuedAsNew(err error) bool {
	var can *workflow.ContinueAsNewError
	return errors.As(err, &can)
}

// errorType names an application error by its Type ("CardDeclined"), which says more than the
// Go type of a Temporal error wrapper.
func errorType(err error) pagerless.CaptureOption {
	var ae *temporal.ApplicationError
	if errors.As(err, &ae) && ae.Type() != "" {
		return pagerless.ErrorType(ae.Type())
	}
	var te *temporal.TimeoutError
	if errors.As(err, &te) {
		return pagerless.ErrorType("TimeoutError." + te.TimeoutType().String())
	}
	return nil
}
