package pagerlesstemporal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pagerless "github.com/lcbasu/pagerless-go"
	"github.com/lcbasu/pagerless-go/pagerlesstest"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func ChargeCards(ctx context.Context, shop string) error {
	if shop == "panic" {
		var m map[string]int
		m[shop]++ // nil map write: a real panic
	}
	if shop == "declined" {
		return temporal.NewNonRetryableApplicationError("card declined for shop", "CardDeclined", nil)
	}
	return nil
}

func NightlyExport(ctx workflow.Context, shop string) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	if err := workflow.Sleep(ctx, 90*time.Second); err != nil {
		return err
	}
	return workflow.ExecuteActivity(ctx, ChargeCards, shop).Get(ctx, nil)
}

type harness struct {
	ingest *pagerlesstest.Server
	pc     *pagerless.Client
	ic     *Interceptor
	env    *testsuite.TestWorkflowEnvironment
}

func newHarness(t *testing.T, scheduleID string, opts ...Option) *harness {
	t.Helper()
	ingest := pagerlesstest.NewServer(t)
	pc, err := pagerless.New("pl_test", pagerless.WithEndpoint(ingest.URL()))
	if err != nil {
		t.Fatal(err)
	}
	ic := New(nil, append([]Option{WithClient(pc)}, opts...)...)
	ic.describe = func(_ context.Context, id string) (*client.ScheduleSpec, error) {
		if id != "nightly-export" {
			return nil, errors.New("schedule not found")
		}
		return &client.ScheduleSpec{CronExpressions: []string{"30 3 * * *"}, TimeZoneName: "Europe/Berlin"}, nil
	}
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{ic}})
	env.RegisterWorkflow(NightlyExport)
	env.RegisterActivity(ChargeCards)
	if scheduleID != "" {
		if err := env.SetTypedSearchAttributesOnStart(temporal.NewSearchAttributes(ScheduledByID.ValueSet(scheduleID))); err != nil {
			t.Fatal(err)
		}
	}
	return &harness{ingest: ingest, pc: pc, ic: ic, env: env}
}

func (h *harness) run(t *testing.T, shop string) error {
	t.Helper()
	h.env.ExecuteWorkflow(NightlyExport, shop)
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	h.ic.wait()
	_ = h.pc.Close(context.Background())
	return h.env.GetWorkflowError()
}

func TestScheduledRunChecksInOK(t *testing.T) {
	h := newHarness(t, "nightly-export")
	if err := h.run(t, "ok"); err != nil {
		t.Fatal(err)
	}
	cis := h.ingest.CheckIns()
	if len(cis) != 1 {
		t.Fatalf("want one check-in, got %+v", cis)
	}
	ci := cis[0]
	if ci.Slug != "nightly-export" || ci.Status != "ok" || ci.ID == "" {
		t.Fatalf("check-in: %+v", ci)
	}
	if ci.DurationMs == nil || *ci.DurationMs < 90_000 {
		t.Fatalf("took should cover the run (workflow time): %v", ci.DurationMs)
	}
	if ci.Schedule == nil || ci.Schedule.Cron != "30 3 * * *" || ci.Schedule.TZ != "Europe/Berlin" {
		t.Fatalf("the schedule's timing goes with the check-in: %+v", ci.Schedule)
	}
	if errs := h.ingest.Errors(); len(errs) != 0 {
		t.Fatalf("no errors on a good run: %+v", errs)
	}
}

func TestScheduledRunThatFailsReportsBoth(t *testing.T) {
	h := newHarness(t, "Nightly Export (EU)")
	if err := h.run(t, "declined"); err == nil {
		t.Fatal("workflow should fail")
	}
	cis := h.ingest.CheckIns()
	if len(cis) != 1 || cis[0].Slug != "nightly-export-eu" || cis[0].Status != "failed" || !strings.Contains(cis[0].Message, "card declined") {
		t.Fatalf("failed check-in under the connect slug: %+v", cis)
	}
	if cis[0].Schedule != nil {
		t.Fatalf("an unknown schedule is sent without timing: %+v", cis[0].Schedule)
	}
	byRoute := map[string]pagerlesstest.Event{}
	for _, e := range h.ingest.Errors() {
		byRoute[e.Route] = e
	}
	a, ok := byRoute["activity ChargeCards"]
	if !ok || a.Error.Type != "CardDeclined" || a.Tags["workflow"] != "NightlyExport" || a.Tags["attempt"] != "1" {
		t.Fatalf("activity failure: %+v", byRoute)
	}
	if w, ok := byRoute["workflow NightlyExport"]; !ok || w.Error.Type != "CardDeclined" {
		t.Fatalf("workflow failure: %+v", byRoute)
	}
}

func TestActivityPanicIsReported(t *testing.T) {
	h := newHarness(t, "")
	if err := h.run(t, "panic"); err == nil {
		t.Fatal("workflow should fail")
	}
	var p *pagerlesstest.Event
	for _, e := range h.ingest.Errors() {
		if e.Route == "activity ChargeCards" && strings.HasPrefix(e.Error.Type, "panic: ") {
			p = &e
		}
	}
	if p == nil || !strings.Contains(p.Error.Stack, "pagerless-go/temporal.ChargeCards") {
		t.Fatalf("activity panic with its stack: %+v", h.ingest.Errors())
	}
	if len(h.ingest.CheckIns()) != 0 {
		t.Fatal("a run no schedule started has no check-in")
	}
}

func TestSkippedSchedulesDontCheckIn(t *testing.T) {
	h := newHarness(t, "agent-run-42", SkipSchedules("agent-run-"))
	_ = h.run(t, "ok")
	if n := len(h.ingest.CheckIns()); n != 0 {
		t.Fatalf("got %d check-ins", n)
	}
}

func TestScheduleOf(t *testing.T) {
	r := func(a, b, step int) client.ScheduleRange { return client.ScheduleRange{Start: a, End: b, Step: step} }
	cases := []struct {
		name string
		spec client.ScheduleSpec
		want pagerless.Schedule
		ok   bool
	}{
		{"interval", client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: 30 * time.Minute, Offset: time.Minute}}}, pagerless.Schedule{Every: 30 * time.Minute}, true},
		{"cron", client.ScheduleSpec{CronExpressions: []string{"0 */6 * * *"}}, pagerless.Schedule{Cron: "0 */6 * * *"}, true},
		{"every shorthand", client.ScheduleSpec{CronExpressions: []string{"@every 5m"}}, pagerless.Schedule{}, false},
		{"calendar as the server keeps cron", client.ScheduleSpec{Calendars: []client.ScheduleCalendarSpec{{
			Second: []client.ScheduleRange{{}}, Minute: []client.ScheduleRange{r(0, 59, 15)}, Hour: []client.ScheduleRange{r(0, 23, 1)},
			DayOfMonth: []client.ScheduleRange{r(1, 31, 1)}, Month: []client.ScheduleRange{r(1, 12, 1)},
			DayOfWeek: []client.ScheduleRange{r(1, 3, 1), r(5, 0, 0)},
		}}, TimeZoneName: "UTC"}, pagerless.Schedule{Cron: "*/15 * * * 1-3,5", TZ: "UTC"}, true},
		{"calendar defaults: midnight daily", client.ScheduleSpec{Calendars: []client.ScheduleCalendarSpec{{}}}, pagerless.Schedule{Cron: "0 0 * * *"}, true},
		{"seconds can't be cron", client.ScheduleSpec{Calendars: []client.ScheduleCalendarSpec{{Second: []client.ScheduleRange{r(30, 30, 1)}}}}, pagerless.Schedule{}, false},
		{"two specs", client.ScheduleSpec{CronExpressions: []string{"0 1 * * *", "0 13 * * *"}}, pagerless.Schedule{}, false},
	}
	for _, c := range cases {
		got, ok := ScheduleOf(c.spec)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("%s: got %+v %v, want %+v %v", c.name, got, ok, c.want, c.ok)
		}
	}
}

// created stands in for the Temporal service behind the client: it accepts any schedule.
type created struct {
	interceptor.ClientOutboundInterceptorBase
}

func (*created) CreateSchedule(context.Context, *interceptor.ScheduleClientCreateInput) (client.ScheduleHandle, error) {
	return nil, nil
}

func TestAsAClientInterceptorItLearnsEachSchedulesTiming(t *testing.T) {
	h := newHarness(t, "Hourly-Rollup")
	h.ic.describe = nil // installed on the client: no Describe, only what CreateSchedule saw
	out := h.ic.InterceptClient(&created{})
	_, _ = out.CreateSchedule(context.Background(), &interceptor.ScheduleClientCreateInput{Options: &client.ScheduleOptions{
		ID:   "Hourly-Rollup",
		Spec: client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: time.Hour}}},
	}})
	if err := h.run(t, "ok"); err != nil {
		t.Fatal(err)
	}
	cis := h.ingest.CheckIns()
	if len(cis) != 1 || cis[0].Slug != "hourly-rollup" || cis[0].Schedule == nil || cis[0].Schedule.ExpectEverySeconds != 3600 {
		t.Fatalf("check-in with the learned timing: %+v", cis)
	}
	var _ interceptor.Interceptor = h.ic // both a client and a worker interceptor
}
