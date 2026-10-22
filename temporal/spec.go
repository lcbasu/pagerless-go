package pagerlesstemporal

import (
	"strconv"
	"strings"

	pagerless "github.com/lcbasu/pagerless-go"
	"go.temporal.io/sdk/client"
)

// ScheduleOf turns a Temporal schedule spec into a check-in schedule: one interval → Every, one
// cron expression or calendar → a 5-field cron. false when it can't be said exactly (several
// specs, seconds, years); the check-in is then created without timing, set it in the app.
func ScheduleOf(spec client.ScheduleSpec) (pagerless.Schedule, bool) {
	s := pagerless.Schedule{TZ: spec.TimeZoneName}
	switch {
	case len(spec.Intervals) == 1 && len(spec.Calendars) == 0 && len(spec.CronExpressions) == 0:
		s.Every = spec.Intervals[0].Every
		return s, s.Every > 0
	case len(spec.CronExpressions) == 1 && len(spec.Calendars) == 0 && len(spec.Intervals) == 0:
		expr := strings.TrimSpace(spec.CronExpressions[0])
		if len(strings.Fields(expr)) != 5 && !strings.HasPrefix(expr, "@") || strings.HasPrefix(expr, "@every") {
			return s, false
		}
		s.Cron = expr
		return s, true
	case len(spec.Calendars) == 1 && len(spec.Intervals) == 0 && len(spec.CronExpressions) == 0:
		cron, ok := calendarCron(spec.Calendars[0])
		s.Cron = cron
		return s, ok
	}
	return s, false
}

// calendarCron writes a calendar spec (what Temporal keeps a cron expression as) back as cron.
func calendarCron(c client.ScheduleCalendarSpec) (string, bool) {
	if len(c.Year) > 0 {
		return "", false
	}
	for _, r := range c.Second {
		if r.Start != 0 || max(r.End, r.Start) != 0 {
			return "", false // cron has no seconds
		}
	}
	fields := []string{
		cronField(c.Minute, 0, 59, "0"),
		cronField(c.Hour, 0, 23, "0"),
		cronField(c.DayOfMonth, 1, 31, "*"),
		cronField(c.Month, 1, 12, "*"),
		cronField(c.DayOfWeek, 0, 6, "*"),
	}
	return strings.Join(fields, " "), true
}

func cronField(rs []client.ScheduleRange, lo, hi int, empty string) string {
	if len(rs) == 0 {
		return empty
	}
	parts := make([]string, 0, len(rs))
	for _, r := range rs {
		end := max(r.End, r.Start) // End defaults to Start
		var p string
		switch {
		case r.Start <= lo && end >= hi:
			p = "*"
		case end == r.Start:
			p = strconv.Itoa(r.Start)
		default:
			p = strconv.Itoa(r.Start) + "-" + strconv.Itoa(end)
		}
		if r.Step > 1 && end != r.Start {
			p += "/" + strconv.Itoa(r.Step)
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, ",")
}
