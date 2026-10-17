package prometheus

import (
	"context"
	"strings"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// poHistogram returns the sample count and the cumulative bucket counts by
// upper bound of the histogram family name, which must have exactly one series.
// It avoids importing client_model so the module keeps it indirect.
func poHistogram(t *testing.T, reg *prometheus.Registry, name string) (uint64, map[float64]uint64) {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		if len(f.GetMetric()) != 1 {
			t.Fatalf("%s has %d series, want 1", name, len(f.GetMetric()))
		}
		h := f.GetMetric()[0].GetHistogram()
		buckets := make(map[float64]uint64)
		for _, b := range h.GetBucket() {
			buckets[b.GetUpperBound()] = b.GetCumulativeCount()
		}
		return h.GetSampleCount(), buckets
	}
	t.Fatalf("family %s not found", name)
	return 0, nil
}

func TestObserver_ScrapeHandlerAndTransition(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	o := NewObserver(reg)

	o.HandlerDone(ctx, event.HandlerResult{
		Handler:  "billing",
		Event:    "order.created",
		Outcome:  event.OutcomeDeadLettered,
		Duration: 30 * time.Millisecond,
	})
	o.BreakerTransition(ctx, event.BreakerTransition{
		Handler: "billing",
		From:    event.BreakerClosed,
		To:      event.BreakerOpen,
		Reason:  event.BreakerReasonFailureThreshold,
		Seq:     1,
	})

	const want = `
# HELP eventbus_breaker_state Circuit breaker state: 0 closed, 1 open, 2 half_open. Absent means it never transitioned.
# TYPE eventbus_breaker_state gauge
eventbus_breaker_state{handler="billing"} 1
# HELP eventbus_breaker_transitions_total Circuit breaker state transitions.
# TYPE eventbus_breaker_transitions_total counter
eventbus_breaker_transitions_total{from="closed",handler="billing",reason="failure_threshold",to="open"} 1
# HELP eventbus_handler_total Handler chain invocations by outcome.
# TYPE eventbus_handler_total counter
eventbus_handler_total{event="order.created",handler="billing",outcome="dead_lettered"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want),
		"eventbus_breaker_state", "eventbus_breaker_transitions_total", "eventbus_handler_total"); err != nil {
		t.Fatal(err)
	}

	count, buckets := poHistogram(t, reg, "eventbus_handler_duration_seconds")
	if count != 1 {
		t.Fatalf("duration count = %d, want 1", count)
	}
	for _, ub := range DefaultBuckets {
		var want uint64
		if ub >= 0.05 {
			want = 1
		}
		if buckets[ub] != want {
			t.Errorf("bucket le=%v count = %d, want %d", ub, buckets[ub], want)
		}
	}
}
