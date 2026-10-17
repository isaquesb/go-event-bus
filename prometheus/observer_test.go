package prometheus

import (
	"context"
	"errors"
	"slices"
	"sort"
	"strings"
	"sync"
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

// poExerciseAll calls every Observer method once with the fixed values below.
func poExerciseAll(o *Observer) {
	ctx := context.Background()
	o.RetryAttempt(ctx, event.RetryAttemptInfo{Handler: "h", Event: "e", Attempt: 1, Class: event.AttemptTransient, Err: errors.New("boom id=42"), Delay: time.Second})
	o.BreakerTransition(ctx, event.BreakerTransition{Handler: "h", From: event.BreakerClosed, To: event.BreakerOpen, Reason: event.BreakerReasonFailureThreshold, Seq: 1})
	o.Failure(ctx, event.FailureContext{Handler: "h", Outcome: event.OutcomeDeadLettered, Reason: "exhausted", Cause: errors.New("boom id=42")})
	o.DeadLettered(ctx, event.DeadLetterInfo{Handler: "h", Event: "e", Reason: "exhausted", Subject: "orders.42"})
	o.HandlerDone(ctx, event.HandlerResult{Handler: "h", Event: "e", Outcome: event.OutcomeOK, Duration: time.Millisecond, Err: errors.New("boom id=42")})
	o.DecodeFailed(ctx, event.DecodeFailure{Handler: "h", Subject: "orders.42", Reason: "invalid_json", Err: errors.New("boom id=42")})
	o.DLQPublishFailed(ctx, event.DLQPublishFailure{Handler: "h", Event: "e", Reason: "exhausted", Subject: "orders.42", Err: errors.New("boom id=42")})
	o.StoreError(ctx, event.StoreErrorInfo{Handler: "h", Event: "e", Store: "idempotency", Op: "begin", Err: errors.New("boom id=42")})
	o.IdempotencyStale(ctx, event.IdempotencyStaleInfo{Handler: "h", Event: "e", Age: time.Minute})
	o.InternalError(ctx, event.InternalErrorInfo{Component: event.ComponentTransport, Op: event.OpAck, Handler: "h", Subject: "orders.42", Err: errors.New("boom id=42")})
}

// poFamilies gathers reg and returns, per family name, the sorted label names of
// its series (every series of a family has the same label names).
func poFamilies(t *testing.T, reg *prometheus.Registry) map[string][]string {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := make(map[string][]string, len(fams))
	for _, f := range fams {
		if len(f.GetMetric()) == 0 {
			t.Fatalf("family %s has no series", f.GetName())
		}
		var first []string
		for i, m := range f.GetMetric() {
			var names []string
			for _, l := range m.GetLabel() {
				names = append(names, l.GetName())
			}
			sort.Strings(names)
			if i == 0 {
				first = names
			} else if !slices.Equal(first, names) {
				t.Errorf("family %s series disagree on label names: %v vs %v", f.GetName(), first, names)
			}
		}
		out[f.GetName()] = first
	}
	return out
}

var poWantLabels = map[string][]string{
	"eventbus_handler_duration_seconds":  {"event", "handler", "outcome"},
	"eventbus_handler_total":             {"event", "handler", "outcome"},
	"eventbus_retry_attempts_total":      {"event", "handler", "reason"},
	"eventbus_breaker_transitions_total": {"from", "handler", "reason", "to"},
	"eventbus_breaker_state":             {"handler"},
	"eventbus_dlq_records_total":         {"event", "handler", "reason"},
	"eventbus_dlq_publish_errors_total":  {"event", "handler"},
	"eventbus_decode_failures_total":     {"reason"},
	"eventbus_store_errors_total":        {"handler", "op", "store"},
	"eventbus_idempotency_stale_total":   {"handler"},
	"eventbus_internal_errors_total":     {"component", "op"},
}

func TestObserver_EveryMethodMapped(t *testing.T) {
	reg := prometheus.NewRegistry()
	o := NewObserver(reg)
	poExerciseAll(o)

	got := poFamilies(t, reg)
	if len(got) != len(poWantLabels) {
		t.Fatalf("got %d families, want %d: %v", len(got), len(poWantLabels), got)
	}
	for name := range poWantLabels {
		if _, ok := got[name]; !ok {
			t.Errorf("family %s missing", name)
		}
	}
}

func TestObserver_FailureAddsNoSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	o := NewObserver(reg)
	o.Failure(context.Background(), event.FailureContext{Handler: "h", Outcome: event.OutcomeDeadLettered, Reason: "exhausted"})

	if n := testutil.CollectAndCount(reg); n != 0 {
		t.Fatalf("Failure created %d series, want 0", n)
	}
}

func TestObserver_LabelSets(t *testing.T) {
	reg := prometheus.NewRegistry()
	o := NewObserver(reg)
	poExerciseAll(o)

	got := poFamilies(t, reg)
	for name, want := range poWantLabels {
		if !slices.Equal(got[name], want) {
			t.Errorf("%s labels = %v, want %v", name, got[name], want)
		}
	}

	// No unbounded value ever becomes a label value: the error text and the
	// subject carry "42".
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if strings.Contains(l.GetValue(), "42") || strings.Contains(l.GetValue(), "boom") {
					t.Errorf("%s label %s=%q carries unbounded text", f.GetName(), l.GetName(), l.GetValue())
				}
			}
		}
	}
}

func TestObserver_SignalValues(t *testing.T) {
	reg := prometheus.NewRegistry()
	o := NewObserver(reg)
	poExerciseAll(o)

	checks := []struct {
		name string
		got  float64
	}{
		{"retry reason is the attempt class", testutil.ToFloat64(o.retryAttempts.WithLabelValues("h", "e", "transient"))},
		{"dlq record", testutil.ToFloat64(o.dlqRecords.WithLabelValues("h", "e", "exhausted"))},
		{"dlq publish error", testutil.ToFloat64(o.dlqPublishErrors.WithLabelValues("h", "e"))},
		{"decode failure uses only reason", testutil.ToFloat64(o.decodeFailures.WithLabelValues("invalid_json"))},
		{"store error", testutil.ToFloat64(o.storeErrors.WithLabelValues("h", "idempotency", "begin"))},
		{"idempotency stale", testutil.ToFloat64(o.idempotencyStale.WithLabelValues("h"))},
		{"internal error uses component and op", testutil.ToFloat64(o.internalErrors.WithLabelValues("transport", "ack"))},
	}
	for _, c := range checks {
		if c.got != 1 {
			t.Errorf("%s = %v, want 1", c.name, c.got)
		}
	}
}

func TestObserver_BreakerGaugeSeq(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	o := NewObserver(reg)
	tr := func(from, to event.BreakerState, seq uint64) event.BreakerTransition {
		return event.BreakerTransition{Handler: "h", From: from, To: to, Reason: event.BreakerReasonFailureThreshold, Seq: seq}
	}
	state := func() float64 { return testutil.ToFloat64(o.breakerState.WithLabelValues("h")) }

	o.BreakerTransition(ctx, tr(event.BreakerClosed, event.BreakerOpen, 2))
	o.BreakerTransition(ctx, tr(event.BreakerOpen, event.BreakerClosed, 1)) // late, older Seq
	if got := state(); got != 1 {
		t.Fatalf("state after Seq 2 then Seq 1 = %v, want 1 (open)", got)
	}
	o.BreakerTransition(ctx, tr(event.BreakerOpen, event.BreakerHalfOpen, 3))
	if got := state(); got != 2 {
		t.Fatalf("state after Seq 3 = %v, want 2 (half_open)", got)
	}
	o.BreakerTransition(ctx, tr(event.BreakerHalfOpen, event.BreakerClosed, 4))
	if got := state(); got != 0 {
		t.Fatalf("state after Seq 4 = %v, want 0 (closed)", got)
	}

	var total float64
	for _, to := range []event.BreakerState{event.BreakerOpen, event.BreakerClosed, event.BreakerHalfOpen} {
		for _, from := range []event.BreakerState{event.BreakerClosed, event.BreakerOpen, event.BreakerHalfOpen} {
			total += testutil.ToFloat64(o.breakerTransitions.WithLabelValues("h", string(from), string(to), string(event.BreakerReasonFailureThreshold)))
		}
	}
	if total != 4 {
		t.Fatalf("transitions counted = %v, want 4 (all of them, including the stale one)", total)
	}
}

func TestObserver_BreakerGaugePerHandler(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	o := NewObserver(reg)

	o.BreakerTransition(ctx, event.BreakerTransition{Handler: "a", From: event.BreakerClosed, To: event.BreakerOpen, Reason: event.BreakerReasonFailureThreshold, Seq: 5})
	o.BreakerTransition(ctx, event.BreakerTransition{Handler: "b", From: event.BreakerClosed, To: event.BreakerOpen, Reason: event.BreakerReasonFailureThreshold, Seq: 1})

	if got := testutil.ToFloat64(o.breakerState.WithLabelValues("b")); got != 1 {
		t.Fatalf("handler b state = %v, want 1: Seq is tracked per handler", got)
	}
}

// TestObserver_BreakerGaugeNewBreakerSameHandler is the tracer end of WR-03: a
// rebuilt breaker that reuses a handler name starts again at Seq 1 and must
// still move the gauge, while a late transition of the old breaker stays
// ignored.
func TestObserver_BreakerGaugeNewBreakerSameHandler(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	o := NewObserver(reg)
	tr := func(id uint64, to event.BreakerState, seq uint64) event.BreakerTransition {
		return event.BreakerTransition{Handler: "h", To: to, Reason: event.BreakerReasonFailureThreshold, Seq: seq, BreakerID: id}
	}
	state := func() float64 { return testutil.ToFloat64(o.breakerState.WithLabelValues("h")) }

	o.BreakerTransition(ctx, tr(1, event.BreakerOpen, 1))
	o.BreakerTransition(ctx, tr(1, event.BreakerHalfOpen, 2))
	o.BreakerTransition(ctx, tr(1, event.BreakerOpen, 3))
	if got := state(); got != 1 {
		t.Fatalf("state after breaker 1 ends open = %v, want 1", got)
	}

	o.BreakerTransition(ctx, tr(2, event.BreakerClosed, 1))
	if got := state(); got != 0 {
		t.Fatalf("state after breaker 2 Seq 1 closed = %v, want 0: the gauge froze on the old breaker", got)
	}

	o.BreakerTransition(ctx, tr(1, event.BreakerHalfOpen, 2)) // late, older Seq of breaker 1
	if got := state(); got != 0 {
		t.Fatalf("state after a late breaker 1 Seq 2 = %v, want 0 (ignored)", got)
	}
}

func TestObserver_Namespace(t *testing.T) {
	reg := prometheus.NewRegistry()
	o := NewObserver(reg, WithNamespace("shop"))
	poExerciseAll(o)

	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(fams) != len(poWantLabels) {
		t.Fatalf("got %d families, want %d", len(fams), len(poWantLabels))
	}
	for _, f := range fams {
		name, ok := strings.CutPrefix(f.GetName(), "shop_")
		if !ok {
			t.Errorf("family %s lacks the shop_ prefix", f.GetName())
			continue
		}
		if _, ok := poWantLabels[name]; !ok {
			t.Errorf("unexpected family %s", f.GetName())
		}
	}
	if testutil.ToFloat64(o.handlerTotal.WithLabelValues("h", "e", "ok")) != 1 {
		t.Fatal("shop_eventbus_handler_total not counted")
	}
}

func TestObserver_Promlint(t *testing.T) {
	reg := prometheus.NewRegistry()
	o := NewObserver(reg)
	poExerciseAll(o)

	problems, err := testutil.GatherAndLint(reg)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Errorf("promlint: %s: %s", p.Metric, p.Text)
	}
}

func TestObserver_Concurrent(t *testing.T) {
	const goroutines, iterations = 64, 500
	reg := prometheus.NewRegistry()
	o := NewObserver(reg)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				poExerciseAll(o)
			}
		}()
	}
	wg.Wait()

	const want = goroutines * iterations
	counters := map[string]float64{
		"handler_total":       testutil.ToFloat64(o.handlerTotal.WithLabelValues("h", "e", "ok")),
		"retry_attempts":      testutil.ToFloat64(o.retryAttempts.WithLabelValues("h", "e", "transient")),
		"breaker_transitions": testutil.ToFloat64(o.breakerTransitions.WithLabelValues("h", "closed", "open", "failure_threshold")),
		"dlq_records":         testutil.ToFloat64(o.dlqRecords.WithLabelValues("h", "e", "exhausted")),
		"dlq_publish_errors":  testutil.ToFloat64(o.dlqPublishErrors.WithLabelValues("h", "e")),
		"decode_failures":     testutil.ToFloat64(o.decodeFailures.WithLabelValues("invalid_json")),
		"store_errors":        testutil.ToFloat64(o.storeErrors.WithLabelValues("h", "idempotency", "begin")),
		"idempotency_stale":   testutil.ToFloat64(o.idempotencyStale.WithLabelValues("h")),
		"internal_errors":     testutil.ToFloat64(o.internalErrors.WithLabelValues("transport", "ack")),
	}
	for name, got := range counters {
		if got != want {
			t.Errorf("%s = %v, want %d", name, got, want)
		}
	}
	count, _ := poHistogram(t, reg, "eventbus_handler_duration_seconds")
	if count != want {
		t.Errorf("duration count = %d, want %d", count, want)
	}
}

func TestObserver_DoubleRegistrationPanics(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewObserver(reg)

	defer func() {
		if recover() == nil {
			t.Fatal("second NewObserver on the same registry did not panic")
		}
	}()
	NewObserver(reg)
}
