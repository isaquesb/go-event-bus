package telemetry

import (
	"context"
	"errors"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// tmNewMetricObserver returns an Observer recording into a ManualReader.
func tmNewMetricObserver() (*Observer, *sdkmetric.ManualReader) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return NewObserver(WithMeterProvider(mp)), reader
}

// tmCollect returns the collected metrics by instrument name.
func tmCollect(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		if sm.Scope.Name != ScopeName {
			t.Errorf("instrumentation scope = %q, want %q", sm.Scope.Name, ScopeName)
		}
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

// tmAllSignals calls every Observer method once with bounded sample values.
func tmAllSignals(o *Observer) {
	ctx := context.Background()
	boom := errors.New("boom")
	o.RetryAttempt(ctx, event.RetryAttemptInfo{Handler: "h", Event: "e", Attempt: 1, Class: event.AttemptTransient, Err: boom, Delay: time.Millisecond})
	o.BreakerTransition(ctx, event.BreakerTransition{Handler: "h", From: event.BreakerClosed, To: event.BreakerOpen, Reason: event.BreakerReasonFailureThreshold, Failures: 3, Seq: 1})
	o.Failure(ctx, event.FailureContext{Handler: "h", Outcome: event.OutcomeFailed})
	o.DeadLettered(ctx, event.DeadLetterInfo{Handler: "h", Event: "e", Reason: "retries_exhausted", Subject: "orders.42"})
	o.HandlerDone(ctx, event.HandlerResult{Handler: "h", Event: "e", Outcome: event.OutcomeOK, Duration: 20 * time.Millisecond})
	o.DecodeFailed(ctx, event.DecodeFailure{Handler: "h", Subject: "orders.42", Reason: "decode_error", Err: boom})
	o.DLQPublishFailed(ctx, event.DLQPublishFailure{Handler: "h", Event: "e", Reason: "retries_exhausted", Subject: "orders.42", Err: boom})
	o.StoreError(ctx, event.StoreErrorInfo{Handler: "h", Event: "e", Store: "idempotency", Op: "begin", Err: boom})
	o.IdempotencyStale(ctx, event.IdempotencyStaleInfo{Handler: "h", Event: "e", Age: time.Second})
	o.InternalError(ctx, event.InternalErrorInfo{Component: event.ComponentTransport, Op: event.OpAck, Handler: "h", Subject: "orders.42", Err: boom})
}

// tmAttrKeys returns the sorted attribute keys of every data point, failing
// when two points of one instrument disagree.
func tmAttrKeys(t *testing.T, m metricdata.Metrics) string {
	t.Helper()
	var sets []attribute.Set
	switch d := m.Data.(type) {
	case metricdata.Sum[int64]:
		for _, p := range d.DataPoints {
			sets = append(sets, p.Attributes)
		}
	case metricdata.Gauge[int64]:
		for _, p := range d.DataPoints {
			sets = append(sets, p.Attributes)
		}
	case metricdata.Histogram[float64]:
		for _, p := range d.DataPoints {
			sets = append(sets, p.Attributes)
		}
	default:
		t.Fatalf("%s: unexpected data type %T", m.Name, m.Data)
	}
	if len(sets) == 0 {
		t.Fatalf("%s: no data points", m.Name)
	}
	var first string
	for i, s := range sets {
		var keys []string
		for _, kv := range s.ToSlice() {
			keys = append(keys, string(kv.Key))
		}
		sort.Strings(keys)
		got := strings.Join(keys, ",")
		if i == 0 {
			first = got
		} else if got != first {
			t.Errorf("%s: data points disagree on attribute keys: %q vs %q", m.Name, first, got)
		}
	}
	return first
}

func TestObserverMetrics_InstrumentNamesAndUnits(t *testing.T) {
	o, reader := tmNewMetricObserver()
	tmAllSignals(o)
	got := tmCollect(t, reader)

	want := map[string]string{
		"eventbus.handler.duration":    "s",
		"eventbus.handler":             "{message}",
		"eventbus.retry.attempts":      "{attempt}",
		"eventbus.breaker.transitions": "{transition}",
		"eventbus.breaker.state":       "{state}",
		"eventbus.dlq.records":         "{record}",
		"eventbus.dlq.publish.errors":  "{error}",
		"eventbus.decode.failures":     "{failure}",
		"eventbus.store.errors":        "{error}",
		"eventbus.idempotency.stale":   "{lock}",
		"eventbus.internal.errors":     "{error}",
	}
	if len(got) != len(want) {
		var names []string
		for n := range got {
			names = append(names, n)
		}
		sort.Strings(names)
		t.Errorf("instruments = %v, want %d of them", names, len(want))
	}
	for name, unit := range want {
		m, ok := got[name]
		if !ok {
			t.Errorf("instrument %s missing", name)
			continue
		}
		if m.Unit != unit {
			t.Errorf("%s unit = %q, want %q", name, m.Unit, unit)
		}
	}

	h, ok := got["eventbus.handler.duration"].Data.(metricdata.Histogram[float64])
	if !ok || len(h.DataPoints) != 1 {
		t.Fatalf("handler.duration data = %#v", got["eventbus.handler.duration"].Data)
	}
	wantBounds := []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	if !slices.Equal(h.DataPoints[0].Bounds, wantBounds) {
		t.Errorf("histogram bounds = %v, want %v", h.DataPoints[0].Bounds, wantBounds)
	}
	if h.DataPoints[0].Sum < 0.019 || h.DataPoints[0].Sum > 0.021 {
		t.Errorf("histogram sum = %v, want ~0.02 seconds", h.DataPoints[0].Sum)
	}
}

func TestObserverMetrics_AttributeSets(t *testing.T) {
	o, reader := tmNewMetricObserver()
	tmAllSignals(o)
	got := tmCollect(t, reader)

	want := map[string]string{
		"eventbus.handler.duration":    "event,handler,outcome",
		"eventbus.handler":             "event,handler,outcome",
		"eventbus.retry.attempts":      "event,handler,reason",
		"eventbus.dlq.records":         "event,handler,reason",
		"eventbus.breaker.transitions": "from,handler,reason,to",
		"eventbus.breaker.state":       "handler",
		"eventbus.idempotency.stale":   "handler",
		"eventbus.dlq.publish.errors":  "event,handler",
		"eventbus.decode.failures":     "reason",
		"eventbus.store.errors":        "handler,op,store",
		"eventbus.internal.errors":     "component,op",
	}
	for name, keys := range want {
		m, ok := got[name]
		if !ok {
			t.Errorf("instrument %s missing", name)
			continue
		}
		if k := tmAttrKeys(t, m); k != keys {
			t.Errorf("%s attribute keys = %q, want %q", name, k, keys)
		}
	}

	// Values come from bounded fields only: no subject, id or error text.
	for name, m := range got {
		sum, ok := m.Data.(metricdata.Sum[int64])
		if !ok {
			continue
		}
		for _, p := range sum.DataPoints {
			for _, kv := range p.Attributes.ToSlice() {
				v := kv.Value.Emit()
				if strings.Contains(v, "orders.42") || strings.Contains(v, "boom") {
					t.Errorf("%s attribute %s leaks %q", name, kv.Key, v)
				}
			}
		}
	}
	rt, ok := got["eventbus.retry.attempts"].Data.(metricdata.Sum[int64])
	if !ok || len(rt.DataPoints) != 1 {
		t.Fatalf("retry.attempts data = %#v", got["eventbus.retry.attempts"].Data)
	}
	if v, _ := rt.DataPoints[0].Attributes.Value("reason"); v.AsString() != "transient" {
		t.Errorf("retry reason = %q, want the attempt class transient", v.AsString())
	}
}

func TestObserverMetrics_BreakerGaugeSeq(t *testing.T) {
	o, reader := tmNewMetricObserver()
	ctx := context.Background()
	tr := func(seq uint64, to event.BreakerState) {
		o.BreakerTransition(ctx, event.BreakerTransition{Handler: "h", From: event.BreakerClosed, To: to, Reason: event.BreakerReasonFailureThreshold, Seq: seq})
	}
	state := func() int64 {
		t.Helper()
		g, ok := tmCollect(t, reader)["eventbus.breaker.state"].Data.(metricdata.Gauge[int64])
		if !ok || len(g.DataPoints) != 1 {
			t.Fatalf("breaker.state data = %#v", g)
		}
		return g.DataPoints[0].Value
	}

	tr(2, event.BreakerOpen)
	tr(1, event.BreakerClosed) // late delivery of an older transition
	if got := state(); got != 1 {
		t.Errorf("gauge after Seq 2 (open) then Seq 1 (closed) = %d, want 1", got)
	}
	tr(3, event.BreakerHalfOpen)
	if got := state(); got != 2 {
		t.Errorf("gauge after Seq 3 (half_open) = %d, want 2", got)
	}

	// The transitions counter still counts every transition.
	sum, ok := tmCollect(t, reader)["eventbus.breaker.transitions"].Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatal("eventbus.breaker.transitions missing")
	}
	var total int64
	for _, p := range sum.DataPoints {
		total += p.Value
	}
	if total != 3 {
		t.Errorf("breaker.transitions total = %d, want 3", total)
	}
}

func TestObserverMetrics_DisabledWithoutProvider(t *testing.T) {
	// No global MeterProvider fallback: nothing may be recorded and nothing may
	// panic. The ManualReader below is attached to a provider the Observer never
	// receives.
	reader := sdkmetric.NewManualReader()
	_ = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	o := NewObserver()
	tmAllSignals(o)
	if got := tmCollect(t, reader); len(got) != 0 {
		t.Errorf("an Observer without WithMeterProvider recorded %d instruments", len(got))
	}
}

func TestObserverMetrics_FailureRecordsNothing(t *testing.T) {
	o, reader := tmNewMetricObserver()
	o.Failure(context.Background(), event.FailureContext{Handler: "h", Outcome: event.OutcomeDeadLettered})
	if got := tmCollect(t, reader); len(got) != 0 {
		t.Errorf("Failure recorded %d instruments, want none", len(got))
	}
}
