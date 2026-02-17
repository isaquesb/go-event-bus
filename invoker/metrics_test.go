package invoker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus/invoker"
)

func TestMetrics_RecordsLatency(t *testing.T) {
	provider := &testMetricProvider{}
	metrics := invoker.NewMetrics(provider)

	evt := &testEvent{name: "user.created"}

	_ = metrics.Invoke(context.Background(), evt, "myhandler", func(ctx context.Context) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	if len(provider.histograms["eventbus_invoker_latency_seconds"]) != 1 {
		t.Error("expected latency histogram to be recorded")
	}

	latency := provider.histograms["eventbus_invoker_latency_seconds"][0]
	if latency < 0.01 {
		t.Errorf("expected latency >= 10ms, got %v seconds", latency)
	}
}

func TestMetrics_RecordsCallCount(t *testing.T) {
	provider := &testMetricProvider{}
	metrics := invoker.NewMetrics(provider)

	evt := &testEvent{name: "user.created"}

	for i := 0; i < 5; i++ {
		_ = metrics.Invoke(context.Background(), evt, "myhandler", func(ctx context.Context) error {
			return nil
		})
	}

	if provider.counters["eventbus_invoker_calls_total"] != 5 {
		t.Errorf("expected 5 calls, got %d", provider.counters["eventbus_invoker_calls_total"])
	}
}

func TestMetrics_TracksResultOk(t *testing.T) {
	provider := &labelTrackingMetricProvider{}
	metrics := invoker.NewMetrics(provider)

	evt := &testEvent{name: "user.created"}

	_ = metrics.Invoke(context.Background(), evt, "myhandler", func(ctx context.Context) error {
		return nil
	})

	if len(provider.calls) != 2 {
		t.Fatalf("expected 2 metric calls (histogram + counter), got %d", len(provider.calls))
	}

	// Check that result=ok is in labels
	for _, call := range provider.calls {
		if call.labels["result"] != "ok" {
			t.Errorf("expected result=ok, got result=%s", call.labels["result"])
		}
		if call.labels["event"] != "user.created" {
			t.Errorf("expected event=user.created, got event=%s", call.labels["event"])
		}
		if call.labels["handler"] != "myhandler" {
			t.Errorf("expected handler=myhandler, got handler=%s", call.labels["handler"])
		}
	}
}

func TestMetrics_TracksResultErr(t *testing.T) {
	provider := &labelTrackingMetricProvider{}
	metrics := invoker.NewMetrics(provider)

	evt := &testEvent{name: "user.created"}

	_ = metrics.Invoke(context.Background(), evt, "myhandler", func(ctx context.Context) error {
		return errors.New("failed")
	})

	// Check that result=err is in labels
	for _, call := range provider.calls {
		if call.labels["result"] != "err" {
			t.Errorf("expected result=err, got result=%s", call.labels["result"])
		}
	}
}

func TestMetrics_PropagatesError(t *testing.T) {
	metrics := invoker.NewMetrics(nil)

	handlerErr := errors.New("handler failed")

	err := metrics.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		return handlerErr
	})

	if !errors.Is(err, handlerErr) {
		t.Errorf("expected error to be propagated, got '%v'", err)
	}
}

func TestMetrics_NilProvider(t *testing.T) {
	// Should not panic with nil provider
	metrics := invoker.NewMetrics(nil)

	err := metrics.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// labelTrackingMetricProvider tracks labels for each call
type labelTrackingMetricProvider struct {
	calls []metricCall
}

type metricCall struct {
	name   string
	labels map[string]string
}

func (m *labelTrackingMetricProvider) IncCounter(name string, n int64, labels invoker.Labels) {
	m.calls = append(m.calls, metricCall{name: name, labels: labels})
}

func (m *labelTrackingMetricProvider) ObserveHistogram(name string, value float64, labels invoker.Labels) {
	m.calls = append(m.calls, metricCall{name: name, labels: labels})
}

func (m *labelTrackingMetricProvider) SetGauge(name string, value float64, labels invoker.Labels) {
	m.calls = append(m.calls, metricCall{name: name, labels: labels})
}

// NoopProvider tests
func TestNoopProvider_DoesNotPanic(t *testing.T) {
	noop := &invoker.NoopProvider{}

	// Should not panic
	noop.IncCounter("test", 1, invoker.Labels{"key": "value"})
	noop.ObserveHistogram("test", 1.0, invoker.Labels{"key": "value"})
	noop.SetGauge("test", 1.0, invoker.Labels{"key": "value"})
}
