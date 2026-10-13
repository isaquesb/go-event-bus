package prometheus

import (
	"sync"
	"testing"

	"github.com/isaquesb/go-event-bus/invoker"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestDoubleCheckedLocking(t *testing.T) {
	reg := prometheus.NewRegistry()
	p := NewProvider("test", WithRegisterer(reg))
	labels := invoker.Labels{"event": "e", "handler": "h", "result": "ok"}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.IncCounter("concurrent_counter", 1, labels)
		}()
	}
	wg.Wait()

	// Exactly one counter registered, no panics
	count := testutil.CollectAndCount(reg)
	if count != 1 {
		t.Errorf("expected 1 registered metric, got %d", count)
	}
}

func TestLabelKeySorting(t *testing.T) {
	labels := invoker.Labels{"z": "1", "a": "2", "m": "3"}
	keys := keysFromLabels(labels)
	expected := []string{"a", "m", "z"}
	if len(keys) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, keys)
	}
	for i, k := range keys {
		if k != expected[i] {
			t.Errorf("position %d: expected %q, got %q", i, expected[i], k)
		}
	}
}

func TestCounterIncrement(t *testing.T) {
	reg := prometheus.NewRegistry()
	p := NewProvider("test", WithRegisterer(reg))
	labels := invoker.Labels{"event": "x", "handler": "h", "result": "ok"}

	p.IncCounter("my_counter", 5, labels)
	p.IncCounter("my_counter", 3, labels)

	// Use testutil.ToFloat64 to assert counter value
	counter := p.getOrCreateCounter("my_counter", labels)
	val := testutil.ToFloat64(counter.With(prometheus.Labels{"event": "x", "handler": "h", "result": "ok"}))
	if val != 8.0 {
		t.Errorf("expected counter value 8.0, got %f", val)
	}
}

func TestHistogramIncrement(t *testing.T) {
	reg := prometheus.NewRegistry()
	p := NewProvider("test", WithRegisterer(reg))
	labels := invoker.Labels{"event": "x", "handler": "h", "result": "ok"}

	// Must not panic
	p.ObserveHistogram("my_hist", 0.5, labels)

	// Verify one sample was recorded
	count := testutil.CollectAndCount(reg)
	if count != 1 {
		t.Errorf("expected 1 registered metric, got %d", count)
	}
}
