// Package prometheus provides a Prometheus-based MetricProvider for the event bus invoker chain.
// It implements the invoker.MetricProvider interface to collect and expose metrics.
package prometheus

import (
	"sort"
	"sync"

	"github.com/isaquesb/go-event-bus/invoker"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Ensure Provider implements MetricProvider
var _ invoker.MetricProvider = (*Provider)(nil)

// Provider implements invoker.MetricProvider using Prometheus.
// It dynamically creates and caches metrics based on the labels provided.
type Provider struct {
	mu         sync.RWMutex
	counters   map[string]*prometheus.CounterVec
	histograms map[string]*prometheus.HistogramVec
	gauges     map[string]*prometheus.GaugeVec
	namespace  string
	registerer prometheus.Registerer
}

// Option is a functional option for configuring the Provider
type Option func(*Provider)

// WithRegisterer sets a custom Prometheus registerer
func WithRegisterer(r prometheus.Registerer) Option {
	return func(p *Provider) {
		p.registerer = r
	}
}

// NewProvider creates a new Prometheus MetricProvider.
// The namespace is prepended to all metric names (e.g., "game_chat" -> "game_chat_eventbus_*").
func NewProvider(namespace string, opts ...Option) *Provider {
	p := &Provider{
		counters:   make(map[string]*prometheus.CounterVec),
		histograms: make(map[string]*prometheus.HistogramVec),
		gauges:     make(map[string]*prometheus.GaugeVec),
		namespace:  namespace,
		registerer: prometheus.DefaultRegisterer,
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

// IncCounter increments a counter metric by n
func (p *Provider) IncCounter(name string, n int64, labels invoker.Labels) {
	counter := p.getOrCreateCounter(name, labels)
	counter.With(toPrometheusLabels(labels)).Add(float64(n))
}

// ObserveHistogram records a value in a histogram
func (p *Provider) ObserveHistogram(name string, value float64, labels invoker.Labels) {
	histogram := p.getOrCreateHistogram(name, labels)
	histogram.With(toPrometheusLabels(labels)).Observe(value)
}

// SetGauge sets a gauge metric to a specific value
func (p *Provider) SetGauge(name string, value float64, labels invoker.Labels) {
	gauge := p.getOrCreateGauge(name, labels)
	gauge.With(toPrometheusLabels(labels)).Set(value)
}

// getOrCreateCounter returns an existing counter or creates a new one
func (p *Provider) getOrCreateCounter(name string, labels invoker.Labels) *prometheus.CounterVec {
	// Check with read lock first
	p.mu.RLock()
	if c, ok := p.counters[name]; ok {
		p.mu.RUnlock()
		return c
	}
	p.mu.RUnlock()

	// Create with write lock
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if c, ok := p.counters[name]; ok {
		return c
	}

	labelNames := keysFromLabels(labels)
	c := promauto.With(p.registerer).NewCounterVec(prometheus.CounterOpts{
		Namespace: p.namespace,
		Name:      name,
		Help:      helpForMetric(name),
	}, labelNames)

	p.counters[name] = c
	return c
}

// getOrCreateHistogram returns an existing histogram or creates a new one
func (p *Provider) getOrCreateHistogram(name string, labels invoker.Labels) *prometheus.HistogramVec {
	// Check with read lock first
	p.mu.RLock()
	if h, ok := p.histograms[name]; ok {
		p.mu.RUnlock()
		return h
	}
	p.mu.RUnlock()

	// Create with write lock
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if h, ok := p.histograms[name]; ok {
		return h
	}

	labelNames := keysFromLabels(labels)
	h := promauto.With(p.registerer).NewHistogramVec(prometheus.HistogramOpts{
		Namespace: p.namespace,
		Name:      name,
		Help:      helpForMetric(name),
		Buckets:   bucketsForMetric(name),
	}, labelNames)

	p.histograms[name] = h
	return h
}

// getOrCreateGauge returns an existing gauge or creates a new one
func (p *Provider) getOrCreateGauge(name string, labels invoker.Labels) *prometheus.GaugeVec {
	// Check with read lock first
	p.mu.RLock()
	if g, ok := p.gauges[name]; ok {
		p.mu.RUnlock()
		return g
	}
	p.mu.RUnlock()

	// Create with write lock
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if g, ok := p.gauges[name]; ok {
		return g
	}

	labelNames := keysFromLabels(labels)
	g := promauto.With(p.registerer).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: p.namespace,
		Name:      name,
		Help:      helpForMetric(name),
	}, labelNames)

	p.gauges[name] = g
	return g
}

// keysFromLabels extracts sorted label names from a Labels map
func keysFromLabels(labels invoker.Labels) []string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// toPrometheusLabels converts invoker.Labels to prometheus.Labels
func toPrometheusLabels(labels invoker.Labels) prometheus.Labels {
	result := make(prometheus.Labels, len(labels))
	for k, v := range labels {
		result[k] = v
	}
	return result
}

// helpForMetric returns the help text for known metrics
func helpForMetric(name string) string {
	switch name {
	case "eventbus_invoker_calls_total":
		return "Total number of event handler invocations"
	case "eventbus_invoker_latency_seconds":
		return "Latency of event handler invocations in seconds"
	case "eventbus_retry_attempts_total":
		return "Total number of retry attempts for failed handlers"
	case "eventbus_circuit_breaker_state":
		return "Current state of circuit breakers (0=closed, 1=open, 2=half-open)"
	case "eventbus_rate_limit_rejected_total":
		return "Total number of events rejected due to rate limiting"
	case "eventbus_idempotency_duplicates_total":
		return "Total number of duplicate events blocked by idempotency check"
	case "eventbus_dlq_events_total":
		return "Total number of events sent to the dead letter queue"
	default:
		return "Auto-generated metric for " + name
	}
}

// bucketsForMetric returns appropriate histogram buckets for known metrics
func bucketsForMetric(name string) []float64 {
	switch name {
	case "eventbus_invoker_latency_seconds":
		// Latency buckets from 1ms to 10s
		return []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	default:
		return prometheus.DefBuckets
	}
}
