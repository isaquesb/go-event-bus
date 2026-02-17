package invoker

import (
	"context"
	"github.com/isaquesb/go-event-bus"
	"time"
)

type MetricProvider interface {
	IncCounter(name string, n int64, labels Labels)
	ObserveHistogram(name string, value float64, labels Labels)
	SetGauge(name string, value float64, labels Labels)
}

type Labels map[string]string

type Metrics struct {
	metrics MetricProvider
}

func NewMetrics(p MetricProvider) *Metrics {
	if nil == p {
		p = &NoopProvider{}
	}
	return &Metrics{metrics: p}
}

func (m *Metrics) Invoke(
	ctx context.Context,
	evt event.Event,
	handlerName string,
	handle func(context.Context) error,
) error {
	start := time.Now()

	err := handle(ctx)

	labels := Labels{
		"event":   evt.Name(),
		"handler": handlerName,
		"result":  "ok",
	}

	if err != nil {
		labels["result"] = "err"
	}

	m.metrics.ObserveHistogram(
		"eventbus_invoker_latency_seconds",
		time.Since(start).Seconds(),
		labels,
	)

	m.metrics.IncCounter(
		"eventbus_invoker_calls_total",
		1,
		labels,
	)

	return err
}

type NoopProvider struct{}

func (n *NoopProvider) IncCounter(_ string, _ int64, _ Labels)         {}
func (n *NoopProvider) ObserveHistogram(_ string, _ float64, _ Labels) {}
func (n *NoopProvider) SetGauge(_ string, _ float64, _ Labels)         {}
