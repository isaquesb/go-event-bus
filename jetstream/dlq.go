package jetstream

import (
	"context"
	"encoding/json"
	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"

	"github.com/nats-io/nats.go/jetstream"
)

type DLQ struct {
	js      jetstream.JetStream
	metrics invoker.MetricProvider
	prefix  string
}

func NewDQL(js jetstream.JetStream, metrics invoker.MetricProvider, prefix string) *DLQ {
	if metrics == nil {
		metrics = &invoker.NoopProvider{}
	}
	if prefix == "" {
		prefix = "dql."
	}
	return &DLQ{
		js:      js,
		prefix:  prefix,
		metrics: metrics,
	}
}

func (d *DLQ) Publish(
	ctx context.Context,
	evt event.Event,
	err error,
) error {
	data, _ := json.Marshal(map[string]any{
		"event": evt,
		"error": err.Error(),
	})

	_, err = d.js.Publish(ctx, d.prefix+evt.Name(), data)

	if err != nil {
		d.metrics.IncCounter(
			"dlq_publish_error_total",
			1,
			invoker.Labels{},
		)
		return err
	}

	d.metrics.IncCounter(
		"dlq_published_total",
		1,
		invoker.Labels{},
	)

	return nil
}
