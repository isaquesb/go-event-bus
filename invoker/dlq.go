package invoker

import (
	"context"
	"errors"
	"github.com/isaquesb/go-event-bus"
)

var ErrSendToDLQ = errors.New("send to dlq")

type DLQPublisher interface {
	Publish(ctx context.Context, evt event.Event, cause error) error
}

func NewDLQ(p DLQPublisher, metrics MetricProvider) *DLQ {
	if metrics == nil {
		metrics = &NoopProvider{}
	}

	return &DLQ{
		publisher: p,
		metrics:   metrics,
	}
}

type DLQ struct {
	publisher DLQPublisher
	metrics   MetricProvider
}

func (i *DLQ) Invoke(
	ctx context.Context,
	evt event.Event,
	handler string,
	next func(context.Context) error,
) error {
	err := next(ctx)
	if err == nil {
		return nil
	}

	// Only capture terminal errors explicitly marked for DLQ
	if !errors.Is(err, ErrSendToDLQ) {
		return err
	}

	if dlqErr := i.publisher.Publish(ctx, evt, err); dlqErr != nil {
		i.metrics.IncCounter(
			"eventbus_dlq_publish_error_total",
			1,
			Labels{"handler": handler},
		)
		return dlqErr
	}

	i.metrics.IncCounter(
		"eventbus_dlq_published_total",
		1,
		Labels{"handler": handler},
	)

	return nil // swallow error, considered handled
}
