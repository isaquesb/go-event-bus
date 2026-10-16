package invoker

import (
	"context"

	"github.com/isaquesb/go-event-bus"
)

// ErrSendToDLQ is the root sentinel event.ErrSendToDLQ, kept here for source
// compatibility. A terminal failure matches it.
var ErrSendToDLQ = event.ErrSendToDLQ

// DLQPublisher publishes a failed message to a dead-letter queue. It is the
// root type: Publish(ctx, event.DLQRecord). The record carries the original
// envelope bytes when the transport has them.
type DLQPublisher = event.DLQPublisher

// NewDLQ returns the DLQ invoker. A nil publisher turns it into a pass-through,
// and the transport backstop reports the failure.
func NewDLQ(p DLQPublisher, metrics MetricProvider) *DLQ {
	if metrics == nil {
		metrics = &NoopProvider{}
	}

	return &DLQ{
		publisher: p,
		metrics:   metrics,
	}
}

// DLQ dead-letters a message when the shared disposition (event.Dispose) says
// Terminal for the current delivery: a terminal error on any delivery, any
// other failure only on the final delivery. Every other outcome is returned
// unchanged for the transport to retry.
//
// Place DLQ outside Retry, so retries run first and only the final outcome is
// dead-lettered. DLQ publishes once per delivery. A failure never comes back as
// nil: a successful dead-letter returns *event.DeadLetteredError (which
// transports Ack), and a failed publish returns *event.DeadLetterFailedError.
type DLQ struct {
	publisher DLQPublisher
	metrics   MetricProvider
}

// Invoke implements event.Invoker.
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

	if i.publisher == nil || event.IsInterrupted(ctx, err) {
		return err
	}

	d, ok := event.DeliveryFromContext(ctx)
	if !ok {
		d = event.Delivery{Count: 1, Max: 1}
	}

	if event.Dispose(err, d) != event.DispositionTerminal {
		return err
	}

	rec := event.NewDLQRecord(ctx, evt, handler, err)
	if perr := event.PublishDeadLetter(ctx, i.publisher, rec); perr != nil {
		i.metrics.IncCounter(
			"eventbus_dlq_publish_error_total",
			1,
			Labels{"handler": handler},
		)
		return &event.DeadLetterFailedError{Cause: err, Reason: rec.Reason, PublishErr: perr}
	}

	i.metrics.IncCounter(
		"eventbus_dlq_published_total",
		1,
		Labels{"handler": handler},
	)

	return &event.DeadLetteredError{Cause: err, Reason: rec.Reason}
}
