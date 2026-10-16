package event

import (
	"context"
	"time"
)

// Delivery describes one delivery attempt of a message to a handler. A
// transport puts it in the handler context with WithDelivery, and Dispose,
// Settle and NewDLQRecord read it back with DeliveryFromContext.
//
// Transports without redelivery (LocalBus, NATS Core) set Count 1 and Max 1,
// so the single delivery is also the final one. The zero value is "unlimited":
// it is never final. A caller that finds no Delivery in its context treats the
// message as final, because no redelivery is known.
//
// Delivery is read-only by contract: do not mutate Raw or Headers.
type Delivery struct {
	// Count is the 1-based delivery number (JetStream NumDelivered, 1 for
	// transports without redelivery).
	Count int
	// Max is the maximum number of deliveries. Values at or below 0 mean
	// unlimited; transports without redelivery set 1.
	Max int
	// Subject is the subject the message was received on.
	Subject string
	// Raw is the original envelope bytes, exactly as received. It is nil for
	// LocalBus, which has no wire form.
	Raw []byte
	// Headers are the transport headers as received, nil when there are none.
	Headers map[string][]string
	// Stream is the JetStream stream name, empty elsewhere.
	Stream string
	// Consumer is the JetStream consumer name, empty elsewhere.
	Consumer string
	// StreamSeq is the JetStream stream sequence, 0 elsewhere.
	StreamSeq uint64
	// PublishedAt is the broker timestamp, zero when unknown.
	PublishedAt time.Time
}

// Final reports whether no further delivery will follow this one: Max is
// positive and Count has reached it. Max at or below 0 is never final.
func (d Delivery) Final() bool { return d.Max > 0 && d.Count >= d.Max }

// Execution describes the handler run that is in progress. A transport or the
// invoker chain puts it in the context with WithExecution.
type Execution struct {
	// Handler is the name of the handler being executed.
	Handler string
	// StartedAt is when the first in-process attempt of this delivery started.
	StartedAt time.Time
}

type deliveryKey struct{}

// WithDelivery returns a context that carries d.
func WithDelivery(ctx context.Context, d Delivery) context.Context {
	return context.WithValue(ctx, deliveryKey{}, d)
}

// DeliveryFromContext returns the Delivery of the message being handled. The
// boolean is false, and the Delivery zero, when ctx carries none.
func DeliveryFromContext(ctx context.Context) (Delivery, bool) {
	d, ok := ctx.Value(deliveryKey{}).(Delivery)
	return d, ok
}

type executionKey struct{}

// WithExecution returns a context that carries x.
func WithExecution(ctx context.Context, x Execution) context.Context {
	return context.WithValue(ctx, executionKey{}, x)
}

// ExecutionFromContext returns the Execution of the handler being run. The
// boolean is false, and the Execution zero, when ctx carries none.
func ExecutionFromContext(ctx context.Context) (Execution, bool) {
	x, ok := ctx.Value(executionKey{}).(Execution)
	return x, ok
}
