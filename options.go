package event

import "time"

// EmitOptions holds configuration for Emit
type EmitOptions struct {
	// Subject overrides the default publish subject (defaults to evt.Name())
	Subject string

	// ID is the message id set by WithID. Empty means unset.
	ID string

	// CorrelationID is the correlation id set by WithCorrelationID.
	// Empty means unset.
	CorrelationID string

	// IDGenerator mints an id when neither WithID nor the event's Id()
	// provides one. It is populated by the bus from its own options after
	// the user options are applied; a value set through an EmitOption is
	// overwritten by the bus. Nil means NewID.
	IDGenerator func() string

	// Headers holds the user headers set by WithHeader. It is nil until the
	// first WithHeader.
	Headers map[string]string

	// InheritHeaders lists the header keys copied from the parent event's
	// Metadata to the emitted event. It is populated by the bus from its own
	// options after the user options are applied and overwritten by the bus.
	InheritHeaders []string
}

// EmitOption configures Emit behavior
type EmitOption func(*EmitOptions)

// WithSubject overrides the subject used when publishing the event.
// Defaults to evt.Name() if not set.
func WithSubject(subject string) EmitOption {
	return func(o *EmitOptions) {
		o.Subject = subject
	}
}

// WithID sets the message id of the emitted event, overriding both the
// event's Id() and the generated id. An empty id leaves the id unset.
func WithID(id string) EmitOption {
	return func(o *EmitOptions) {
		o.ID = id
	}
}

// WithCorrelationID sets the correlation id of the emitted event, overriding
// the one inherited from the parent event. An empty id leaves it unset.
func WithCorrelationID(id string) EmitOption {
	return func(o *EmitOptions) {
		o.CorrelationID = id
	}
}

// WithHeader attaches a user header to the emitted event. It can be repeated;
// the last value for a key wins and an empty key is ignored. An explicit
// header overrides a value inherited from the parent event. The reserved
// trace keys (traceparent, tracestate, baggage) are owned by the transport.
func WithHeader(key, value string) EmitOption {
	return func(o *EmitOptions) {
		if key == "" {
			return
		}
		if o.Headers == nil {
			o.Headers = make(map[string]string)
		}
		o.Headers[key] = value
	}
}

// ApplyEmitOptions applies all emit options and returns the result
func ApplyEmitOptions(evt Event, opts ...EmitOption) EmitOptions {
	o := EmitOptions{Subject: evt.Name()}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// SubscribeOptions holds configuration for Subscribe
type SubscribeOptions struct {
	// HandlerName identifies the handler for metrics and logging
	HandlerName string

	// Stream is the JetStream stream name for subscription
	Stream string

	// Consumer is the durable consumer name for JetStream
	Consumer string

	// MaxDeliver limits redelivery attempts per message.
	// If zero, no limit is applied (JetStream default) and a warning is logged.
	MaxDeliver int

	// BackOff defines delays between redelivery attempts.
	// If nil, a default progressive backoff is used and a warning is logged.
	BackOff []time.Duration
}

// SubscribeOption configures Subscribe behavior
type SubscribeOption func(*SubscribeOptions)

// WithHandlerName sets the handler identifier for metrics and logging
func WithHandlerName(name string) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.HandlerName = name
	}
}

// WithStream sets the JetStream stream name for subscription
func WithStream(stream string) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Stream = stream
	}
}

// WithConsumer sets the durable consumer name for JetStream subscription
func WithConsumer(consumer string) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Consumer = consumer
	}
}

// WithMaxDeliver limits the number of redelivery attempts per message.
func WithMaxDeliver(n int) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.MaxDeliver = n
	}
}

// WithBackOff sets the delays between redelivery attempts.
func WithBackOff(delays []time.Duration) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.BackOff = delays
	}
}

// ApplyOptions applies all options and returns the result
func ApplyOptions(opts ...SubscribeOption) SubscribeOptions {
	var o SubscribeOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
