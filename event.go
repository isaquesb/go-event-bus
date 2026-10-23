package event

import (
	"context"
)

type SyncBus interface {
	EmitSync(ctx context.Context, e Event) (Event, error)
}

type RegisterSubscribers interface {
	RegisterSubscribers(ctx context.Context, subs ...Subscriber) error
}

type Event interface {
	Name() string
}

// WithId lets an event choose its own message id.
//
// Id() becomes the message id of every envelope emitted for the event, so it
// must be unique per message, not per entity: two emits of events with the
// same Id() are the same message to anything that deduplicates on the id. The
// idempotency invoker keys on the same value. An empty Id() falls back to a
// generated id.
type WithId interface {
	Id() string
}

type HandlerFunc[E Event] func(ctx context.Context, evt E) error

type Invoker interface {
	Invoke(
		ctx context.Context,
		evt Event,
		handlerName string,
		handleFn func(context.Context) error,
	) error
}

type WithVersion interface {
	Version() int
}

type HandleFn = func(context.Context, Event) error

type OnErr interface {
	OnErr(ctx context.Context, err error, listenerName string)
}

type namedHandle struct {
	Name string
	Fn   HandleFn
}

type Subscriber interface {
	Name() string
	Events() map[string]HandleFn
}

// SubscribeOptionsProvider can be implemented by Subscriber to provide
// additional subscribe options (e.g. stream name for JetStream).
type SubscribeOptionsProvider interface {
	SubscribeOptions() []SubscribeOption
}
