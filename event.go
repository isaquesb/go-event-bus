package event

import (
	"context"
)

type SyncBus interface {
	EmitSync(ctx context.Context, e Event)
}

type RegisterSubscribers interface {
	RegisterSubscribers(ctx context.Context, subs ...Subscriber) error
}

type Event interface {
	Name() string
}

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
