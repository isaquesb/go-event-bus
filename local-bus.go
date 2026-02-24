package event

import (
	"context"
	"log/slog"
	"sync"
)

const DefaultMaxConcurrent = 16

type LocalBusOptions struct {
	Invoker       Invoker
	OnErr         func(ctx context.Context, evt Event, err error, handler string)
	MaxConcurrent int
}

func NewLocalBus(opts LocalBusOptions) *LocalBus {
	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = DefaultMaxConcurrent
	}
	return &LocalBus{
		handlers: make(map[string][]*namedHandle),
		onErr:    opts.OnErr,
		invoker:  opts.Invoker,
		sem:      make(chan struct{}, opts.MaxConcurrent),
	}
}

// LocalBus implements an in-process async event dispatcher.
// Events are fire-and-forget, without delivery guarantees.
type LocalBus struct {
	mu       sync.RWMutex
	handlers map[string][]*namedHandle
	onErr    func(ctx context.Context, evt Event, err error, handler string)
	invoker  Invoker
	sem      chan struct{}
}

// Subscribe registers a handler for events on the given subject.
// Options can be used to configure the subscription (e.g., WithHandlerName).
func (b *LocalBus) Subscribe(
	ctx context.Context,
	subject string,
	handler HandleFn,
	opts ...SubscribeOption,
) error {
	cfg := ApplyOptions(opts...)

	handlerName := cfg.HandlerName
	if handlerName == "" {
		handlerName = subject // default to subject name
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	slog.Debug("registering handler", "event", subject, "handler", handlerName)
	b.handlers[subject] = append(b.handlers[subject], &namedHandle{
		Name: handlerName,
		Fn:   handler,
	})

	return nil
}

// RegisterSubscribers registers multiple subscribers at once.
func (b *LocalBus) RegisterSubscribers(ctx context.Context, subs ...Subscriber) error {
	for _, s := range subs {
		opts := []SubscribeOption{WithHandlerName(s.Name())}
		if p, ok := s.(SubscribeOptionsProvider); ok {
			opts = append(opts, p.SubscribeOptions()...)
		}
		for evtName, handler := range s.Events() {
			if err := b.Subscribe(ctx, evtName, handler, opts...); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close gracefully shuts down the bus.
// For LocalBus, this is a no-op since there are no external connections.
func (b *LocalBus) Close() error {
	return nil
}

func (b *LocalBus) dispatch(
	ctx context.Context,
	subject string,
	e Event,
	async bool,
) {
	b.mu.RLock()
	hList := b.handlers[subject]
	b.mu.RUnlock()

	if len(hList) == 0 {
		return
	}

	for _, h := range hList {
		select {
		case <-ctx.Done():
			return
		default:
		}

		handler := h

		exec := func() {
			if err := b.runHandler(ctx, e, handler); err != nil {
				if b.onErr != nil {
					b.onErr(ctx, e, err, handler.Name)
				}

				if withOnErr, ok := e.(OnErr); ok {
					withOnErr.OnErr(ctx, err, handler.Name)
				}
			}
		}

		if async {
			select {
			case b.sem <- struct{}{}:
			case <-ctx.Done():
				return
			}

			go func() {
				defer func() { <-b.sem }()
				exec()
			}()
		} else {
			exec()
		}
	}
}

func (b *LocalBus) Emit(ctx context.Context, e Event, opts ...EmitOption) {
	cfg := ApplyEmitOptions(e, opts...)
	b.dispatch(ctx, cfg.Subject, e, true)
}

func (b *LocalBus) EmitSync(ctx context.Context, e Event, opts ...EmitOption) {
	cfg := ApplyEmitOptions(e, opts...)
	b.dispatch(ctx, cfg.Subject, e, false)
}

func (b *LocalBus) runHandler(
	ctx context.Context,
	evt Event,
	h *namedHandle,
) error {
	return b.invoker.Invoke(
		ctx,
		evt,
		h.Name,
		func(ctx context.Context) error {
			return h.Fn(ctx, evt)
		},
	)
}
