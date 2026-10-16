package event

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"
)

const DefaultMaxConcurrent = 16

// passthroughInvoker is a no-op Invoker that calls the handler directly.
// Used as default when no Invoker chain is configured.
type passthroughInvoker struct{}

func (p *passthroughInvoker) Invoke(
	ctx context.Context,
	_ Event,
	_ string,
	fn func(context.Context) error,
) error {
	return fn(ctx)
}

type LocalBusOptions struct {
	Invoker       Invoker
	OnErr         func(ctx context.Context, evt Event, err error, handler string)
	MaxConcurrent int

	// IDGenerator mints ids for events that carry no id of their own.
	// Nil means NewID.
	IDGenerator func() string

	// InheritHeaders lists the header keys copied from the parent event's
	// Metadata to events emitted from its handlers. Empty by default: no
	// header crosses a causal hop unless listed here.
	InheritHeaders []string

	// DLQPublisher is the per-bus dead-letter backstop. A handler failure that
	// a chain DLQ invoker did not already dead-letter (an error raised outside
	// it, or no DLQ invoker at all) is published here. Users usually pass the
	// same publisher they gave invoker.NewDLQ. The record carries Evt and an
	// empty Raw, so a broker-backed publisher needs a registry to encode the
	// event. Nil means terminal failures are reported as dropped.
	DLQPublisher DLQPublisher
}

func NewLocalBus(opts LocalBusOptions) *LocalBus {
	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = DefaultMaxConcurrent
	}
	if opts.Invoker == nil {
		opts.Invoker = &passthroughInvoker{}
	}
	return &LocalBus{
		handlers: make(map[string][]*namedHandle),
		onErr:    opts.OnErr,
		dlq:      opts.DLQPublisher,
		invoker:  opts.Invoker,
		sem:      make(chan struct{}, opts.MaxConcurrent),
		idGen:    opts.IDGenerator,
		inherit:  slices.Clone(opts.InheritHeaders),
	}
}

// LocalBus implements an in-process async event dispatcher.
// Events are fire-and-forget, without delivery guarantees.
type LocalBus struct {
	mu       sync.RWMutex
	handlers map[string][]*namedHandle
	onErr    func(ctx context.Context, evt Event, err error, handler string)
	dlq      DLQPublisher
	invoker  Invoker
	sem      chan struct{}
	idGen    func() string
	inherit  []string
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
			// LocalBus never redelivers, so every delivery is the final one.
			hctx := WithDelivery(ctx, Delivery{Count: 1, Max: 1, Subject: subject})
			hctx = WithExecution(hctx, Execution{Handler: handler.Name, StartedAt: time.Now()})
			if err := b.runHandler(hctx, e, handler); err != nil {
				// Settle is the single place that dead-letters and reports; a
				// local bus has no broker action to take on the Settlement.
				_ = Settle(hctx, SettleInput{
					SubscriptionCtx: ctx,
					Evt:             e,
					Handler:         handler.Name,
					Err:             err,
					Publisher:       b.dlq,
					OnErr:           b.onErr,
				})
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
	cfg.IDGenerator = b.idGen
	cfg.InheritHeaders = b.inherit
	md := ResolveOutbound(ctx, e, cfg)
	b.dispatch(WithMetadata(ctx, md), cfg.Subject, e, true)
}

func (b *LocalBus) EmitSync(ctx context.Context, e Event, opts ...EmitOption) (Event, error) {
	cfg := ApplyEmitOptions(e, opts...)
	cfg.IDGenerator = b.idGen
	cfg.InheritHeaders = b.inherit
	md := ResolveOutbound(ctx, e, cfg)
	b.dispatch(WithMetadata(ctx, md), cfg.Subject, e, false)
	return e, nil
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
