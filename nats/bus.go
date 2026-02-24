package nats

import (
	"context"
	"log/slog"
	"time"

	"github.com/isaquesb/go-event-bus"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("go-event-bus")

// natsConn is an internal interface over *nats.Conn to allow testing.
type natsConn interface {
	Subscribe(subj string, cb nats.MsgHandler) (*nats.Subscription, error)
	Publish(subj string, data []byte) error
	Request(subj string, data []byte, timeout time.Duration) (*nats.Msg, error)
	Close()
}

type BusOptions struct {
	// Timeout default para EmitSync se ctx não tiver deadline
	RequestTimeout time.Duration
	// Invoker for cross-cutting concerns (retry, metrics, etc.)
	Invoker event.Invoker
	OnErr   func(ctx context.Context, evt event.Event, err error, handler string)
}

func NewNatsBus(
	nc *nats.Conn,
	reg event.Registry,
	opts BusOptions,
) *Bus {
	if opts.RequestTimeout <= 0 {
		opts.RequestTimeout = 5 * time.Second
	}

	return &Bus{
		nc:      nc,
		reg:     reg,
		timeout: opts.RequestTimeout,
		invoker: opts.Invoker,
		onErr:   opts.OnErr,
	}
}

// Bus implements a NATS-backed event bus.
type Bus struct {
	nc      natsConn
	reg     event.Registry
	timeout time.Duration
	invoker event.Invoker
	onErr   func(ctx context.Context, evt event.Event, err error, handler string)
}

func (b *Bus) Emit(ctx context.Context, e event.Event, opts ...event.EmitOption) error {
	cfg := event.ApplyEmitOptions(e, opts...)

	ctx, span := tracer.Start(ctx, "eventbus.emit",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination", cfg.Subject),
			attribute.String("event.name", e.Name()),
		),
	)
	defer span.End()

	data, err := b.reg.Encode(ctx, e)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	slog.DebugContext(ctx,
		"nats publish",
		"event", e.Name(),
		"subject", cfg.Subject,
	)

	err = b.nc.Publish(cfg.Subject, data)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func (b *Bus) EmitSync(ctx context.Context, e event.Event) (event.Event, error) {
	ctx, span := tracer.Start(ctx, "eventbus.emit_sync",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination", e.Name()),
			attribute.String("event.name", e.Name()),
		),
	)
	defer span.End()

	data, err := b.reg.Encode(ctx, e)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	timeout := b.timeout
	if dl, ok := ctx.Deadline(); ok {
		timeout = time.Until(dl)
	}

	slog.DebugContext(ctx,
		"nats request",
		"event", e.Name(),
	)

	msg, err := b.nc.Request(e.Name(), data, timeout)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	_, evt, decErr := b.reg.Decode(ctx, msg.Data)
	if decErr != nil {
		span.RecordError(decErr)
		span.SetStatus(codes.Error, decErr.Error())
	}
	return evt, decErr
}

// Subscribe registers a handler for events on the given subject.
// Options can be used to configure the subscription (e.g., WithHandlerName).
func (b *Bus) Subscribe(
	ctx context.Context,
	subject string,
	handler event.HandleFn,
	opts ...event.SubscribeOption,
) error {
	cfg := event.ApplyOptions(opts...)

	handlerName := cfg.HandlerName
	if handlerName == "" {
		handlerName = subject // default to subject name
	}

	_, err := b.nc.Subscribe(subject, func(msg *nats.Msg) {
		msgCtx, evt, err := b.reg.Decode(context.Background(), msg.Data)
		if err != nil {
			slog.Error(
				"failed to decode event",
				"subject", subject,
				"err", err,
			)
			return
		}

		// Use Invoker if available
		var handleErr error
		if b.invoker != nil {
			handleErr = b.invoker.Invoke(msgCtx, evt, handlerName, func(ctx context.Context) error {
				return handler(ctx, evt)
			})
		} else {
			handleErr = handler(msgCtx, evt)
		}

		if handleErr != nil {
			if b.onErr != nil {
				b.onErr(msgCtx, evt, handleErr, handlerName)
			}

			if withOnErr, ok := evt.(event.OnErr); ok {
				withOnErr.OnErr(msgCtx, handleErr, handlerName)
			}
		}
	})

	return err
}

// SubscribeSync registers a handler that responds synchronously to requests.
// Options can be used to configure the subscription (e.g., WithHandlerName).
func (b *Bus) SubscribeSync(
	ctx context.Context,
	subject string,
	handler event.HandleFn,
	opts ...event.SubscribeOption,
) error {
	cfg := event.ApplyOptions(opts...)

	handlerName := cfg.HandlerName
	if handlerName == "" {
		handlerName = subject
	}

	_, err := b.nc.Subscribe(subject, func(msg *nats.Msg) {
		msgCtx, evt, err := b.reg.Decode(context.Background(), msg.Data)
		if err != nil {
			return
		}

		// Use Invoker if available
		var handleErr error
		if b.invoker != nil {
			handleErr = b.invoker.Invoke(msgCtx, evt, handlerName, func(ctx context.Context) error {
				return handler(ctx, evt)
			})
		} else {
			handleErr = handler(msgCtx, evt)
		}

		if handleErr != nil {
			if b.onErr != nil {
				b.onErr(msgCtx, evt, handleErr, handlerName)
			}
			return
		}

		resp, err := b.reg.Encode(msgCtx, evt)
		if err != nil {
			return
		}

		_ = msg.Respond(resp)
	})

	return err
}

// RegisterSubscribers registers multiple subscribers at once.
func (b *Bus) RegisterSubscribers(ctx context.Context, subs ...event.Subscriber) error {
	for _, s := range subs {
		opts := []event.SubscribeOption{event.WithHandlerName(s.Name())}
		if p, ok := s.(event.SubscribeOptionsProvider); ok {
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

// Close gracefully shuts down the bus connection.
func (b *Bus) Close() error {
	b.nc.Close()
	return nil
}
