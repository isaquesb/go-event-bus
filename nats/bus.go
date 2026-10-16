package nats

import (
	"context"
	"log/slog"
	"slices"
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
	PublishMsg(m *nats.Msg) error
	Close()
}

type BusOptions struct {
	// Timeout default para EmitSync se ctx não tiver deadline
	RequestTimeout time.Duration
	// Invoker for cross-cutting concerns (retry, metrics, etc.)
	Invoker event.Invoker
	OnErr   func(ctx context.Context, evt event.Event, err error, handler string)
	// IDGenerator mints event ids when neither WithID nor the event's Id()
	// provides one. Nil means event.NewID.
	IDGenerator func() string
	// InheritHeaders lists the header keys copied from the handled event to
	// events emitted with its ctx. Empty by default.
	InheritHeaders []string
	// DLQPublisher is the per-bus dead-letter backstop. It receives the failures
	// the invoker chain did not dead-letter (no DLQ invoker, or a failure raised
	// outside the chain) and every decode failure, which happens before the
	// chain runs. Nil means such failures are only reported and logged.
	DLQPublisher event.DLQPublisher
	// ReplyErrorMessage makes the Eventbus-Error reply of a failed SubscribeSync
	// request carry the truncated error text after the reason code. Default
	// false, so internal error text never reaches remote requesters.
	ReplyErrorMessage bool
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
		idGen:   opts.IDGenerator,
		inherit: slices.Clone(opts.InheritHeaders),

		dlq:         opts.DLQPublisher,
		replyErrMsg: opts.ReplyErrorMessage,
	}
}

// Bus implements a NATS-backed event bus.
type Bus struct {
	nc      natsConn
	reg     event.Registry
	timeout time.Duration
	invoker event.Invoker
	onErr   func(ctx context.Context, evt event.Event, err error, handler string)
	idGen   func() string
	inherit []string

	dlq         event.DLQPublisher
	replyErrMsg bool
}

// reply sends data and h to the reply subject of msg through the natsConn seam.
// It does nothing for a message without a reply subject (a plain publish).
func (b *Bus) reply(ctx context.Context, msg *nats.Msg, data []byte, h nats.Header) {
	if msg.Reply == "" {
		return
	}
	if err := b.nc.PublishMsg(&nats.Msg{Subject: msg.Reply, Header: h, Data: data}); err != nil {
		slog.WarnContext(ctx, "nats reply failed", "subject", msg.Subject, "err", err)
	}
}

// replyError sends the failure reply of a request: the HeaderError header and
// no body.
func (b *Bus) replyError(ctx context.Context, msg *nats.Msg, reason string, err error) {
	h := nats.Header{}
	h.Set(HeaderError, errorHeaderValue(reason, err, b.replyErrMsg))
	b.reply(ctx, msg, nil, h)
}

// resolve applies the bus-owned options to cfg and resolves the Metadata of an
// event about to be emitted.
func (b *Bus) resolve(ctx context.Context, e event.Event, cfg event.EmitOptions) event.Metadata {
	cfg.IDGenerator = b.idGen
	cfg.InheritHeaders = b.inherit
	return event.ResolveOutbound(ctx, e, cfg)
}

// newID returns a fresh event id from the bus generator, falling back to
// event.NewID when none is configured or it returns an empty id.
func (b *Bus) newID() string {
	if b.idGen != nil {
		if id := b.idGen(); id != "" {
			return id
		}
	}
	return event.NewID()
}

func (b *Bus) Emit(ctx context.Context, e event.Event, opts ...event.EmitOption) error {
	cfg := event.ApplyEmitOptions(e, opts...)
	md := b.resolve(ctx, e, cfg)

	ctx, span := tracer.Start(ctx, "eventbus.emit",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination", cfg.Subject),
			attribute.String("event.name", e.Name()),
		),
	)
	defer span.End()

	data, err := event.EncodeEvent(ctx, b.reg, e, md)
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
	// No user options: the signature stays EmitSync(ctx, e) so *Bus keeps
	// satisfying event.SyncBus. Lineage comes from the Metadata in ctx.
	md := b.resolve(ctx, e, event.ApplyEmitOptions(e))

	ctx, span := tracer.Start(ctx, "eventbus.emit_sync",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination", e.Name()),
			attribute.String("event.name", e.Name()),
		),
	)
	defer span.End()

	data, err := event.EncodeEvent(ctx, b.reg, e, md)
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

	if value := msg.Header.Get(HeaderError); value != "" {
		remote := parseRemoteError(e.Name(), value)
		span.RecordError(remote)
		span.SetStatus(codes.Error, remote.Error())
		return nil, remote
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
		msgCtx, evt, err := b.reg.Decode(ctx, msg.Data)
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
		msgCtx, evt, err := b.reg.Decode(ctx, msg.Data)
		if err != nil {
			return
		}

		msgCtx = event.WithDelivery(msgCtx, deliveryOf(msg))
		msgCtx = event.WithExecution(msgCtx, event.Execution{Handler: handlerName, StartedAt: time.Now()})

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
			s := event.Settle(msgCtx, event.SettleInput{
				SubscriptionCtx: ctx,
				Evt:             evt,
				Handler:         handlerName,
				Err:             handleErr,
				Publisher:       b.dlq,
				OnErr:           b.onErr,
			})
			reason := s.Reason
			if reason == "" {
				reason = event.ReasonOf(handleErr)
			}
			b.replyError(msgCtx, msg, reason, handleErr)
			return
		}

		// The reply is a new message caused by the request: a fresh id (a
		// WithId request event echoed back must not reuse the request id),
		// CausationID = request id and the request CorrelationID, both
		// resolved from the Metadata in msgCtx.
		rcfg := event.ApplyEmitOptions(evt)
		rcfg.ID = b.newID()
		md := b.resolve(msgCtx, evt, rcfg)
		resp, err := event.EncodeEvent(msgCtx, b.reg, evt, md)
		if err != nil {
			slog.ErrorContext(msgCtx, "failed to encode reply",
				"subject", subject,
				"err", err,
			)
			return
		}

		b.reply(msgCtx, msg, resp, nil)
	})

	return err
}

// deliveryOf describes msg as a delivery. NATS Core never redelivers, so every
// delivery is also the final one.
func deliveryOf(msg *nats.Msg) event.Delivery {
	return event.Delivery{
		Count:   1,
		Max:     1,
		Subject: msg.Subject,
		Raw:     msg.Data,
		Headers: map[string][]string(msg.Header),
	}
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
