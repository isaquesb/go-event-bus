package jetstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/isaquesb/go-event-bus"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ErrStreamRequired is returned when Subscribe is called without a stream
var ErrStreamRequired = errors.New("stream is required for JetStream subscription")

// ErrInvalidConfig is returned when consumer configuration is invalid.
var ErrInvalidConfig = errors.New("eventbus: invalid consumer configuration")

var tracer = otel.Tracer("go-event-bus")

// jsBackend is an internal interface over jetstream.JetStream to allow testing.
type jsBackend interface {
	Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
	CreateOrUpdateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error)
}

type BusOptions struct {
	Invoker event.Invoker

	OnErr func(ctx context.Context, evt event.Event, err error, handler string)

	// CircuitOpenDelay is the delay before redelivery when circuit breaker is open.
	// This should roughly match the circuit breaker's OpenTimeout.
	// If zero, defaults to 30s and logs a warning.
	CircuitOpenDelay time.Duration

	// RateLimitDelay is the delay before redelivery when rate limited.
	// If zero, defaults to 5s and logs a warning.
	RateLimitDelay time.Duration

	// IDGenerator generates event ids. Nil means event.NewID.
	IDGenerator func() string

	// InheritHeaders lists the header keys copied from the handled event to
	// events emitted with its ctx. Empty by default: nothing is inherited.
	InheritHeaders []string

	// DLQPublisher is the per-bus dead-letter backstop. It receives failures
	// that a chain DLQ invoker did not dead-letter and every undecodable
	// message. It is usually the same jetstream.NewDLQ value given to
	// invoker.NewDLQ. Nil means such messages are terminated with
	// TermWithReason and reported as dropped.
	DLQPublisher event.DLQPublisher
}

type Bus struct {
	js       jsBackend
	registry event.Registry
	onErr    func(ctx context.Context, evt event.Event, err error, handler string)
	invoker  event.Invoker
	idGen    func() string
	inherit  []string
	dlq      event.DLQPublisher

	circuitOpenDelay time.Duration
	rateLimitDelay   time.Duration

	mu        sync.Mutex
	consumers []jetstream.ConsumeContext
}

func NewBus(
	js jsBackend,
	registry event.Registry,
	opts BusOptions,
) *Bus {
	if opts.CircuitOpenDelay == 0 {
		opts.CircuitOpenDelay = 30 * time.Second
		slog.Warn("eventbus: CircuitOpenDelay not configured, using default",
			"default", opts.CircuitOpenDelay,
			"hint", "set BusOptions.CircuitOpenDelay to match your CircuitBreakerConfig.OpenTimeout",
		)
	}
	if opts.RateLimitDelay == 0 {
		opts.RateLimitDelay = 5 * time.Second
		slog.Warn("eventbus: RateLimitDelay not configured, using default",
			"default", opts.RateLimitDelay,
			"hint", "set BusOptions.RateLimitDelay to a value appropriate for your rate limit window",
		)
	}
	if opts.DLQPublisher == nil {
		slog.Warn("eventbus: BusOptions.DLQPublisher not configured, failures the chain did not dead-letter and undecodable messages are terminated and reported as dropped",
			"hint", "set BusOptions.DLQPublisher to the jetstream.NewDLQ value given to invoker.NewDLQ",
		)
	}
	return &Bus{
		js:               js,
		registry:         registry,
		onErr:            opts.OnErr,
		invoker:          opts.Invoker,
		circuitOpenDelay: opts.CircuitOpenDelay,
		rateLimitDelay:   opts.RateLimitDelay,
		idGen:            opts.IDGenerator,
		inherit:          slices.Clone(opts.InheritHeaders),
		dlq:              opts.DLQPublisher,
	}
}

// resolve applies the bus-owned options to cfg and resolves the Metadata of an
// event about to be emitted.
func (b *Bus) resolve(ctx context.Context, e event.Event, cfg event.EmitOptions) event.Metadata {
	cfg.IDGenerator = b.idGen
	cfg.InheritHeaders = b.inherit
	return event.ResolveOutbound(ctx, e, cfg)
}

func (b *Bus) Emit(
	ctx context.Context,
	evt event.Event,
	opts ...event.EmitOption,
) error {
	cfg := event.ApplyEmitOptions(evt, opts...)
	md := b.resolve(ctx, evt, cfg)

	ctx, span := tracer.Start(ctx, "eventbus.emit",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination", cfg.Subject),
			attribute.String("event.name", evt.Name()),
		),
	)
	defer span.End()

	data, err := event.EncodeEvent(ctx, b.registry, evt, md)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	_, err = b.js.Publish(ctx, cfg.Subject, data)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

// defaultBackOff is used when no BackOff is configured for a consumer.
var defaultBackOff = []time.Duration{
	1 * time.Second,
	5 * time.Second,
	30 * time.Second,
	1 * time.Minute,
}

// Subscribe registers a handler for events on the given subject.
// JetStream subscriptions require a stream name via event.WithStream option.
// Optional consumer name can be provided via event.WithConsumer.
func (b *Bus) Subscribe(
	ctx context.Context,
	subject string,
	handler event.HandleFn,
	opts ...event.SubscribeOption,
) error {
	cfg := event.ApplyOptions(opts...)

	if cfg.Stream == "" {
		return ErrStreamRequired
	}

	consumerName := cfg.Consumer
	if consumerName == "" {
		consumerName = subject
	}

	handlerName := cfg.HandlerName
	if handlerName == "" {
		handlerName = consumerName
	}

	if cfg.MaxDeliver == 0 {
		slog.Warn("eventbus: MaxDeliver not set, messages will be redelivered indefinitely on failure",
			"consumer", consumerName,
			"subject", subject,
			"hint", "use event.WithMaxDeliver(N) to limit redelivery attempts",
		)
	}

	backOff := cfg.BackOff
	if backOff == nil {
		backOff = defaultBackOff
		slog.Warn("eventbus: BackOff not set, using default progressive backoff",
			"consumer", consumerName,
			"default", defaultBackOff,
			"hint", "use event.WithBackOff([]time.Duration{...}) to configure",
		)
	}

	if cfg.MaxDeliver > 0 && cfg.MaxDeliver <= len(backOff) {
		return fmt.Errorf(
			"%w: MaxDeliver (%d) must be greater than len(BackOff) (%d) — "+
				"NATS requires MaxDeliver > len(BackOff) to apply the backoff schedule",
			ErrInvalidConfig, cfg.MaxDeliver, len(backOff),
		)
	}

	consumerCfg := jetstream.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject,
		MaxDeliver:    cfg.MaxDeliver,
		BackOff:       backOff,
	}

	c, err := b.js.CreateOrUpdateConsumer(ctx, cfg.Stream, consumerCfg)
	if err != nil {
		return err
	}

	policy := deliveryPolicy{maxDeliver: cfg.MaxDeliver, backOff: backOff}
	cc, err := c.Consume(
		b.msgHandler(ctx, handlerName, handler, policy),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			slog.Warn("consumer error",
				"handler", handlerName,
				"error", err,
			)
		}),
	)
	if err != nil {
		return err
	}

	b.mu.Lock()
	b.consumers = append(b.consumers, cc)
	b.mu.Unlock()

	return nil
}

// RegisterSubscribers registers multiple subscribers at once.
// Each subscriber must implement event.SubscribeOptionsProvider to supply at
// minimum event.WithStream, otherwise ErrStreamRequired is returned.
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

// Close gracefully drains all consumers and waits for in-flight messages.
func (b *Bus) Close() error {
	b.mu.Lock()
	consumers := b.consumers
	b.consumers = nil
	b.mu.Unlock()

	for _, cc := range consumers {
		cc.Drain()
	}
	for _, cc := range consumers {
		<-cc.Closed()
	}
	return nil
}

// deliveryPolicy is the redelivery configuration validated at Subscribe time.
// The zero value is unlimited deliveries with the default backoff.
type deliveryPolicy struct {
	// maxDeliver is the consumer MaxDeliver; values at or below 0 mean unlimited
	// and are never final.
	maxDeliver int
	// backOff is the consumer BackOff schedule, indexed by delivery count.
	backOff []time.Duration
}

// delivery describes msg as a delivery attempt. The count comes from the
// message metadata, so no Consumer.Info round trip is needed.
func (b *Bus) delivery(msg jetstream.Msg, policy deliveryPolicy) event.Delivery {
	d := event.Delivery{
		Count:   1,
		Max:     policy.maxDeliver,
		Subject: msg.Subject(),
		Raw:     msg.Data(),
		Headers: map[string][]string(msg.Headers()),
	}
	md, err := msg.Metadata()
	if err != nil {
		slog.Warn("eventbus: message metadata unavailable, treating as first delivery",
			"subject", d.Subject,
			"error", err,
		)
		return d
	}
	if md.NumDelivered > 0 {
		d.Count = int(md.NumDelivered)
	}
	d.Stream = md.Stream
	d.Consumer = md.Consumer
	d.StreamSeq = md.Sequence.Stream
	d.PublishedAt = md.Timestamp
	return d
}

// msgHandler returns a JetStream MessageHandler that decodes, invokes and
// settles a message. Every outcome goes through event.Settle once and maps to
// exactly one broker action:
//
//   - Ack: success, duplicate, or already dead-lettered by the chain
//   - Nak: only the subscription context ended (the message is redelivered at
//     once to another consumer)
//   - NakWithDelay: transient or exhausted failure, flow control, or a failed
//     dead-letter publish
//   - TermWithReason: only after the DLQ publish succeeded, or when no
//     DLQPublisher is configured (dropped)
func (b *Bus) msgHandler(
	ctx context.Context,
	handlerName string,
	fn event.HandleFn,
	policy deliveryPolicy,
) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		d := b.delivery(msg, policy)

		msgCtx, evt, err := b.registry.Decode(ctx, msg.Data())
		if err != nil {
			// A decode failure has no event and no handler run. Settle
			// dead-letters the raw bytes (permanent failures) or retries until
			// the final delivery (ErrUnknownVersion), never a silent Term.
			err = event.WrapDecodeError(err)
			s := event.Settle(event.WithDelivery(ctx, d), event.SettleInput{
				SubscriptionCtx: ctx,
				Evt:             nil,
				Handler:         handlerName,
				Err:             err,
				Publisher:       b.dlq,
				OnErr:           b.onErr,
			})
			b.apply(msg, handlerName, s, err, d, policy)
			return
		}

		msgCtx = event.WithDelivery(msgCtx, d)
		msgCtx = event.WithExecution(msgCtx, event.Execution{Handler: handlerName, StartedAt: time.Now()})

		err = b.invoker.Invoke(msgCtx, evt, handlerName, func(ctx context.Context) error {
			return fn(ctx, evt)
		})

		s := event.Settle(msgCtx, event.SettleInput{
			SubscriptionCtx: ctx,
			Evt:             evt,
			Handler:         handlerName,
			Err:             err,
			Publisher:       b.dlq,
			OnErr:           b.onErr,
		})
		b.apply(msg, handlerName, s, err, d, policy)
	}
}

// apply performs the one broker action for s. A failing ack call is logged and
// otherwise ignored: the server redelivers an unacknowledged message, which can
// duplicate a DLQ record (at-least-once, D-13).
func (b *Bus) apply(msg jetstream.Msg, handlerName string, s event.Settlement, err error, d event.Delivery, policy deliveryPolicy) {
	var aerr error
	switch s.Disposition {
	case event.DispositionAck:
		aerr = msg.Ack()
	case event.DispositionRetry:
		// Only the interruption path: the subscription context ended.
		aerr = msg.Nak()
	case event.DispositionRetryDelayed:
		errForDelay := s.Err
		if errForDelay == nil {
			errForDelay = err
		}
		aerr = msg.NakWithDelay(b.delayFor(errForDelay, d, policy.backOff))
	case event.DispositionTerminal:
		aerr = msg.TermWithReason(string(s.Outcome) + ": " + s.Reason)
	}
	if aerr != nil {
		slog.Warn("eventbus: acknowledging message failed, the broker may redeliver it",
			"handler", handlerName,
			"subject", d.Subject,
			"disposition", s.Disposition.String(),
			"error", aerr,
		)
	}
}

// delayFor is the redelivery delay for err on delivery d: the flow-control
// delays for circuit-open and rate-limited failures, otherwise the BackOff
// schedule indexed by the delivery count and clamped to its last entry.
func (b *Bus) delayFor(err error, d event.Delivery, backOff []time.Duration) time.Duration {
	if len(backOff) == 0 {
		backOff = defaultBackOff
	}
	switch {
	case errors.Is(err, event.ErrDeadLetterFailed):
		// A failed dead-letter publish follows the schedule even when its cause
		// is flow control.
	case errors.Is(err, event.ErrCircuitOpen):
		return b.circuitOpenDelay
	case errors.Is(err, event.ErrRateLimited):
		return b.rateLimitDelay
	}
	return backOff[min(max(d.Count-1, 0), len(backOff)-1)]
}
