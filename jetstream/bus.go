package jetstream

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ErrStreamRequired is returned when Subscribe is called without a stream
var ErrStreamRequired = errors.New("stream is required for JetStream subscription")

var tracer = otel.Tracer("go-event-bus")

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
}

type Bus struct {
	js       jetstream.JetStream
	registry event.Registry
	onErr    func(ctx context.Context, evt event.Event, err error, handler string)
	invoker  event.Invoker

	circuitOpenDelay time.Duration
	rateLimitDelay   time.Duration

	mu        sync.Mutex
	consumers []jetstream.ConsumeContext
}

func NewBus(
	js jetstream.JetStream,
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
	return &Bus{
		js:               js,
		registry:         registry,
		onErr:            opts.OnErr,
		invoker:          opts.Invoker,
		circuitOpenDelay: opts.CircuitOpenDelay,
		rateLimitDelay:   opts.RateLimitDelay,
	}
}

func (b *Bus) Emit(
	ctx context.Context,
	subject string,
	evt event.Event,
) error {
	ctx, span := tracer.Start(ctx, "eventbus.emit",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination", subject),
			attribute.String("event.name", evt.Name()),
		),
	)
	defer span.End()

	data, err := b.registry.Encode(ctx, evt)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	_, err = b.js.Publish(ctx, subject, data)
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

	cc, err := c.Consume(
		b.msgHandler(ctx, handlerName, handler),
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

// msgHandler returns a JetStream MessageHandler that decodes, invokes, and acks/naks messages.
// It classifies errors to avoid infinite redelivery loops:
//   - ErrDuplicate: message already processed, ack immediately
//   - ErrCircuitOpen: downstream unavailable, delay redelivery
//   - ErrRateLimited: too many events, delay redelivery
//   - other errors: nak for immediate retry (bounded by MaxDeliver/BackOff)
func (b *Bus) msgHandler(
	ctx context.Context,
	handlerName string,
	fn event.HandleFn,
) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		msgCtx, evt, err := b.registry.Decode(ctx, msg.Data())
		if err != nil {
			_ = msg.Term() // malformed forever
			return
		}

		err = b.invoker.Invoke(msgCtx, evt, handlerName, func(ctx context.Context) error {
			return fn(ctx, evt)
		})

		switch {
		case err == nil:
			_ = msg.Ack()

		case errors.Is(err, invoker.ErrDuplicate):
			_ = msg.Ack() // already processed

		case errors.Is(err, invoker.ErrCircuitOpen):
			_ = msg.NakWithDelay(b.circuitOpenDelay)

		case errors.Is(err, invoker.ErrRateLimited):
			_ = msg.NakWithDelay(b.rateLimitDelay)

		default:
			if b.onErr != nil {
				b.onErr(ctx, evt, err, handlerName)
			}
			_ = msg.Nak()
		}
	}
}
