package jetstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// ErrStreamRequired is returned when Subscribe is called without a stream
var ErrStreamRequired = errors.New("stream is required for JetStream subscription")

// ErrInvalidConfig is returned when consumer configuration is invalid.
var ErrInvalidConfig = errors.New("eventbus: invalid consumer configuration")

// tracerName is the instrumentation scope of the producer spans.
const tracerName = "github.com/isaquesb/go-event-bus/jetstream"

// jsBackend is an internal interface over jetstream.JetStream to allow testing.
// Publishing goes through PublishMsg, not Publish, so a mock can read the
// Nats-Msg-Id header Emit sets: jetstream.WithMsgID only sets the same header,
// so the wire result is identical.
type jsBackend interface {
	PublishMsg(ctx context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
	CreateOrUpdateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error)
}

// PublishRetryPolicy bounds the internal retry of transient publish failures
// (see BusOptions.PublishRetry). The zero value means the defaults noted on
// each field.
type PublishRetryPolicy struct {
	// MaxAttempts is the total number of publish attempts, the first one
	// included. Values at or below 0 mean 3; 1 disables the retry.
	MaxAttempts int

	// BaseDelay is the backoff before the second attempt; it doubles per
	// attempt up to MaxDelay and carries equal jitter. 0 means 100 ms.
	BaseDelay time.Duration

	// MaxDelay caps the backoff between attempts. 0 means 1 s.
	MaxDelay time.Duration

	// AttemptTimeout bounds each attempt, so a lost acknowledgement cannot
	// consume the whole caller deadline. 0 means 2 s; a negative value means no
	// per-attempt bound beyond the caller ctx and the nats.go default.
	AttemptTimeout time.Duration
}

type BusOptions struct {
	// Invoker wraps every handler call. Nil means no invokers: the handler is
	// called directly (invoker.NewChain()).
	Invoker event.Invoker

	// Observer receives every signal of the bus: the Failure of each terminal
	// outcome (exactly once across redeliveries, never on an intermediate
	// NakWithDelay), HandlerDone, DecodeFailed, DLQPublishFailed and
	// InternalError. It is configured once here and travels in the handler ctx.
	// Nil means event.NewSlogObserver(nil), so failures are never silent;
	// event.NopObserver{} silences the bus. The bus wraps it with
	// event.MultiObserver, which isolates its panics.
	Observer event.Observer

	// TracerProvider creates the producer spans of Emit. Nil means the global
	// provider (otel.GetTracerProvider), resolved at call time.
	TracerProvider trace.TracerProvider

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

	// PublishRetry retries transient publish failures with the same
	// Nats-Msg-Id, so the stream deduplicates a retry whose first attempt was
	// stored but whose acknowledgement was lost. The zero value is 3 attempts
	// with a 100 ms to 1 s jittered backoff and a 2 s per-attempt timeout.
	PublishRetry PublishRetryPolicy

	// ShutdownTimeout is the bound Close applies to Shutdown. Values at or
	// below 0 mean event.DefaultShutdownTimeout.
	ShutdownTimeout time.Duration
}

var _ event.Shutdowner = (*Bus)(nil)

type Bus struct {
	js       jsBackend
	registry event.Registry
	invoker  event.Invoker
	idGen    func() string
	inherit  []string
	dlq      event.DLQPublisher
	pubRetry PublishRetryPolicy

	// observer is the configured bus Observer, installed in every handler ctx.
	// nil (a struct-literal bus) means event.NopObserver{}, see obs.
	observer event.Observer
	// tp creates the producer spans; nil means the global provider.
	tp trace.TracerProvider

	shutdownTimeout time.Duration

	circuitOpenDelay time.Duration
	rateLimitDelay   time.Duration

	// life is the shutdown state machine shared with LocalBus and NATS Core.
	life event.Lifecycle

	mu        sync.Mutex
	consumers []jetstream.ConsumeContext
	// drained is nil until the first Shutdown, which owns the consumer drain and
	// closes it when the drain step ends. draining is the consumers that drain
	// owns, the ones force stops.
	drained  chan struct{}
	draining []jetstream.ConsumeContext
	stopOnce sync.Once
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
	if opts.Invoker == nil {
		opts.Invoker = invoker.NewChain()
	}
	if opts.DLQPublisher == nil {
		slog.Warn("eventbus: BusOptions.DLQPublisher not configured, failures the chain did not dead-letter and undecodable messages are terminated and reported as dropped",
			"hint", "set BusOptions.DLQPublisher to the jetstream.NewDLQ value given to invoker.NewDLQ",
		)
	}
	if opts.ShutdownTimeout <= 0 {
		opts.ShutdownTimeout = event.DefaultShutdownTimeout
	}
	obs := opts.Observer
	if obs == nil {
		obs = event.NewSlogObserver(nil)
	}
	return &Bus{
		pubRetry:         opts.PublishRetry.normalized(),
		shutdownTimeout:  opts.ShutdownTimeout,
		js:               js,
		registry:         registry,
		observer:         event.MultiObserver(obs),
		tp:               opts.TracerProvider,
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

// Emit encodes evt and publishes it to the stream that covers the subject.
//
// The message carries the resolved Metadata.ID as its Nats-Msg-Id header, so
// the id precedence WithID, then Event.Id(), then a generated id decides the
// broker dedup key. The stream deduplicates within its Duplicates window (2
// minutes by default); an app-level re-Emit is deduplicated only when it
// carries the same id (WithID or Event.Id()), because generated ids differ per
// call. A publish acknowledged as a duplicate is not an error.
//
// Once Shutdown or Close began, Emit returns event.ErrBusClosed, except for a
// handler emitting with its own ctx while its message is still in flight.
func (b *Bus) Emit(
	ctx context.Context,
	evt event.Event,
	opts ...event.EmitOption,
) error {
	release, err := b.life.Admit(ctx)
	if err != nil {
		return err
	}
	defer release()

	cfg := event.ApplyEmitOptions(evt, opts...)
	md := b.resolve(ctx, evt, cfg)

	ctx, span := b.startPublish(ctx, cfg.Subject, md.ID)
	defer span.End()

	data, err := event.EncodeEvent(ctx, b.registry, evt, md)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	msg := &nats.Msg{Subject: cfg.Subject, Data: data, Header: nats.Header{}}
	msg.Header.Set(jetstream.MsgIDHeader, md.ID)

	ack, attempts, err := b.publish(ctx, msg)
	span.SetAttributes(attribute.Int("eventbus.publish.attempts", attempts))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if ack != nil && ack.Duplicate {
		slog.DebugContext(ctx, "eventbus: publish acknowledged as duplicate",
			"subject", cfg.Subject,
			"msg_id", md.ID,
		)
	}
	return nil
}

// Defaults of PublishRetryPolicy.
const (
	defaultPublishAttempts       = 3
	defaultPublishBaseDelay      = 100 * time.Millisecond
	defaultPublishMaxDelay       = time.Second
	defaultPublishAttemptTimeout = 2 * time.Second
)

// normalized replaces each zero field with its default. A negative
// AttemptTimeout is kept: it disables the per-attempt bound.
func (p PublishRetryPolicy) normalized() PublishRetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = defaultPublishAttempts
	}
	if p.BaseDelay <= 0 {
		p.BaseDelay = defaultPublishBaseDelay
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = defaultPublishMaxDelay
	}
	if p.AttemptTimeout == 0 {
		p.AttemptTimeout = defaultPublishAttemptTimeout
	}
	return p
}

// backoff is the wait after the given failed attempt (1-based): BaseDelay
// doubled per attempt, capped at MaxDelay, with equal jitter (a random value
// between half the delay and the full delay).
func (p PublishRetryPolicy) backoff(attempt int) time.Duration {
	delay := p.MaxDelay
	if shift := attempt - 1; shift < 32 && p.BaseDelay <= p.MaxDelay>>shift {
		delay = p.BaseDelay << shift
	}
	half := delay / 2
	return half + time.Duration(rand.Int64N(int64(delay-half)+1))
}

// retryablePublish reports whether err is a transient publish failure worth
// another attempt with the same message id: a per-attempt timeout or lost
// acknowledgement while the caller ctx is still alive, no responders, or no
// stream response. Server API errors, a closed connection and everything else
// are permanent.
func retryablePublish(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, jetstream.ErrNoStreamResponse)
}

// publish sends msg, retrying transient failures with the same msg (and so the
// same Nats-Msg-Id, which the stream deduplicates). It returns the ack, the
// number of attempts made and the last error. No attempt starts once ctx is
// done, and the wait before an attempt is skipped when the caller deadline
// would pass first.
func (b *Bus) publish(ctx context.Context, msg *nats.Msg) (*jetstream.PubAck, int, error) {
	policy := b.pubRetry
	var lastErr error
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr == nil {
				lastErr = err
			}
			return nil, attempt - 1, lastErr
		}

		actx, cancel := ctx, context.CancelFunc(func() {})
		if policy.AttemptTimeout > 0 {
			actx, cancel = context.WithTimeout(ctx, policy.AttemptTimeout)
		}
		ack, err := b.js.PublishMsg(actx, msg)
		cancel()
		if err == nil {
			return ack, attempt, nil
		}
		lastErr = err
		if attempt >= policy.MaxAttempts || !retryablePublish(ctx, err) {
			return nil, attempt, err
		}

		delay := policy.backoff(attempt)
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) <= delay {
			return nil, attempt, err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, attempt, err
		case <-timer.C:
		}
	}
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
	if b.life.Closing() {
		return event.ErrBusClosed
	}

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
		jetstream.ConsumeErrHandler(b.consumeErr(ctx, handlerName, subject)),
	)
	if err != nil {
		return err
	}

	// Shutdown may have begun while Consume ran: a consumer registered after
	// the drain took its list would escape it.
	b.mu.Lock()
	if b.life.Closing() {
		b.mu.Unlock()
		cc.Stop()
		return event.ErrBusClosed
	}
	b.consumers = append(b.consumers, cc)
	b.mu.Unlock()

	return nil
}

// consumeErr is the error handler of a consumer's Consume loop: it receives the
// errors nats.go raises while pulling messages (heartbeat loss, a deleted
// consumer, a broken connection) and reports each as an InternalError signal
// (Op consume). The bus Observer is installed on the subscribe ctx once, when
// the handler is built.
func (b *Bus) consumeErr(ctx context.Context, handlerName, subject string) func(jetstream.ConsumeContext, error) {
	octx := event.WithObserver(ctx, b.obs())
	return func(_ jetstream.ConsumeContext, err error) {
		event.ObserverFromContext(octx).InternalError(octx, event.InternalErrorInfo{
			Component: event.ComponentTransport,
			Op:        event.OpConsume,
			Handler:   handlerName,
			Subject:   subject,
			Err:       err,
		})
	}
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

// Shutdown stops accepting work and drains the bus bounded by ctx.
//
// New Emit and Subscribe calls return event.ErrBusClosed, except a handler
// emitting with its own ctx while its message is in flight. Every consumer is
// drained: messages already buffered are handed back to the server with Nak
// instead of running (D-27), and Shutdown waits for each consumer to report
// Closed and for the in-flight handlers to return.
//
// Concurrent and repeated calls share one drain. The first caller owns it; a
// later caller waits for it, bounded by its own ctx, and never reports a
// completed drain while a consumer has not reported Closed. A caller whose ctx
// expires first stops the consumers, interrupts the running handlers with cause
// event.ErrShutdownInterrupted (their messages are Naked, never dead-lettered)
// and returns ctx.Err(); the other callers then stop waiting. Otherwise each
// caller returns nil once no handler is in flight.
func (b *Bus) Shutdown(ctx context.Context) error {
	b.life.BeginShutdown()

	b.mu.Lock()
	if drained := b.drained; drained != nil {
		b.mu.Unlock()
		select {
		case <-drained:
		case <-ctx.Done():
			b.force()
			return ctx.Err()
		}
		return b.life.Wait(ctx)
	}
	drained := make(chan struct{})
	b.drained = drained
	b.draining = b.consumers
	b.consumers = nil
	consumers := b.draining
	b.mu.Unlock()

	if err := b.drain(ctx, consumers, drained); err != nil {
		return err
	}
	return b.life.Wait(ctx)
}

// drain is the owner's drain step: it drains every consumer and waits for each
// to report Closed, then closes drained. It stops waiting when a concurrent
// caller forced the shutdown, and forces it itself, returning ctx.Err(), when
// ctx expires first.
func (b *Bus) drain(ctx context.Context, consumers []jetstream.ConsumeContext, drained chan struct{}) error {
	defer close(drained)
	for _, cc := range consumers {
		cc.Drain()
	}
	for _, cc := range consumers {
		select {
		case <-cc.Closed():
		case <-b.life.Context().Done():
			return nil
		case <-ctx.Done():
			b.force()
			return ctx.Err()
		}
	}
	return nil
}

// force stops the drained consumers, once however many callers reach their
// deadline, and interrupts the handlers.
func (b *Bus) force() {
	b.stopOnce.Do(func() {
		b.mu.Lock()
		consumers := b.draining
		b.mu.Unlock()
		for _, cc := range consumers {
			cc.Stop()
		}
	})
	b.life.Interrupt()
}

// Close shuts the bus down with Shutdown, bounded by BusOptions.ShutdownTimeout
// (event.DefaultShutdownTimeout when unset). It never waits without a bound: on
// timeout it returns context.DeadlineExceeded after interrupting the handlers.
func (b *Bus) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), b.shutdownTimeout)
	defer cancel()
	return b.Shutdown(ctx)
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

// obs returns the bus Observer, event.NopObserver{} for a struct-literal bus
// built without one.
func (b *Bus) obs() event.Observer {
	if b.observer == nil {
		return event.NopObserver{}
	}
	return b.observer
}

// internalError reports an error inside the adapter as an InternalError signal.
func (b *Bus) internalError(ctx context.Context, op, handler, subject string, err error) {
	b.obs().InternalError(ctx, event.InternalErrorInfo{
		Component: event.ComponentTransport,
		Op:        op,
		Handler:   handler,
		Subject:   subject,
		Err:       err,
	})
}

// tracer returns the tracer of the producer spans: BusOptions.TracerProvider, or
// the global provider resolved at call time when none was configured.
func (b *Bus) tracer() trace.Tracer {
	tp := b.tp
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	return tp.Tracer(tracerName)
}

// startPublish starts the producer span of an Emit following the OTel messaging
// conventions: "publish <subject>", kind Producer. The attributes are ids and
// the subject only, never the payload.
func (b *Bus) startPublish(ctx context.Context, subject, id string) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("nats"),
		semconv.MessagingOperationName("publish"),
		semconv.MessagingOperationTypeSend,
		semconv.MessagingDestinationName(subject),
	}
	if id != "" {
		attrs = append(attrs, semconv.MessagingMessageID(id))
	}
	return b.tracer().Start(ctx, "publish "+subject,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attrs...),
	)
}

// msgScope installs the bus Observer and a fresh FailureScope on ctx. Every
// delivery gets its own scope, so the exactly-once guard of one delivery never
// leaks into another, and the observer overwrites one inherited from a bus the
// handler ctx came from. A non-nil g sees every Failure on its way to the bus
// Observer, see guardObserver.
func (b *Bus) msgScope(ctx context.Context, g *settleGuard) context.Context {
	var o event.Observer = b.obs()
	if g != nil {
		o = guardObserver{Observer: o, g: g}
	}
	ctx = event.WithObserver(ctx, o)
	ctx, _ = event.WithFailureScope(ctx)
	return ctx
}

// delivery describes msg as a delivery attempt. The count comes from the
// message metadata, so no Consumer.Info round trip is needed.
func (b *Bus) delivery(ctx context.Context, msg jetstream.Msg, policy deliveryPolicy) event.Delivery {
	d := event.Delivery{
		System:  "nats",
		Count:   1,
		Max:     policy.maxDeliver,
		Subject: msg.Subject(),
		Raw:     msg.Data(),
		Headers: map[string][]string(msg.Headers()),
	}
	md, err := msg.Metadata()
	if err != nil {
		// The delivery is treated as the first one: the count is a lower bound.
		b.internalError(ctx, event.OpConsume, "", d.Subject,
			fmt.Errorf("jetstream: message metadata unavailable, treating as first delivery: %w", err))
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
//
// The bus Observer and a fresh FailureScope are installed on the handler ctx
// before Decode, so Settle reports each terminal outcome to Observer.Failure
// exactly once and never on an intermediate NakWithDelay.
//
// A panic outside the handler (Decode, Settle, the DLQ publisher) is recovered,
// reported as an InternalError signal (Op panic) and never reaches the nats.go
// delivery goroutine. When no broker action happened yet the message is handed
// back with NakWithDelay on the BackOff schedule, so it is redelivered without
// waiting for AckWait and a poison message cannot spin. A message that Settle
// already dead-lettered or reported as terminal is instead ended with
// TermWithReason on a non-final delivery, so it never runs again and is never
// dead-lettered twice. On the final delivery a message not yet dead-lettered or
// reported is instead reported to Observer.Failure (outcome dropped, reason
// panic, nil event, *event.PanicError) and ended with TermWithReason, because
// the server would not redeliver it. The recover never dead-letters and never
// calls the reason-less Term. Its broker action happens while the message is
// still counted in flight, so Shutdown does not return before it.
func (b *Bus) msgHandler(
	ctx context.Context,
	handlerName string,
	fn event.HandleFn,
	policy deliveryPolicy,
) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		// acted is set immediately before every broker action, so the recover
		// below never acts on a message twice. guard records whether Settle got
		// past the point of no return. Track comes first and the recover is
		// registered after the deferred release, so the recover runs first on
		// unwinding: its single broker action goes out while the message is still
		// counted in flight and Shutdown cannot return before it (G-09-2).
		acted := false
		guard := &settleGuard{}
		release, ok := b.life.Track()
		if ok {
			defer release()
		}
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			stack := debug.Stack()
			pe := &event.PanicError{Value: r, Stack: stack}
			b.internalError(ctx, event.OpPanic, handlerName, msg.Subject(), pe)
			if acted {
				return
			}
			acted = true
			d := b.delivery(ctx, msg, policy)
			var aerr error
			op := event.OpNak
			switch {
			case d.Final() && !guard.ended:
				// The server will not redeliver: report, then end the message.
				b.reportPanicOnFinal(ctx, handlerName, d, pe)
				op = event.OpTerm
				aerr = msg.TermWithReason(termReasonPanicOnFinal)
			case guard.ended && !d.Final():
				op = event.OpTerm
				aerr = msg.TermWithReason(termReasonPanicAfterSettle)
			default:
				// A non-final delivery with no settlement, or the final
				// delivery after settlement.
				aerr = msg.NakWithDelay(b.delayFor(pe, d, policy.backOff))
			}
			if aerr != nil {
				b.internalError(ctx, op, handlerName, d.Subject, aerr)
			}
		}()

		if !ok || b.life.Closing() {
			acted = true
			b.handBack(ctx, msg, handlerName, policy)
			return
		}

		// hctx keeps the Subscribe ctx values and cancellation and is also
		// cancelled, with cause ErrShutdownInterrupted, when Shutdown gives up.
		// It is the SubscriptionCtx of every Settle call, so a deadline
		// cancellation is an interruption (Nak), never a dead-letter. The bus
		// Observer and the delivery's FailureScope are installed before Decode, so
		// Decode, the handler and Settle all see them.
		hctx, stop := b.life.HandlerContext(ctx, false)
		defer stop()
		hctx = b.msgScope(hctx, guard)

		d := b.delivery(hctx, msg, policy)

		msgCtx, evt, err := b.registry.Decode(hctx, msg.Data())
		if err != nil {
			// A decode failure has no event and no handler run. Settle
			// dead-letters the raw bytes (permanent failures) or retries until
			// the final delivery (ErrUnknownVersion), never a silent Term.
			err = event.WrapDecodeError(err)
			s := event.Settle(event.WithDelivery(hctx, d), event.SettleInput{
				SubscriptionCtx: hctx,
				Evt:             nil,
				Handler:         handlerName,
				Err:             err,
				Publisher:       guard.publisher(b.dlq),
			})
			acted = true
			b.apply(hctx, msg, handlerName, s, err, d, policy)
			return
		}

		msgCtx = event.WithDelivery(msgCtx, d)
		msgCtx = event.WithExecution(msgCtx, event.Execution{Handler: handlerName, StartedAt: time.Now()})

		err = event.InvokeHandler(msgCtx, b.invoker, evt, handlerName, func(c context.Context) error {
			return fn(c, evt)
		})
		err = event.WrapInterrupted(hctx, err)

		s := event.Settle(msgCtx, event.SettleInput{
			SubscriptionCtx: hctx,
			Evt:             evt,
			Handler:         handlerName,
			Err:             err,
			Publisher:       guard.publisher(b.dlq),
		})
		acted = true
		b.apply(msgCtx, msg, handlerName, s, err, d, policy)
	}
}

// termReasonPanicAfterSettle is the TermWithReason text of a message whose
// callback panicked after Settle dead-lettered or reported it.
const termReasonPanicAfterSettle = "panic: recovered after settlement"

// termReasonPanicOnFinal is the TermWithReason text of a message whose callback
// panicked outside the handler on its final delivery, before Settle
// dead-lettered or reported it. The server would not redeliver it, so the bus
// reports it to Observer.Failure and ends it.
const termReasonPanicOnFinal = "panic: recovered on the final delivery before settlement"

// settleGuard records, for one message, whether Settle got past the point of no
// return: the bus DLQ publish returned nil, or Settle began reporting the
// terminal outcome. Only two calls set ended: the Publish of the wrapper from
// publisher and the Failure of the guardObserver that msgScope installs. Settle
// calls Observer.Failure only from its terminal report, after any dead-letter
// publish and before the event's own OnErr method, so every reporting callback
// runs with ended already true, and so does the chain-dead-lettered path.
//
// msgHandler's recover reads ended in two places: the non-final Term after
// settlement, and the final-delivery report-and-Term before settlement. A
// message that was dead-lettered or reported must not be redelivered, since that
// would rerun the handler, write a second DLQ record and report the outcome
// again. It is used on the delivery goroutine only, so it needs no lock.
type settleGuard struct {
	ended bool
}

// publisher wraps p so a successful Publish sets ended. A nil p stays nil:
// Settle's dropped branch keys on a nil Publisher.
func (g *settleGuard) publisher(p event.DLQPublisher) event.DLQPublisher {
	if p == nil {
		return nil
	}
	return guardedPublisher{p: p, g: g}
}

type guardedPublisher struct {
	p event.DLQPublisher
	g *settleGuard
}

func (w guardedPublisher) Publish(ctx context.Context, rec event.DLQRecord) error {
	err := w.p.Publish(ctx, rec)
	if err == nil {
		w.g.ended = true
	}
	return err
}

// guardObserver forwards every signal to the bus Observer and sets ended before
// it forwards a Failure, so the flag is true even when the observer panics.
type guardObserver struct {
	event.Observer
	g *settleGuard
}

func (o guardObserver) Failure(ctx context.Context, f event.FailureContext) {
	o.g.ended = true
	o.Observer.Failure(ctx, f)
}

// reportPanicOnFinal reports a panic outside the handler to Observer.Failure:
// outcome dropped (the message is ended without a dead-letter record), reason
// panic, no event, the *event.PanicError as cause. It is called only from the
// msgHandler recover, on the final delivery, when Settle did not dead-letter or
// report the message, so the Failure fires at most once. The call has its own
// recover: a panicking observer (a bus built as a struct literal, as the
// production observer is wrapped in event.MultiObserver) cannot escape the
// msgHandler recover or skip the TermWithReason that follows.
func (b *Bus) reportPanicOnFinal(ctx context.Context, handlerName string, d event.Delivery, pe *event.PanicError) {
	defer func() {
		if value := recover(); value != nil {
			func() {
				defer func() { _ = recover() }()
				b.internalError(ctx, event.OpPanic, handlerName, d.Subject,
					&event.PanicError{Value: value, Stack: debug.Stack()})
			}()
		}
	}()
	d.Raw = nil
	fctx := event.WithDelivery(ctx, d)
	b.obs().Failure(fctx, event.FailureContext{
		Handler:  handlerName,
		Outcome:  event.OutcomeDropped,
		Reason:   event.ReasonPanic,
		Cause:    pe,
		Invoker:  event.FailingInvokerOf(pe),
		Delivery: d,
		At:       time.Now().UTC(),
	})
}

// handBack gives an unstarted message back to the server with Nak while the bus
// is closing or interrupted: the handler does not run, the message is
// redelivered to another consumer and is never dead-lettered (Phase 8 D-07).
//
// Each hand-back consumes one delivery, the accepted D-27 cost. On the final
// delivery the server will not redeliver, so the hand-back is reported once as
// interrupted: Settle sees a cancelled subscription context, never publishes
// and reports to Observer.Failure with a nil event. The Nak has already happened
// by then.
func (b *Bus) handBack(ctx context.Context, msg jetstream.Msg, handlerName string, policy deliveryPolicy) {
	hctx := b.msgScope(ctx, nil)
	d := b.delivery(hctx, msg, policy)
	if err := msg.Nak(); err != nil {
		b.internalError(hctx, event.OpNak, handlerName, d.Subject, err)
	}
	if !d.Final() {
		return
	}
	cctx, cancel := context.WithCancelCause(hctx)
	cancel(event.ErrShutdownInterrupted)
	event.Settle(event.WithDelivery(hctx, d), event.SettleInput{
		SubscriptionCtx: cctx,
		Evt:             nil,
		Handler:         handlerName,
		Err:             fmt.Errorf("%w: %w", event.ErrShutdownInterrupted, context.Canceled),
		Publisher:       b.dlq,
	})
}

// apply performs the one broker action for s. A failing ack call is reported as
// an InternalError signal (Op ack, nak or term; NakWithDelay counts as nak) and
// otherwise ignored: the server redelivers an unacknowledged message, which can
// duplicate a DLQ record (at-least-once, D-13).
func (b *Bus) apply(ctx context.Context, msg jetstream.Msg, handlerName string, s event.Settlement, err error, d event.Delivery, policy deliveryPolicy) {
	var aerr error
	var op string
	switch s.Disposition {
	case event.DispositionAck:
		op = event.OpAck
		aerr = msg.Ack()
	case event.DispositionRetry:
		// Only the interruption path: the subscription context ended.
		op = event.OpNak
		aerr = msg.Nak()
	case event.DispositionRetryDelayed:
		errForDelay := s.Err
		if errForDelay == nil {
			errForDelay = err
		}
		op = event.OpNak
		aerr = msg.NakWithDelay(b.delayFor(errForDelay, d, policy.backOff))
	case event.DispositionTerminal:
		op = event.OpTerm
		aerr = msg.TermWithReason(string(s.Outcome) + ": " + s.Reason)
	}
	if aerr != nil {
		b.internalError(ctx, op, handlerName, d.Subject, aerr)
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
