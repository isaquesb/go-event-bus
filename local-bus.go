package event

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"sync"
	"time"
)

const DefaultMaxConcurrent = 16

// DefaultQueueSize is the number of tasks LocalBusOptions.QueueSize parks when
// it is not set.
const DefaultQueueSize = 1024

// ErrQueueFull is reported, never returned, when an external emit finds no free
// execution slot and no queue room (OverflowDrop), or when the emitter context
// expired while it waited for queue room (OverflowBlock). The event is dropped
// for that handler and the drop is reported once through the bus OnErr, the
// event's own OnErr and an Error-level slog record.
var ErrQueueFull = errors.New("event: queue full")

// OverflowPolicy selects what LocalBus does with an external emit when every
// execution slot is busy and the queue is full. An emit made from inside a
// handler of the same bus is never subject to it: it runs inline.
type OverflowPolicy int

const (
	// OverflowBlock is the default. The emit parks up to QueueSize tasks and,
	// beyond that, waits for queue room for as long as the emitter ctx is alive.
	// When the emitter ctx ends first the event is dropped with ErrQueueFull.
	// Under sustained overload the emitter pays, with its latency.
	OverflowBlock OverflowPolicy = iota
	// OverflowUnbounded never blocks and never drops: every task is parked,
	// whatever the count. Under sustained overload memory pays, and parked
	// tasks are lost if the process dies.
	OverflowUnbounded
	// OverflowDrop reports ErrQueueFull immediately when the queue is full.
	// Under sustained overload the event pays.
	OverflowDrop
)

// String returns block, unbounded, drop or unknown.
func (p OverflowPolicy) String() string {
	switch p {
	case OverflowBlock:
		return "block"
	case OverflowUnbounded:
		return "unbounded"
	case OverflowDrop:
		return "drop"
	}
	return "unknown"
}

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
	Invoker Invoker
	OnErr   func(ctx context.Context, evt Event, err error, handler string)

	// MaxConcurrent is the number of async handlers that run at once. Values at
	// or below 0 mean DefaultMaxConcurrent.
	//
	// Throughput is MaxConcurrent divided by handler latency. The queue only
	// absorbs bursts; under sustained overload Overflow decides who pays
	// (OverflowBlock the emitter's latency, OverflowUnbounded memory and
	// durability, OverflowDrop the event). An emit made from inside a handler
	// of this bus never waits and runs inline when no slot is free, provided it
	// passes the handler ctx; the inline depth equals the self-emit chain depth.
	MaxConcurrent int

	// QueueSize is the number of tasks parked while they wait for an execution
	// slot. Values at or below 0 mean DefaultQueueSize. It is ignored by
	// OverflowUnbounded. See MaxConcurrent for what the queue does and does not
	// do for throughput.
	QueueSize int

	// Overflow selects the policy for an external emit that finds no free slot
	// and no queue room. The zero value is OverflowBlock. Tasks have no
	// ordering guarantee under any policy. See MaxConcurrent for the cost of
	// each policy.
	Overflow OverflowPolicy

	// ShutdownTimeout is the bound Close applies to Shutdown. Values at or
	// below 0 mean DefaultShutdownTimeout.
	ShutdownTimeout time.Duration

	// HandlerTimeout bounds each async handler execution, including an inline
	// nested one. When above 0 the handler ctx carries a deadline whose cause is
	// ErrHandlerTimeout, placed above the invoker chain so Retry and the circuit
	// breaker see it. Expiry is a transient failure like any other: because
	// LocalBus deliveries are final it ends dead-lettered (or dropped without a
	// DLQPublisher), never as an interruption, and the circuit breaker counts it
	// through the ctx cause. The reported error matches ErrHandlerTimeout and
	// context.DeadlineExceeded. EmitSync handlers keep the caller's ctx and are
	// never bounded by it. A handler must honor its ctx for the timeout to take
	// effect: the bus cannot stop a goroutine. The dead-letter publish of a
	// timed-out execution runs on a ctx detached from the timeout that keeps the
	// handler ctx values and is cancelled only by the Shutdown deadline. 0 means
	// no timeout.
	HandlerTimeout time.Duration

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
	if opts.QueueSize <= 0 {
		opts.QueueSize = DefaultQueueSize
	}
	if opts.ShutdownTimeout <= 0 {
		opts.ShutdownTimeout = DefaultShutdownTimeout
	}
	if opts.Invoker == nil {
		opts.Invoker = &passthroughInvoker{}
	}
	return &LocalBus{
		handlers:        make(map[string][]*namedHandle),
		onErr:           opts.OnErr,
		dlq:             opts.DLQPublisher,
		invoker:         opts.Invoker,
		exec:            make(chan struct{}, opts.MaxConcurrent),
		pending:         make(chan struct{}, opts.QueueSize),
		overflow:        opts.Overflow,
		shutdownTimeout: opts.ShutdownTimeout,
		handlerTimeout:  opts.HandlerTimeout,
		idGen:           opts.IDGenerator,
		inherit:         slices.Clone(opts.InheritHeaders),
	}
}

// LocalBus is an in-process event dispatcher. Emit runs handlers
// asynchronously, detached from the emitter ctx (see Emit); EmitSync runs them
// on the caller goroutine. Shutdown and Close drain in-flight handlers within a
// bound. There is no ordering guarantee between tasks.
type LocalBus struct {
	mu              sync.RWMutex
	handlers        map[string][]*namedHandle
	onErr           func(ctx context.Context, evt Event, err error, handler string)
	dlq             DLQPublisher
	invoker         Invoker
	exec            chan struct{}
	pending         chan struct{}
	overflow        OverflowPolicy
	shutdownTimeout time.Duration
	handlerTimeout  time.Duration
	idGen           func() string
	inherit         []string
	life            Lifecycle
}

var _ Shutdowner = (*LocalBus)(nil)

// Subscribe registers a handler for events on the given subject.
// Options can be used to configure the subscription (e.g., WithHandlerName).
// It returns ErrBusClosed once Shutdown or Close began.
func (b *LocalBus) Subscribe(
	ctx context.Context,
	subject string,
	handler HandleFn,
	opts ...SubscribeOption,
) error {
	if b.life.Closing() {
		return ErrBusClosed
	}

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

// Shutdown stops accepting emits and waits for the in-flight handlers and the
// tasks parked for a slot, bounded by ctx.
//
// A handler of this bus may keep emitting while the bus drains (a nested emit,
// made with the handler ctx); those children are drained too, which ends when
// the causal chain ends or ctx expires. When ctx expires first, every handler
// ctx is cancelled with cause ErrShutdownInterrupted, parked tasks are handed
// back as interrupted, new emits are refused, and Shutdown returns ctx.Err()
// without waiting further. Interrupted tasks are reported once through the
// single report sink and are never dead-lettered. Shutdown is safe to call more
// than once and concurrently.
func (b *LocalBus) Shutdown(ctx context.Context) error {
	return b.life.Shutdown(ctx)
}

// Close shuts the bus down, bounded by LocalBusOptions.ShutdownTimeout (default
// DefaultShutdownTimeout). It calls Shutdown and returns its error.
func (b *LocalBus) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), b.shutdownTimeout)
	defer cancel()
	return b.Shutdown(ctx)
}

// Emit dispatches e to every handler subscribed to its subject and returns
// without waiting for them. It has no error return: a drop or a rejection is
// reported through the bus OnErr, the event's OnErr and slog, never lost.
//
// Async handlers run under a handler ctx that:
//
//   - keeps the values of the emitter ctx (Metadata, trace span, baggage);
//   - is not cancelled when the emitter ctx is done, and carries none of its
//     deadline, so a handler outlives the request that emitted it;
//   - is cancelled when the Shutdown deadline expires, with cause
//     ErrShutdownInterrupted.
//
// The OTel span of the emitter may already have ended when the handler runs;
// child spans still parent to it. The emitter ctx only bounds how long Emit
// waits for queue room under OverflowBlock. An emit made with a handler ctx of
// this bus never waits for a slot: it runs inline when none is free. Tasks have
// no ordering guarantee. EmitSync handlers keep the caller's ctx instead. After
// Shutdown the event is dropped and reported with ErrBusClosed.
func (b *LocalBus) Emit(ctx context.Context, e Event, opts ...EmitOption) {
	cfg := ApplyEmitOptions(e, opts...)
	cfg.IDGenerator = b.idGen
	cfg.InheritHeaders = b.inherit
	md := ResolveOutbound(ctx, e, cfg)
	ctx = WithMetadata(ctx, md)

	hList := b.snapshot(cfg.Subject)
	if len(hList) == 0 {
		return
	}

	release, err := b.life.Admit(ctx)
	if err != nil {
		b.guardReport(ctx, e, "", func() { b.reject(ctx, cfg.Subject, e, err) })
		return
	}
	defer release()

	for _, h := range hList {
		b.dispatchAsync(ctx, cfg.Subject, e, h)
	}
}

// EmitSync runs every handler subscribed to the event's subject, in
// registration order, on the caller goroutine and under the caller's ctx.
//
// It returns the joined terminal failures (errors.Join), each wrapped in a
// *HandlerError that names the handler: find it with errors.As, and match the
// cause with errors.Is (a recovered panic matches ErrHandlerPanic). Every
// handler runs even when an earlier one failed. A failure that was dead-lettered
// (ErrDeadLettered) and a duplicate (ErrDuplicate) are handled outcomes, not
// errors; when every handler succeeds or is handled the error is nil.
//
// EmitSync stops dispatching when the caller ctx is done and includes ctx.Err()
// in the joined error; a handler that is already running is not interrupted by
// the bus. After Shutdown it returns ErrBusClosed. LocalBusOptions.HandlerTimeout
// does not apply here: the handlers keep the caller's ctx. A failure is settled
// on a ctx detached from the caller's cancellation and deadline, so EmitSync can
// return after that deadline by the time the dead-letter publish takes, bounded
// by its in-process retry and the Shutdown deadline.
//
// Releases up to v0.3.0 returned a nil error here whatever the handlers did.
func (b *LocalBus) EmitSync(ctx context.Context, e Event, opts ...EmitOption) (Event, error) {
	cfg := ApplyEmitOptions(e, opts...)
	cfg.IDGenerator = b.idGen
	cfg.InheritHeaders = b.inherit
	md := ResolveOutbound(ctx, e, cfg)
	ctx = WithMetadata(ctx, md)

	hList := b.snapshot(cfg.Subject)
	if len(hList) == 0 {
		return e, nil
	}

	release, err := b.life.Admit(ctx)
	if err != nil {
		return e, err
	}
	defer release()

	var errs []error
	for _, h := range hList {
		if cerr := ctx.Err(); cerr != nil {
			errs = append(errs, cerr)
			break
		}
		rerr := b.execute(ctx, cfg.Subject, e, h, false)
		if rerr == nil || errors.Is(rerr, ErrDeadLettered) || errors.Is(rerr, ErrDuplicate) {
			continue
		}
		errs = append(errs, &HandlerError{Handler: h.Name, Err: rerr})
	}
	return e, errors.Join(errs...)
}

// snapshot returns the handlers of subject. The slice is append-only, so the
// returned header is safe to read after the lock is released.
func (b *LocalBus) snapshot(subject string) []*namedHandle {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.handlers[subject]
}

// dispatchAsync schedules one handler. The order is fixed: a free execution
// slot, then an inline run for a nested emit, then the overflow policy.
//
// Every report made here runs on the Emit caller goroutine through guardReport,
// and every synchronous drop branch releases its Track count from a defer, so a
// panicking or exiting reporter cannot leak the in-flight count. The parked
// goroutine owns its release through a defer as well and recovers like runTask,
// so a panicking OnErr during a shutdown interruption cannot crash the process
// or block a later Shutdown.
func (b *LocalBus) dispatchAsync(ctx context.Context, subject string, e Event, h *namedHandle) {
	release, ok := b.life.Track()
	if !ok {
		b.guardReport(ctx, e, h.Name, func() { b.interrupted(ctx, subject, e, h.Name) })
		return
	}

	select {
	case b.exec <- struct{}{}:
		go b.runTask(ctx, subject, e, h, release, true)
		return
	default:
	}

	// An emit from inside a handler of this bus never waits: that is what
	// removes the self-emit deadlock by construction.
	if b.life.Nested(ctx) {
		b.runTask(ctx, subject, e, h, release, false)
		return
	}

	parked := false
	switch b.overflow {
	case OverflowDrop:
		select {
		case b.pending <- struct{}{}:
			parked = true
		default:
			defer release()
			b.guardReport(ctx, e, h.Name, func() { b.dropQueueFull(ctx, subject, e, h.Name) })
			return
		}
	case OverflowBlock:
		// Try without waiting first, so a done emitter ctx never wins a random
		// select when there is room.
		select {
		case b.pending <- struct{}{}:
			parked = true
		default:
			select {
			case b.pending <- struct{}{}:
				parked = true
			case <-ctx.Done():
				defer release()
				b.guardReport(ctx, e, h.Name, func() { b.dropQueueFull(ctx, subject, e, h.Name) })
				return
			case <-b.life.Context().Done():
				defer release()
				b.guardReport(ctx, e, h.Name, func() { b.interrupted(ctx, subject, e, h.Name) })
				return
			}
		}
	case OverflowUnbounded:
		// No budget: every task is parked.
	}

	go func() {
		defer release()
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "event: panic in LocalBus parked task",
					"handler", h.Name,
					"event", safeEventName(e),
					"panic", fmt.Sprint(r),
					"stack", string(debug.Stack()),
				)
			}
		}()
		select {
		case b.exec <- struct{}{}:
			if parked {
				<-b.pending
			}
			if b.life.Context().Err() != nil {
				<-b.exec
				b.interrupted(ctx, subject, e, h.Name)
				return
			}
			b.runTask(ctx, subject, e, h, release, true)
		case <-b.life.Context().Done():
			if parked {
				<-b.pending
			}
			b.interrupted(ctx, subject, e, h.Name)
		}
	}()
}

// guardReport runs a report made on the Emit caller goroutine (the Admit
// rejection, the refused Track, a queue-full drop and a bus-ctx interruption)
// under a recover. The report reaches user code (the bus OnErr, the event's
// OnErr hook), and a panic there must neither unwind through Emit, which would
// skip the remaining handlers of the same emit, nor leak accounting. The panic
// is logged instead, consistent with runTask.
func (b *LocalBus) guardReport(ctx context.Context, e Event, handler string, report func()) {
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "event: panic in LocalBus report",
				"handler", handler,
				"event", safeEventName(e),
				"panic", fmt.Sprint(r),
				"stack", string(debug.Stack()),
			)
		}
	}()
	report()
}

// runTask runs one handler and releases its accounting. The recover guards the
// code InvokeHandler does not (OnErr, Settle, DLQ publishers); handler and
// invoker panics are already converted to errors by InvokeHandler.
func (b *LocalBus) runTask(
	ctx context.Context,
	subject string,
	e Event,
	h *namedHandle,
	release func(),
	slot bool,
) {
	defer release()
	if slot {
		defer func() { <-b.exec }()
	}
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "event: panic in LocalBus task",
				"handler", h.Name,
				"event", safeEventName(e),
				"panic", fmt.Sprint(r),
				"stack", string(debug.Stack()),
			)
		}
	}()
	_ = b.execute(ctx, subject, e, h, true)
}

// execute runs one handler through the invoker chain and settles a failure.
func (b *LocalBus) execute(
	ctx context.Context,
	subject string,
	e Event,
	h *namedHandle,
	async bool,
) error {
	hctx, stop := b.life.HandlerContext(ctx, async)
	defer stop()
	subCtx := hctx

	// The timeout sits above the invoker chain and below the Lifecycle ctx:
	// SubscriptionCtx stays the bus ctx, so a timeout is never mistaken for a
	// shutdown interruption. Sync handlers keep the caller's ctx.
	if async && b.handlerTimeout > 0 {
		var cancel context.CancelFunc
		hctx, cancel = context.WithTimeoutCause(hctx, b.handlerTimeout, ErrHandlerTimeout)
		defer cancel()
	}

	// LocalBus never redelivers, so every delivery is the final one.
	hctx = WithDelivery(hctx, Delivery{Count: 1, Max: 1, Subject: subject})
	hctx = WithExecution(hctx, Execution{Handler: h.Name, StartedAt: time.Now()})

	err := InvokeHandler(hctx, b.invoker, e, h.Name, func(c context.Context) error {
		return h.Fn(c, e)
	})
	if err == nil {
		return nil
	}
	if (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) &&
		errors.Is(context.Cause(hctx), ErrHandlerTimeout) {
		err = fmt.Errorf("%w: %w", ErrHandlerTimeout, err)
	}
	err = WrapInterrupted(subCtx, err)

	// Settle runs on a ctx detached from the HandlerTimeout deadline and from the
	// EmitSync caller's cancellation, so a DLQ publisher that honors its ctx can
	// persist a timed-out or late failure instead of losing it. The ctx keeps the
	// Delivery, Execution, Metadata and trace values of hctx and is still
	// cancelled at the Shutdown deadline, so a hung publisher cannot hold the
	// task past it. SubscriptionCtx stays subCtx: a timeout is never read as an
	// interruption.
	sctx, sstop := b.life.HandlerContext(hctx, true)
	defer sstop()

	// Settle is the single place that dead-letters and reports; a local bus has
	// no broker action to take on the Settlement.
	s := Settle(sctx, SettleInput{
		SubscriptionCtx: subCtx,
		Evt:             e,
		Handler:         h.Name,
		Err:             err,
		Publisher:       b.dlq,
		OnErr:           b.onErr,
	})
	if s.Err != nil {
		return s.Err
	}
	return err
}

// reject reports an emit refused before any handler ran.
func (b *LocalBus) reject(ctx context.Context, subject string, e Event, err error) {
	reportUnsettled(WithDelivery(ctx, Delivery{Count: 1, Max: 1, Subject: subject}),
		SettleInput{Evt: e, OnErr: b.onErr}, OutcomeRejected, err)
}

// dropQueueFull reports a task dropped for lack of a slot and queue room.
func (b *LocalBus) dropQueueFull(ctx context.Context, subject string, e Event, handler string) {
	reportUnsettled(WithDelivery(ctx, Delivery{Count: 1, Max: 1, Subject: subject}),
		SettleInput{Evt: e, Handler: handler, OnErr: b.onErr}, OutcomeDropped, ErrQueueFull)
}

// interrupted reports a task that never ran because the bus was shut down. When
// the bus ctx is cancelled the report goes through Settle, which sees an
// interruption and never dead-letters; otherwise the bus is merely closed.
func (b *LocalBus) interrupted(ctx context.Context, subject string, e Event, handler string) {
	bus := b.life.Context()
	if bus.Err() == nil {
		b.reject(ctx, subject, e, ErrBusClosed)
		return
	}
	_ = Settle(WithDelivery(ctx, Delivery{Count: 1, Max: 1, Subject: subject}), SettleInput{
		SubscriptionCtx: bus,
		Evt:             e,
		Handler:         handler,
		Err:             WrapInterrupted(bus, context.Canceled),
		Publisher:       b.dlq,
		OnErr:           b.onErr,
	})
}
