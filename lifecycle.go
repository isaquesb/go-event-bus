package event

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrBusClosed is returned or reported for new work after Shutdown or Close
	// began. Work that was already accepted is not affected: it drains.
	ErrBusClosed = errors.New("event: bus closed")

	// ErrShutdownInterrupted is the cause of the bus context cancellation when
	// the Shutdown context expired before the in-flight work drained. Handlers
	// cancelled by it are reported, never silently lost: use WrapInterrupted to
	// make a handler error match both this sentinel and the context error.
	ErrShutdownInterrupted = errors.New("event: shutdown interrupted")
)

// DefaultShutdownTimeout is the bound Close applies to the drain when the bus
// has no ShutdownTimeout option set. Each bus exposes a ShutdownTimeout option
// to override it.
const DefaultShutdownTimeout = 30 * time.Second

// Shutdowner is the optional interface of a Bus that can shut down gracefully.
// It is implemented by LocalBus, nats.Bus and jetstream.Bus.
//
// Shutdown stops accepting new work, drains the in-flight handlers bounded by
// ctx, cancels them when ctx expires first and returns ctx.Err(). Application
// code type-asserts a Bus to Shutdowner without knowing the transport. Close
// calls the same logic with DefaultShutdownTimeout, or the bus option.
type Shutdowner interface {
	Shutdown(ctx context.Context) error
}

type lifecycleKey struct{}

// Lifecycle is the shutdown state machine shared by every bus: admission of new
// work, in-flight accounting, a bounded drain, a forced cancel and the handler
// context join. LocalBus, the NATS Core bus and the JetStream bus embed one so
// their shutdown semantics cannot diverge.
//
// The zero value is ready to use. A Lifecycle must not be copied after first
// use: embed it by value in a bus that is always used through a pointer.
//
// In-flight accounting is a counter guarded by a mutex plus an idle channel,
// deliberately not a wait group. A handler that emits a child event while the
// bus is draining is an increment while a waiter is waiting, which is exactly
// what a wait group forbids; here the check and the increment happen under one
// mutex, a nested child is admitted only while its parent is still counted, the
// waiter blocks on a channel, and release is idempotent and meant to run from a
// defer so runtime.Goexit cannot leak a count.
type Lifecycle struct {
	mu         sync.Mutex
	closed     bool
	forced     bool
	done       bool
	inflight   int
	idle       chan struct{}
	idleClosed bool
	base       context.Context
	cancel     context.CancelCauseFunc
}

var _ Shutdowner = (*Lifecycle)(nil)

// init creates the bus context lazily. It must be called with l.mu held.
func (l *Lifecycle) init() {
	if l.base == nil {
		l.base, l.cancel = context.WithCancelCause(context.Background())
	}
}

// Context returns the bus context. It is cancelled, with cause
// ErrShutdownInterrupted, when Interrupt runs.
func (l *Lifecycle) Context() context.Context {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.init()
	return l.base
}

// releaser builds the idempotent release func of one counted unit of work. It
// never marks the lifecycle done: only Wait does, so messages delivered during
// a graceful drain are still tracked.
func (l *Lifecycle) releaser() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			l.inflight--
			if l.closed && l.inflight == 0 && l.idle != nil && !l.idleClosed {
				close(l.idle)
				l.idleClosed = true
			}
			l.mu.Unlock()
		})
	}
}

// Admit admits new work, such as an emit. It returns ErrBusClosed once shutdown
// began, except for a nested admission: a ctx carrying this lifecycle's marker
// (see HandlerContext) is admitted while the in-flight count is above zero, so
// a handler can emit during the drain. After Interrupt every admission is
// rejected, which stops an unbounded causation chain at the deadline.
//
// The caller must call release exactly when the work ends, normally from a
// defer. release is idempotent.
func (l *Lifecycle) Admit(ctx context.Context) (release func(), err error) {
	nested := l.Nested(ctx)
	l.mu.Lock()
	l.init()
	if l.forced || l.done {
		l.mu.Unlock()
		return nil, ErrBusClosed
	}
	if l.closed && !(nested && l.inflight > 0) {
		l.mu.Unlock()
		return nil, ErrBusClosed
	}
	l.inflight++
	l.mu.Unlock()
	return l.releaser(), nil
}

// Track counts work that was already accepted, such as an inbound broker
// message or a task of an admitted emit. It is allowed during a graceful drain
// and refused (ok false) after Interrupt and after Wait observed the idle
// state, so nothing runs after close.
func (l *Lifecycle) Track() (release func(), ok bool) {
	l.mu.Lock()
	l.init()
	if l.forced || l.done {
		l.mu.Unlock()
		return nil, false
	}
	l.inflight++
	l.mu.Unlock()
	return l.releaser(), true
}

// Nested reports whether ctx carries this lifecycle's marker, that is whether
// it descends from a context made by HandlerContext.
func (l *Lifecycle) Nested(ctx context.Context) bool {
	v, _ := ctx.Value(lifecycleKey{}).(*Lifecycle)
	return v == l
}

// HandlerContext derives the context a handler runs with. It keeps every value
// of parent (Metadata, trace). With detach true it drops the cancellation and
// the deadline of parent, so an async handler survives its emitter; with detach
// false it keeps both. In both cases the result is cancelled by Interrupt with
// cause ErrShutdownInterrupted and carries the marker Nested detects.
//
// The returned func releases the resources of the derived context and must be
// called when the handler returns.
func (l *Lifecycle) HandlerContext(parent context.Context, detach bool) (context.Context, context.CancelFunc) {
	if detach {
		parent = context.WithoutCancel(parent)
	}
	c, cancel := context.WithCancelCause(parent)
	bus := l.Context()
	stop := context.AfterFunc(bus, func() { cancel(context.Cause(bus)) })
	c = context.WithValue(c, lifecycleKey{}, l)
	return c, func() {
		stop()
		cancel(nil)
	}
}

// BeginShutdown stops admitting new work. It is idempotent.
func (l *Lifecycle) BeginShutdown() {
	l.mu.Lock()
	l.init()
	l.closed = true
	l.mu.Unlock()
}

// Wait blocks until the in-flight count reaches zero and returns nil, after
// which Track is refused. When ctx expires first it calls Interrupt and returns
// ctx.Err() without waiting further. Call it after BeginShutdown; Shutdown does
// both.
func (l *Lifecycle) Wait(ctx context.Context) error {
	for {
		l.mu.Lock()
		l.init()
		if l.inflight == 0 {
			l.done = true
			l.mu.Unlock()
			return nil
		}
		if l.idle == nil || l.idleClosed {
			l.idle = make(chan struct{})
			l.idleClosed = false
		}
		idle := l.idle
		l.mu.Unlock()

		select {
		case <-idle:
			// A Track may have run in between: loop and re-check.
		case <-ctx.Done():
			l.Interrupt()
			return ctx.Err()
		}
	}
}

// Interrupt refuses all new work, admissions and tracking alike, and cancels
// the bus context with cause ErrShutdownInterrupted, which cancels every
// context made by HandlerContext. It is idempotent.
func (l *Lifecycle) Interrupt() {
	l.mu.Lock()
	l.init()
	l.forced = true
	l.closed = true
	cancel := l.cancel
	l.mu.Unlock()
	cancel(ErrShutdownInterrupted)
}

// Shutdown is BeginShutdown followed by Wait(ctx). It is safe to call more than
// once and concurrently.
func (l *Lifecycle) Shutdown(ctx context.Context) error {
	l.BeginShutdown()
	return l.Wait(ctx)
}

// Closing reports whether shutdown began or the lifecycle was interrupted.
func (l *Lifecycle) Closing() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed || l.forced
}

// WrapInterrupted reports a handler error caused by a shutdown. When err is a
// context error (context.Canceled or context.DeadlineExceeded) and ctx was
// cancelled by Interrupt, it returns an error that matches both
// ErrShutdownInterrupted and err with errors.Is. Any other err, or an err that
// already matches ErrShutdownInterrupted, is returned unchanged.
func WrapInterrupted(ctx context.Context, err error) error {
	if err == nil || errors.Is(err, ErrShutdownInterrupted) {
		return err
	}
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if !errors.Is(context.Cause(ctx), ErrShutdownInterrupted) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrShutdownInterrupted, err)
}
