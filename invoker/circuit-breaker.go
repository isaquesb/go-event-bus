package invoker

import (
	"context"
	"errors"
	"fmt"
	"github.com/isaquesb/go-event-bus"
	"sync"
	"sync/atomic"
	"time"
)

type circuitState int

const (
	stateClosed circuitState = iota
	stateOpen
	stateHalfOpen
)

// String returns closed, open or half_open.
func (s circuitState) String() string {
	switch s {
	case stateClosed:
		return "closed"
	case stateOpen:
		return "open"
	case stateHalfOpen:
		return "half_open"
	}
	return "unknown"
}

// Transition reasons. They are the Phase 10 vocabulary for state changes.
const (
	reasonFailureThreshold   = "failure_threshold"
	reasonOpenTimeoutElapsed = "open_timeout_elapsed"
	reasonProbeSucceeded     = "probe_succeeded"
	reasonProbeFailed        = "probe_failed"
)

// ErrCircuitOpen is the root sentinel event.ErrCircuitOpen, kept here for
// source compatibility.
var ErrCircuitOpen = event.ErrCircuitOpen

// token is what admit hands to record. gen is the breaker generation at
// admission: every state transition (and every probe lease supersede)
// increments the breaker generation, so a completion carrying an older gen is
// stale and is ignored. probe marks the single half-open probe.
type token struct {
	gen   uint64
	probe bool
}

// outcome is how a finished call counts toward the breaker.
type outcome int

const (
	outcomeSuccess outcome = iota
	outcomeFailure
	// outcomeNeutral neither opens, closes nor resets anything. In half-open
	// it releases the probe slot.
	outcomeNeutral
)

// transition describes one state change. It is built under the breaker lock
// and consumed after the lock is released.
type transition struct {
	from, to circuitState
	reason   string
	failures int
	at       time.Time
	seq      uint64
}

type handlerBreaker struct {
	state     circuitState
	failures  int
	successes int
	openedAt  time.Time

	// gen is incremented on every state transition and on a probe lease
	// supersede; seq only on state transitions.
	gen uint64
	seq uint64
	// probing is true while the single half-open probe is in flight; probeAt
	// is when it was admitted (the lease start).
	probing bool
	probeAt time.Time

	mu sync.Mutex
}

// move changes the state and describes the change. The caller holds b.mu.
func (b *handlerBreaker) move(to circuitState, reason string, now time.Time) *transition {
	tr := &transition{
		from:     b.state,
		to:       to,
		reason:   reason,
		failures: b.failures,
		at:       now,
	}
	b.state = to
	b.gen++
	b.seq++
	tr.seq = b.seq
	return tr
}

type CircuitBreakerConfig struct {
	FailureThreshold int
	SuccessThreshold int
	OpenTimeout      time.Duration

	// OnStateChange, when set, is called for every state transition of every
	// handler breaker. It runs after the breaker lock is released, on the
	// goroutine of the message that caused the transition, with that message's
	// context, and after the Observer carried by the context received the same
	// event.BreakerTransition. It must not block: dispatch elsewhere when the
	// work is slow. It may call State and Invoke on the same breaker. Calls from
	// different goroutines may overlap; Seq, which increases by one per
	// transition of a handler breaker, gives the order. A panic in the callback
	// is recovered and reported as an InternalError signal; it never fails the
	// message.
	OnStateChange func(ctx context.Context, t event.BreakerTransition)
}

// breakerIDs hands out the identity of each CircuitBreaker. The first id is 1,
// so zero keeps meaning "unknown" in event.BreakerTransition.BreakerID.
var breakerIDs atomic.Uint64

func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		id:       breakerIDs.Add(1),
		cfg:      cfg,
		breakers: make(map[string]*handlerBreaker),
	}
}

// CircuitBreaker is a per-handler circuit breaker.
//
// Counted failures open it after FailureThreshold of them: unclassified and
// retryable errors, errors with Retryable() false that are not terminal, and
// handler timeouts (a context error while the context cause is
// event.ErrHandlerTimeout). Neutral outcomes never open, close or reset it:
// terminal errors (PermanentError, ErrSendToDLQ, a panic), context
// cancellation, a bare context.DeadlineExceeded on a live context, and the
// flow-control errors ErrDuplicate, ErrDeadLettered, ErrInProgress,
// ErrRateLimited, ErrCircuitOpen and ErrDeadLetterFailed. The rule is the same
// in every state and the classifier is not configurable.
//
// After OpenTimeout the breaker turns half-open and admits exactly one probe
// at a time; concurrent callers get ErrCircuitOpen. A successful probe frees
// the slot for the next caller until SuccessThreshold successes close the
// breaker. A counted probe failure reopens it; a neutral probe outcome keeps
// it half-open and frees the slot. The probe holds a lease of OpenTimeout: a
// probe still running after that is superseded by the next arrival and its
// late completion is ignored. A zero OpenTimeout never expires the lease.
type CircuitBreaker struct {
	// id is process-unique and never zero. It is copied into every
	// event.BreakerTransition this breaker reports, so an Observer can tell
	// two breakers that serve the same handler name apart and order Seq per
	// breaker.
	id  uint64
	cfg CircuitBreakerConfig

	mu       sync.Mutex
	breakers map[string]*handlerBreaker
}

func (c *CircuitBreaker) Invoke(
	ctx context.Context,
	_ event.Event,
	handler string,
	next func(context.Context) error,
) error {
	b := c.breaker(handler)

	tok, tr, state, ok := c.admit(b)
	c.report(ctx, handler, tr, state)
	if !ok {
		return ErrCircuitOpen
	}

	// If next panics or exits through runtime.Goexit, release the probe slot
	// with a neutral outcome; the panic keeps propagating.
	done := false
	defer func() {
		if !done {
			tr, state := c.record(b, tok, outcomeNeutral)
			c.report(ctx, handler, tr, state)
		}
	}()

	err := next(ctx)
	done = true

	tr, state = c.record(b, tok, c.classify(ctx, err))
	c.report(ctx, handler, tr, state)
	return err
}

// admit decides under the breaker lock whether a call may run. The lock is
// never held while next runs. It also returns the breaker state after the
// decision, captured under the lock.
func (c *CircuitBreaker) admit(b *handlerBreaker) (token, *transition, circuitState, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	switch b.state {
	case stateOpen:
		if now.Sub(b.openedAt) < c.cfg.OpenTimeout {
			return token{}, nil, b.state, false
		}
		b.successes = 0
		tr := b.move(stateHalfOpen, reasonOpenTimeoutElapsed, now)
		b.probing = true
		b.probeAt = now
		return token{gen: b.gen, probe: true}, tr, b.state, true

	case stateHalfOpen:
		if b.probing {
			// Lease (D-28): a probe that outlived OpenTimeout is superseded.
			// A zero OpenTimeout never expires the lease.
			if c.cfg.OpenTimeout > 0 && now.Sub(b.probeAt) >= c.cfg.OpenTimeout {
				b.gen++
				b.probeAt = now
				return token{gen: b.gen, probe: true}, nil, b.state, true
			}
			return token{}, nil, b.state, false
		}
		b.probing = true
		b.probeAt = now
		return token{gen: b.gen, probe: true}, nil, b.state, true
	}

	return token{gen: b.gen}, nil, b.state, true
}

// record applies the outcome of a finished call under the breaker lock. A
// completion from an older generation is ignored and never touches the probe
// slot. It also returns the breaker state after the call, captured under the
// lock.
func (c *CircuitBreaker) record(b *handlerBreaker, tok token, out outcome) (*transition, circuitState) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if tok.gen != b.gen {
		return nil, b.state
	}

	now := time.Now()
	switch b.state {
	case stateClosed:
		switch out {
		case outcomeFailure:
			b.failures++
			if b.failures >= c.cfg.FailureThreshold {
				b.openedAt = now
				return b.move(stateOpen, reasonFailureThreshold, now), b.state
			}
		case outcomeSuccess:
			b.failures = 0
		}

	case stateHalfOpen:
		if !tok.probe {
			return nil, b.state
		}
		b.probing = false
		switch out {
		case outcomeFailure:
			b.openedAt = now
			return b.move(stateOpen, reasonProbeFailed, now), b.state
		case outcomeSuccess:
			b.successes++
			if b.successes >= c.cfg.SuccessThreshold {
				b.failures = 0
				b.successes = 0
				return b.move(stateClosed, reasonProbeSucceeded, now), b.state
			}
		}
	}

	return nil, b.state
}

// classify decides how the result of next counts toward the breaker (D-12).
// The rule is the same in the closed and half-open states, evaluated in order:
//
//   - nil is a success.
//   - A panic (event.ErrHandlerPanic) is neutral, even when the panicked value
//     was a context error.
//   - A handler timeout is counted: an error matching event.ErrHandlerTimeout,
//     or a context error while the context cause is event.ErrHandlerTimeout.
//   - Any other context.Canceled or context.DeadlineExceeded is neutral. A bare
//     DeadlineExceeded on a live context (a downstream client timeout) is
//     therefore neutral too: only a transport-set handler timeout counts.
//   - Otherwise event.Dispose with a non-final Delivery decides: Ack and
//     Terminal are neutral; a delayed retry is counted, except for the
//     flow-control signals ErrCircuitOpen, ErrRateLimited, ErrInProgress,
//     ErrDeadLetterFailed and ErrUnknownVersion, which are neutral.
func (c *CircuitBreaker) classify(ctx context.Context, err error) outcome {
	if err == nil {
		return outcomeSuccess
	}
	if errors.Is(err, event.ErrHandlerPanic) {
		return outcomeNeutral
	}

	ctxErr := errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
	if errors.Is(err, event.ErrHandlerTimeout) ||
		(ctxErr && errors.Is(context.Cause(ctx), event.ErrHandlerTimeout)) {
		return outcomeFailure
	}
	if ctxErr {
		return outcomeNeutral
	}

	switch event.Dispose(err, event.Delivery{}) {
	case event.DispositionAck, event.DispositionTerminal:
		return outcomeNeutral
	case event.DispositionRetryDelayed:
		if errors.Is(err, ErrCircuitOpen) ||
			errors.Is(err, event.ErrRateLimited) ||
			errors.Is(err, event.ErrInProgress) ||
			errors.Is(err, event.ErrDeadLetterFailed) ||
			errors.Is(err, event.ErrUnknownVersion) {
			return outcomeNeutral
		}
		return outcomeFailure
	}
	return outcomeFailure
}

// State returns the current state of the breaker for handler: closed for a
// handler that never ran. It never creates a breaker and is safe to call from
// OnStateChange.
func (c *CircuitBreaker) State(handler string) event.BreakerState {
	c.mu.Lock()
	b, ok := c.breakers[handler]
	c.mu.Unlock()
	if !ok {
		return event.BreakerClosed
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	return breakerState(b.state)
}

func breakerState(s circuitState) event.BreakerState {
	return event.BreakerState(s.String())
}

// report publishes what a call learned about the breaker once the lock is
// released: the state into the FailureScope, then the transition, if any, to
// the Observer and OnStateChange.
func (c *CircuitBreaker) report(ctx context.Context, handler string, tr *transition, state circuitState) {
	if scope, ok := event.FailureScopeFromContext(ctx); ok {
		scope.SetBreaker(breakerState(state))
	}
	c.emitTransition(ctx, handler, tr)
}

// emitTransition reports a state change after the lock was released. It does
// nothing for nil. A panic in the Observer or in OnStateChange is recovered
// and reported as an InternalError; it never fails the message.
func (c *CircuitBreaker) emitTransition(ctx context.Context, handler string, tr *transition) {
	if tr == nil {
		return
	}

	t := event.BreakerTransition{
		Handler:   handler,
		From:      breakerState(tr.from),
		To:        breakerState(tr.to),
		Reason:    event.BreakerReason(tr.reason),
		Failures:  tr.failures,
		At:        tr.at,
		Seq:       tr.seq,
		BreakerID: c.id,
	}

	c.guarded(ctx, handler, func() {
		event.ObserverFromContext(ctx).BreakerTransition(ctx, t)
	})
	if c.cfg.OnStateChange != nil {
		c.guarded(ctx, handler, func() {
			c.cfg.OnStateChange(ctx, t)
		})
	}
}

// guarded runs fn and turns a panic into an InternalError signal.
func (c *CircuitBreaker) guarded(ctx context.Context, handler string, fn func()) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		// The observer reporting the panic may panic as well; nothing may
		// escape into the message path.
		defer func() { _ = recover() }()
		event.ObserverFromContext(ctx).InternalError(ctx, event.InternalErrorInfo{
			Component: event.ComponentCircuitBreaker,
			Op:        event.OpPanic,
			Handler:   handler,
			Err:       fmt.Errorf("circuit breaker callback panicked: %v", r),
		})
	}()
	fn()
}

func (c *CircuitBreaker) breaker(handler string) *handlerBreaker {
	c.mu.Lock()
	defer c.mu.Unlock()

	if b, ok := c.breakers[handler]; ok {
		return b
	}

	b := &handlerBreaker{state: stateClosed}
	c.breakers[handler] = b

	return b
}
