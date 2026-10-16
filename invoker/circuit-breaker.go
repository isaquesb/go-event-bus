package invoker

import (
	"context"
	"errors"
	"github.com/isaquesb/go-event-bus"
	"log/slog"
	"sync"
	"time"
)

/**
Expose:
circuit_state{handler=}
circuit_open_total
circuit_half_open_total
circuit_blocked_total
*/

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
}

func NewCircuitBreaker(cfg CircuitBreakerConfig, metrics MetricProvider) *CircuitBreaker {
	if metrics == nil {
		metrics = &NoopProvider{}
	}

	return &CircuitBreaker{
		cfg:      cfg,
		metrics:  metrics,
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
	cfg     CircuitBreakerConfig
	metrics MetricProvider

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

	tok, tr, ok := c.admit(b)
	c.emitTransition(ctx, handler, tr)
	if !ok {
		c.metrics.IncCounter(
			"eventbus_circuit_blocked_total",
			1,
			Labels{"handler": handler},
		)
		return ErrCircuitOpen
	}

	// If next panics or exits through runtime.Goexit, release the probe slot
	// with a neutral outcome; the panic keeps propagating.
	done := false
	defer func() {
		if !done {
			c.emitTransition(ctx, handler, c.record(b, tok, outcomeNeutral))
		}
	}()

	err := next(ctx)
	done = true

	c.emitTransition(ctx, handler, c.record(b, tok, c.classify(ctx, err)))
	return err
}

// admit decides under the breaker lock whether a call may run. The lock is
// never held while next runs.
func (c *CircuitBreaker) admit(b *handlerBreaker) (token, *transition, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	switch b.state {
	case stateOpen:
		if now.Sub(b.openedAt) < c.cfg.OpenTimeout {
			return token{}, nil, false
		}
		b.successes = 0
		tr := b.move(stateHalfOpen, reasonOpenTimeoutElapsed, now)
		b.probing = true
		b.probeAt = now
		return token{gen: b.gen, probe: true}, tr, true

	case stateHalfOpen:
		if b.probing {
			// Lease (D-28): a probe that outlived OpenTimeout is superseded.
			// A zero OpenTimeout never expires the lease.
			if c.cfg.OpenTimeout > 0 && now.Sub(b.probeAt) >= c.cfg.OpenTimeout {
				b.gen++
				b.probeAt = now
				return token{gen: b.gen, probe: true}, nil, true
			}
			return token{}, nil, false
		}
		b.probing = true
		b.probeAt = now
		return token{gen: b.gen, probe: true}, nil, true
	}

	return token{gen: b.gen}, nil, true
}

// record applies the outcome of a finished call under the breaker lock. A
// completion from an older generation is ignored and never touches the probe
// slot.
func (c *CircuitBreaker) record(b *handlerBreaker, tok token, out outcome) *transition {
	b.mu.Lock()
	defer b.mu.Unlock()

	if tok.gen != b.gen {
		return nil
	}

	now := time.Now()
	switch b.state {
	case stateClosed:
		switch out {
		case outcomeFailure:
			b.failures++
			if b.failures >= c.cfg.FailureThreshold {
				b.openedAt = now
				return b.move(stateOpen, reasonFailureThreshold, now)
			}
		case outcomeSuccess:
			b.failures = 0
		}

	case stateHalfOpen:
		if !tok.probe {
			return nil
		}
		b.probing = false
		switch out {
		case outcomeFailure:
			b.openedAt = now
			return b.move(stateOpen, reasonProbeFailed, now)
		case outcomeSuccess:
			b.successes++
			if b.successes >= c.cfg.SuccessThreshold {
				b.failures = 0
				b.successes = 0
				return b.move(stateClosed, reasonProbeSucceeded, now)
			}
		}
	}

	return nil
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

// emitTransition reports a state change after the lock was released. It does
// nothing for nil.
func (c *CircuitBreaker) emitTransition(ctx context.Context, handler string, tr *transition) {
	if tr == nil {
		return
	}

	switch tr.to {
	case stateHalfOpen:
		c.metrics.IncCounter(
			"eventbus_circuit_half_open_total",
			1,
			Labels{"handler": handler},
		)
	case stateOpen:
		c.metrics.IncCounter(
			"eventbus_circuit_open_total",
			1,
			Labels{"handler": handler},
		)
	}
	c.emitState(handler, tr.to)

	slog.DebugContext(ctx, "eventbus: circuit breaker transition",
		"handler", handler,
		"from", tr.from.String(),
		"to", tr.to.String(),
		"reason", tr.reason,
		"failures", tr.failures,
		"seq", tr.seq,
	)
}

func (c *CircuitBreaker) emitState(handler string, state circuitState) {
	c.metrics.SetGauge(
		"eventbus_circuit_state",
		float64(state),
		Labels{"handler": handler},
	)
}

func (c *CircuitBreaker) breaker(handler string) *handlerBreaker {
	c.mu.Lock()
	defer c.mu.Unlock()

	if b, ok := c.breakers[handler]; ok {
		return b
	}

	b := &handlerBreaker{state: stateClosed}
	c.breakers[handler] = b

	c.emitState(handler, stateClosed)

	return b
}
