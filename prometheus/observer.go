// Package prometheus provides an event.Observer that translates go-event-bus
// signals into a fixed Prometheus metric set.
//
// Construct one Observer per process and pass it as BusOptions.Observer, alone
// or inside event.MultiObserver:
//
//	reg := prometheus.NewRegistry()
//	obs := eventbusprom.NewObserver(reg)
//	// pass obs as the bus Observer option
//
// Every metric vector is declared once in NewObserver with fixed label names.
// Nothing is created while signals arrive, so a label mismatch cannot panic on
// the handler goroutine. Label values are bounded: handler and event names,
// closed outcome, reason and state vocabularies. Event ids, subjects and error
// text are never used as labels.
//
// # Metric set
//
//	eventbus_handler_duration_seconds   histogram  handler, event, outcome
//	eventbus_handler_total              counter    handler, event, outcome
//	eventbus_retry_attempts_total       counter    handler, event, reason
//	eventbus_breaker_transitions_total  counter    handler, from, to, reason
//	eventbus_breaker_state              gauge      handler (0 closed, 1 open, 2 half_open)
//	eventbus_dlq_records_total          counter    handler, event, reason
//	eventbus_dlq_publish_errors_total   counter    handler, event
//	eventbus_decode_failures_total      counter    reason
//	eventbus_store_errors_total         counter    handler, store, op
//	eventbus_idempotency_stale_total    counter    handler
//	eventbus_internal_errors_total      counter    component, op
//
// WithNamespace prefixes every name with ns_.
//
// # Migration from v0.3.0
//
// The v0.3.0 Provider created metrics dynamically. The fixed set replaces its
// names as follows (Phase 11 copies this table into the CHANGELOG):
//
//   - eventbus_invoker_calls_total and eventbus_invoker_latency_seconds become
//     eventbus_handler_total and eventbus_handler_duration_seconds.
//   - eventbus_retry_attempt_total becomes eventbus_retry_attempts_total.
//   - eventbus_retry_success_total, eventbus_retry_terminal_total and
//     eventbus_retry_exhausted_total become eventbus_handler_total by outcome
//     and eventbus_dlq_records_total by reason.
//   - eventbus_retry_backoff_ms is removed; the delay is on the retry span
//     event.
//   - eventbus_circuit_state becomes eventbus_breaker_state.
//   - eventbus_circuit_open_total and eventbus_circuit_half_open_total become
//     eventbus_breaker_transitions_total by to.
//   - eventbus_circuit_blocked_total becomes eventbus_handler_total with
//     outcome circuit_open.
//   - eventbus_dlq_published_total, eventbus_dlq_publish_error_total and the
//     unprefixed dlq_published_total and dlq_publish_error_total become
//     eventbus_dlq_records_total and eventbus_dlq_publish_errors_total.
//   - eventbus_idempotency_duplicate_total and
//     eventbus_idempotency_in_progress_total become eventbus_handler_total with
//     outcome duplicate or in_progress.
//   - eventbus_idempotency_error_total and eventbus_ratelimit_error_total
//     become eventbus_store_errors_total by store.
//   - eventbus_idempotency_processing_stale_total becomes
//     eventbus_idempotency_stale_total.
//   - eventbus_ratelimit_blocked_total becomes eventbus_handler_total with
//     outcome rate_limited.
//   - eventbus_idempotency_lock_acquired_total,
//     eventbus_idempotency_executed_total and eventbus_ratelimit_allowed_total
//     are removed; use eventbus_handler_total.
package prometheus

import (
	"context"
	"sync"

	event "github.com/isaquesb/go-event-bus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// DefaultBuckets are the histogram buckets, in seconds, of
// eventbus_handler_duration_seconds. They are the same buckets as v0.3.0.
var DefaultBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// breaker gauge values.
const (
	breakerGaugeClosed   = 0
	breakerGaugeOpen     = 1
	breakerGaugeHalfOpen = 2
)

var _ event.Observer = (*Observer)(nil)

type config struct {
	namespace string
}

// Option configures NewObserver.
type Option func(*config)

// WithNamespace prefixes every metric name with ns_, for example
// WithNamespace("shop") names the handler counter shop_eventbus_handler_total.
func WithNamespace(ns string) Option {
	return func(c *config) { c.namespace = ns }
}

// Observer implements event.Observer with a fixed set of Prometheus metrics.
// It is safe for concurrent use.
type Observer struct {
	handlerDuration    *prometheus.HistogramVec
	handlerTotal       *prometheus.CounterVec
	retryAttempts      *prometheus.CounterVec
	breakerTransitions *prometheus.CounterVec
	breakerState       *prometheus.GaugeVec
	dlqRecords         *prometheus.CounterVec
	dlqPublishErrors   *prometheus.CounterVec
	decodeFailures     *prometheus.CounterVec
	storeErrors        *prometheus.CounterVec
	idempotencyStale   *prometheus.CounterVec
	internalErrors     *prometheus.CounterVec

	// mu guards lastSeq and the breakerState write, so the Seq check and the
	// gauge write are one step.
	mu      sync.Mutex
	lastSeq map[breakerKey]uint64
}

// breakerKey scopes Seq: it increases per handler breaker of one CircuitBreaker
// instance, so the order guard is keyed by the handler and the breaker id.
type breakerKey struct {
	handler string
	breaker uint64
}

// NewObserver declares the whole metric set on reg and returns the Observer. A
// nil reg means prometheus.DefaultRegisterer.
//
// Registering twice on one registry panics, like prometheus.MustRegister; build
// one Observer per registry, or give each its own WithNamespace.
//
// eventbus_breaker_state is set only from BreakerTransition. A handler whose
// series is absent has a breaker that never transitioned, which means closed.
func NewObserver(reg prometheus.Registerer, opts ...Option) *Observer {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}
	f := promauto.With(reg)
	ns := cfg.namespace

	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return f.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: name, Help: help}, labels)
	}

	return &Observer{
		handlerDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns,
			Name:      "eventbus_handler_duration_seconds",
			Help:      "Duration of one handler chain invocation in seconds.",
			Buckets:   DefaultBuckets,
		}, []string{"handler", "event", "outcome"}),
		handlerTotal: counter("eventbus_handler_total",
			"Handler chain invocations by outcome.", "handler", "event", "outcome"),
		retryAttempts: counter("eventbus_retry_attempts_total",
			"Failed handler attempts that the retry policy repeats, by attempt class.", "handler", "event", "reason"),
		breakerTransitions: counter("eventbus_breaker_transitions_total",
			"Circuit breaker state transitions.", "handler", "from", "to", "reason"),
		breakerState: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Name:      "eventbus_breaker_state",
			Help:      "Circuit breaker state: 0 closed, 1 open, 2 half_open. Absent means it never transitioned.",
		}, []string{"handler"}),
		dlqRecords: counter("eventbus_dlq_records_total",
			"Messages successfully published to the dead-letter queue.", "handler", "event", "reason"),
		dlqPublishErrors: counter("eventbus_dlq_publish_errors_total",
			"Failed dead-letter queue publishes.", "handler", "event"),
		decodeFailures: counter("eventbus_decode_failures_total",
			"Messages that could not be decoded.", "reason"),
		storeErrors: counter("eventbus_store_errors_total",
			"Failed idempotency or rate-limit store calls.", "handler", "store", "op"),
		idempotencyStale: counter("eventbus_idempotency_stale_total",
			"Stale idempotency in-progress markers that were taken over.", "handler"),
		internalErrors: counter("eventbus_internal_errors_total",
			"Errors inside the library that no handler sees.", "component", "op"),
		lastSeq: make(map[breakerKey]uint64),
	}
}

// RetryAttempt counts a failed attempt that the retry policy repeats. The reason
// label is the attempt class (transient, terminal, timeout, panic).
func (o *Observer) RetryAttempt(_ context.Context, info event.RetryAttemptInfo) {
	o.retryAttempts.WithLabelValues(info.Handler, info.Event, string(info.Class)).Inc()
}

// Failure does nothing: terminal failures are already visible through
// eventbus_handler_total (HandlerDone) and eventbus_dlq_records_total
// (DeadLettered), so counting them again would double count.
func (o *Observer) Failure(context.Context, event.FailureContext) {}

// DeadLettered counts a successful dead-letter publish.
func (o *Observer) DeadLettered(_ context.Context, info event.DeadLetterInfo) {
	o.dlqRecords.WithLabelValues(info.Handler, info.Event, info.Reason).Inc()
}

// DLQPublishFailed counts a failed dead-letter publish.
func (o *Observer) DLQPublishFailed(_ context.Context, f event.DLQPublishFailure) {
	o.dlqPublishErrors.WithLabelValues(f.Handler, f.Event).Inc()
}

// DecodeFailed counts a message that could not be decoded, by reason only.
func (o *Observer) DecodeFailed(_ context.Context, f event.DecodeFailure) {
	o.decodeFailures.WithLabelValues(f.Reason).Inc()
}

// StoreError counts a failing idempotency or rate-limit store call.
func (o *Observer) StoreError(_ context.Context, e event.StoreErrorInfo) {
	o.storeErrors.WithLabelValues(e.Handler, e.Store, e.Op).Inc()
}

// IdempotencyStale counts a stale in-progress marker that was taken over.
func (o *Observer) IdempotencyStale(_ context.Context, s event.IdempotencyStaleInfo) {
	o.idempotencyStale.WithLabelValues(s.Handler).Inc()
}

// InternalError counts an error inside the library, by component and operation.
func (o *Observer) InternalError(_ context.Context, e event.InternalErrorInfo) {
	o.internalErrors.WithLabelValues(e.Component, e.Op).Inc()
}

// HandlerDone observes the handler duration and counts the invocation by
// outcome.
func (o *Observer) HandlerDone(_ context.Context, r event.HandlerResult) {
	outcome := string(r.Outcome)
	o.handlerDuration.WithLabelValues(r.Handler, r.Event, outcome).Observe(r.Duration.Seconds())
	o.handlerTotal.WithLabelValues(r.Handler, r.Event, outcome).Inc()
}

// BreakerTransition counts the transition and sets eventbus_breaker_state. The
// gauge is last-writer-wins by Seq per breaker instance (handler and BreakerID):
// an older Seq of the same breaker delivered late never overwrites a newer
// state, and a rebuilt breaker that reuses a handler name starts a new order and
// is followed. The gauge itself is labeled by handler only.
func (o *Observer) BreakerTransition(_ context.Context, t event.BreakerTransition) {
	o.breakerTransitions.WithLabelValues(t.Handler, string(t.From), string(t.To), string(t.Reason)).Inc()

	var v float64
	switch t.To {
	case event.BreakerClosed:
		v = breakerGaugeClosed
	case event.BreakerOpen:
		v = breakerGaugeOpen
	case event.BreakerHalfOpen:
		v = breakerGaugeHalfOpen
	default:
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	key := breakerKey{handler: t.Handler, breaker: t.BreakerID}
	if last, ok := o.lastSeq[key]; ok && t.Seq <= last {
		return
	}
	o.lastSeq[key] = t.Seq
	o.breakerState.WithLabelValues(t.Handler).Set(v)
}
