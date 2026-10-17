package event

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// DefaultLogEvery is the default deduplication window of a SlogObserver.
const DefaultLogEvery = 10 * time.Second

// soMaxKeys caps the number of deduplication keys a SlogObserver tracks.
const soMaxKeys = 1024

// soSummaryMsg is the message of the summary record that closes a
// deduplication window.
const soSummaryMsg = "eventbus: repeated log records suppressed"

// SlogOption configures a SlogObserver.
type SlogOption func(*SlogObserver)

// WithLogEvery sets the deduplication window. Within one window the first
// record per (signal, handler, outcome, reason) is logged at once and repeats
// are only counted; one summary record with the suppressed count is logged when
// the window ends. A zero or negative d disables deduplication.
func WithLogEvery(d time.Duration) SlogOption {
	return func(o *SlogObserver) {
		if d < 0 {
			d = 0
		}
		o.every = d
	}
}

// SlogObserver is an Observer that writes every signal that needs a log line as
// a structured log/slog record, with the message context. It is the default
// observer of every bus when none is configured, which keeps failures from
// being silent with zero configuration; NopObserver{} silences everything.
//
// Records are deduplicated per window (see WithLogEvery) so a failure storm
// produces one line plus a summary instead of thousands. Deduplication only
// affects logs: metrics and traces stay exact. Circuit breaker transitions and
// retry attempts are never deduplicated.
type SlogObserver struct {
	logger *slog.Logger
	every  time.Duration

	mu      sync.Mutex
	entries map[soKey]*soEntry
}

var _ Observer = (*SlogObserver)(nil)

// soKey identifies one deduplication series.
type soKey struct {
	signal, handler, outcome, reason string
}

// soEntry is the state of one deduplication series.
type soEntry struct {
	first      time.Time
	suppressed int
	level      slog.Level
	timer      *time.Timer
}

// NewSlogObserver returns a SlogObserver that writes to logger. A nil logger
// resolves slog.Default() at log time, so the observer follows later
// slog.SetDefault calls.
func NewSlogObserver(logger *slog.Logger, opts ...SlogOption) *SlogObserver {
	o := &SlogObserver{logger: logger, every: DefaultLogEvery}
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	return o
}

// log writes one record to the configured logger, or to slog.Default() when
// none was configured.
func (o *SlogObserver) log(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	logger := o.logger
	if logger == nil {
		logger = slog.Default()
	}
	if !logger.Enabled(ctx, level) {
		return
	}
	logger.LogAttrs(ctx, level, msg, attrs...)
}

// record logs one deduplicable record. With deduplication enabled, the first
// record per key and window is logged at once; repeats inside the window are
// counted and reported by one summary record when the window ends. A key that
// would exceed soMaxKeys is logged untracked, so the keyspace never grows
// without bound. The logger is never called while holding mu.
func (o *SlogObserver) record(ctx context.Context, k soKey, level slog.Level, msg string, attrs ...slog.Attr) {
	if o.every <= 0 {
		o.log(ctx, level, msg, attrs...)
		return
	}
	now := time.Now()
	o.mu.Lock()
	if e, ok := o.entries[k]; ok {
		if e.suppressed == 0 && now.Sub(e.first) >= o.every {
			// The window elapsed with nothing suppressed: start a new one.
			e.first = now
			e.level = level
			o.mu.Unlock()
			o.log(ctx, level, msg, attrs...)
			return
		}
		e.suppressed++
		if e.suppressed == 1 {
			e.timer = time.AfterFunc(o.every-now.Sub(e.first), func() { o.flushKey(k, e) })
		}
		o.mu.Unlock()
		return
	}
	if len(o.entries) >= soMaxKeys {
		for key, e := range o.entries {
			if e.suppressed == 0 && now.Sub(e.first) >= o.every {
				delete(o.entries, key)
			}
		}
	}
	if len(o.entries) >= soMaxKeys {
		o.mu.Unlock()
		o.log(ctx, level, msg, attrs...)
		return
	}
	if o.entries == nil {
		o.entries = make(map[soKey]*soEntry)
	}
	o.entries[k] = &soEntry{first: now, level: level}
	o.mu.Unlock()
	o.log(ctx, level, msg, attrs...)
}

// flushKey ends the window of k: when e is still the tracked entry it removes it
// and logs the summary. It is the body of the window timer.
func (o *SlogObserver) flushKey(k soKey, e *soEntry) {
	o.mu.Lock()
	if o.entries[k] != e {
		o.mu.Unlock()
		return
	}
	delete(o.entries, k)
	o.mu.Unlock()
	o.summary(k, e)
}

// summary logs the record that reports how many records of k were suppressed.
func (o *SlogObserver) summary(k soKey, e *soEntry) {
	o.log(context.Background(), e.level, soSummaryMsg,
		slog.String("signal", k.signal),
		slog.String("handler", k.handler),
		slog.String("outcome", k.outcome),
		slog.String("reason", k.reason),
		slog.Int("suppressed", e.suppressed),
		slog.Duration("window", o.every),
	)
}

// Flush logs every pending summary at once and stops all window timers. Call it
// on shutdown, or in tests, so that no suppressed count is lost.
func (o *SlogObserver) Flush() {
	type pending struct {
		k soKey
		e *soEntry
	}
	var out []pending
	o.mu.Lock()
	for k, e := range o.entries {
		if e.suppressed > 0 {
			if e.timer != nil {
				e.timer.Stop()
			}
			out = append(out, pending{k, e})
		}
	}
	o.entries = nil
	o.mu.Unlock()
	for _, p := range out {
		o.summary(p.k, p.e)
	}
}

// soOpt appends slog.String(key, v) only when v is not empty.
func soOpt(attrs []slog.Attr, key, v string) []slog.Attr {
	if v == "" {
		return attrs
	}
	return append(attrs, slog.String(key, v))
}

// Failure logs a terminal failure: Warn for a dead-lettered or rejected
// message, Error otherwise. A recovered panic adds its stack.
func (o *SlogObserver) Failure(ctx context.Context, f FailureContext) {
	level := slog.LevelError
	if errors.Is(f.Cause, ErrDeadLettered) || f.Outcome == OutcomeRejected {
		level = slog.LevelWarn
	}
	attrs := []slog.Attr{
		slog.String("outcome", string(f.Outcome)),
		slog.String("reason", f.Reason),
		slog.String("handler", f.Handler),
	}
	if f.Event != nil {
		attrs = append(attrs, slog.String("event", f.Event.Name()))
	}
	attrs = soOpt(attrs, "event_id", f.Metadata.ID)
	attrs = soOpt(attrs, "correlation_id", f.Metadata.CorrelationID)
	attrs = append(attrs,
		slog.Int("attempts", f.Attempts),
		slog.Int("delivery_count", f.Delivery.Count),
		slog.Int("max_deliver", f.Delivery.Max),
		slog.String("subject", f.Delivery.Subject),
		slog.String("invoker", string(f.Invoker)),
	)
	attrs = soOpt(attrs, "breaker", string(f.Breaker))
	attrs = append(attrs, slog.Int("history_len", len(f.History)))
	if f.Cause != nil {
		attrs = append(attrs, slog.String("err", f.Cause.Error()))
	}
	var pe *PanicError
	if errors.As(f.Cause, &pe) && pe != nil && len(pe.Stack) > 0 {
		attrs = append(attrs, slog.String("stack", string(pe.Stack)))
	}
	o.record(ctx, soKey{"failure", f.Handler, string(f.Outcome), f.Reason}, level, "eventbus: message failed", attrs...)
}

// DeadLettered logs a successful dead-letter publish at Info.
func (o *SlogObserver) DeadLettered(ctx context.Context, info DeadLetterInfo) {
	o.record(ctx, soKey{"dead_lettered", info.Handler, "", info.Reason}, slog.LevelInfo, "eventbus: message dead-lettered",
		slog.String("handler", info.Handler),
		slog.String("event", info.Event),
		slog.String("reason", info.Reason),
		slog.String("subject", info.Subject),
		slog.Int("attempts", info.Attempts),
		slog.Int("delivery_count", info.Delivery.Count),
		slog.Int("size", info.Size),
	)
}

// DecodeFailed logs an undecodable message at Warn.
func (o *SlogObserver) DecodeFailed(ctx context.Context, f DecodeFailure) {
	attrs := []slog.Attr{
		slog.String("handler", f.Handler),
		slog.String("subject", f.Subject),
		slog.String("reason", f.Reason),
		slog.Int("delivery_count", f.Delivery.Count),
		slog.Bool("final", f.Final),
	}
	if f.Err != nil {
		attrs = append(attrs, slog.String("err", f.Err.Error()))
	}
	o.record(ctx, soKey{"decode_failed", f.Handler, "", f.Reason}, slog.LevelWarn, "eventbus: decode failed", attrs...)
}

// DLQPublishFailed logs a failed dead-letter publish at Error.
func (o *SlogObserver) DLQPublishFailed(ctx context.Context, f DLQPublishFailure) {
	attrs := []slog.Attr{
		slog.String("handler", f.Handler),
		slog.String("event", f.Event),
		slog.String("reason", f.Reason),
		slog.String("subject", f.Subject),
		slog.Int("attempts", f.Attempts),
		slog.Bool("final", f.Final),
	}
	if f.Err != nil {
		attrs = append(attrs, slog.String("err", f.Err.Error()))
	}
	o.record(ctx, soKey{"dlq_publish_failed", f.Handler, "", f.Reason}, slog.LevelError, "eventbus: dead-letter publish failed", attrs...)
}

// StoreError logs a failing idempotency or rate-limit store call at Error.
func (o *SlogObserver) StoreError(ctx context.Context, e StoreErrorInfo) {
	attrs := []slog.Attr{
		slog.String("handler", e.Handler),
		slog.String("event", e.Event),
		slog.String("store", e.Store),
		slog.String("op", e.Op),
	}
	if e.Err != nil {
		attrs = append(attrs, slog.String("err", e.Err.Error()))
	}
	o.record(ctx, soKey{"store_error", e.Handler, "", e.Store + ":" + e.Op}, slog.LevelError, "eventbus: store error", attrs...)
}

// IdempotencyStale logs a taken-over stale in-progress marker at Warn.
func (o *SlogObserver) IdempotencyStale(ctx context.Context, s IdempotencyStaleInfo) {
	o.record(ctx, soKey{"idempotency_stale", s.Handler, "", ""}, slog.LevelWarn, "eventbus: stale idempotency lock",
		slog.String("handler", s.Handler),
		slog.String("event", s.Event),
		slog.Duration("age", s.Age),
	)
}

// InternalError logs an error inside the library at Error. A recovered panic
// adds its stack.
func (o *SlogObserver) InternalError(ctx context.Context, e InternalErrorInfo) {
	attrs := []slog.Attr{
		slog.String("component", e.Component),
		slog.String("op", e.Op),
		slog.String("handler", e.Handler),
		slog.String("subject", e.Subject),
	}
	if e.Err != nil {
		attrs = append(attrs, slog.String("err", e.Err.Error()))
	}
	var pe *PanicError
	if errors.As(e.Err, &pe) && pe != nil && len(pe.Stack) > 0 {
		attrs = append(attrs, slog.String("stack", string(pe.Stack)))
	}
	o.record(ctx, soKey{"internal_error", e.Handler, "", e.Component + ":" + e.Op}, slog.LevelError, "eventbus: internal error", attrs...)
}

// BreakerTransition logs a circuit breaker state change: Warn when the breaker
// opens, Info otherwise. It is never deduplicated.
func (o *SlogObserver) BreakerTransition(ctx context.Context, t BreakerTransition) {
	level := slog.LevelInfo
	if t.To == BreakerOpen {
		level = slog.LevelWarn
	}
	o.log(ctx, level, "eventbus: circuit breaker transition",
		slog.String("handler", t.Handler),
		slog.String("from", string(t.From)),
		slog.String("to", string(t.To)),
		slog.String("reason", string(t.Reason)),
		slog.Int("failures", t.Failures),
		slog.Uint64("seq", t.Seq),
	)
}

// RetryAttempt logs a scheduled retry at Debug. It is never deduplicated.
func (o *SlogObserver) RetryAttempt(ctx context.Context, info RetryAttemptInfo) {
	attrs := []slog.Attr{
		slog.String("handler", info.Handler),
		slog.String("event", info.Event),
		slog.Int("attempt", info.Attempt),
		slog.String("class", string(info.Class)),
		slog.Duration("delay", info.Delay),
	}
	if info.Err != nil {
		attrs = append(attrs, slog.String("err", info.Err.Error()))
	}
	o.log(ctx, slog.LevelDebug, "eventbus: retry scheduled", attrs...)
}

// HandlerDone logs nothing: metrics and traces cover it.
func (o *SlogObserver) HandlerDone(context.Context, HandlerResult) {}
