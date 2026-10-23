package event_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"github.com/isaquesb/go-event-bus"
)

// fsPermanent is a permanent error: Terminal() reports true, like the
// invoker module's PermanentError, which root tests cannot import.
type fsPermanent struct{ msg string }

func (e fsPermanent) Error() string  { return e.msg }
func (e fsPermanent) Terminal() bool { return true }

// fsEvent is an event with its own OnErr method that records every call.
type fsEvent struct {
	mu   sync.Mutex
	errs []error
}

func (e *fsEvent) Name() string { return "orders.created" }
func (e *fsEvent) OnErr(_ context.Context, err error, _ string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.errs = append(e.errs, err)
}
func (e *fsEvent) calls() []error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]error(nil), e.errs...)
}

// fsObs is the recording Observer of the Settle tests: an obsRecorder whose
// observer() also writes every log-worthy signal through slog.Default() without
// deduplication, so the tests that capture log records see each one.
type fsObs struct{ obsRecorder }

// ctx returns parent carrying the fanned-out observer.
func (o *fsObs) ctx(parent context.Context) context.Context {
	return event.WithObserver(parent, event.MultiObserver(o, event.NewSlogObserver(nil, event.WithLogEvery(0))))
}

// causes returns the Cause of every recorded Failure.
func (o *fsObs) causes() []error {
	var out []error
	for _, f := range o.Failures() {
		out = append(out, f.Cause)
	}
	return out
}

func fsMessageCtx(d event.Delivery) context.Context {
	ctx := event.WithMetadata(context.Background(), event.Metadata{ID: "evt-1", Name: "orders.created"})
	ctx = event.WithDelivery(ctx, d)
	return event.WithExecution(ctx, event.Execution{Handler: "h"})
}

// Tracer: Delivery in ctx -> Dispose -> NewDLQRecord -> PublishDeadLetter ->
// MemoryDLQ -> DeadLetteredError -> Observer.Failure and OnErr, end to end.
func TestSettle_TerminalDeadLetteredOnce(t *testing.T) {
	raw := []byte(`{"id":"evt-1","name":"orders.created","version":1}`)
	ctx := fsMessageCtx(event.Delivery{Count: 1, Max: 1, Subject: "orders.created", Raw: raw})
	cause := fsPermanent{msg: "bad order"}
	evt := &fsEvent{}
	obs := &fsObs{}
	dlq := event.NewMemoryDLQ()

	got := event.Settle(obs.ctx(ctx), event.SettleInput{
		SubscriptionCtx: context.Background(),
		Evt:             evt,
		Handler:         "h",
		Err:             &event.FailureError{Cause: cause, Reason: event.ReasonTerminal, Attempts: 2},
		Publisher:       dlq,
	})

	if got.Disposition != event.DispositionTerminal {
		t.Fatalf("Disposition = %v, want terminal", got.Disposition)
	}
	if got.Outcome != event.OutcomeDeadLettered {
		t.Fatalf("Outcome = %q, want dead_lettered", got.Outcome)
	}
	if got.Reason != event.ReasonTerminal {
		t.Fatalf("Reason = %q, want terminal", got.Reason)
	}

	recs := dlq.Records()
	if len(recs) != 1 {
		t.Fatalf("published %d records, want 1", len(recs))
	}
	rec := recs[0]
	if string(rec.Raw) != string(raw) {
		t.Errorf("Raw = %q, want the input bytes", rec.Raw)
	}
	if rec.EventID != "evt-1" || rec.Handler != "h" || rec.Subject != "orders.created" || rec.Attempts != 2 {
		t.Errorf("record = id %q handler %q subject %q attempts %d", rec.EventID, rec.Handler, rec.Subject, rec.Attempts)
	}

	for name, calls := range map[string][]error{"observer Failure": obs.causes(), "event OnErr": evt.calls()} {
		if len(calls) != 1 {
			t.Fatalf("%s called %d times, want 1", name, len(calls))
		}
		if !errors.Is(calls[0], event.ErrDeadLettered) || !errors.Is(calls[0], cause) {
			t.Errorf("%s error = %v, want ErrDeadLettered and the cause", name, calls[0])
		}
	}
}

func TestSettle_ChainDeadLetteredIsAcked(t *testing.T) {
	ctx := fsMessageCtx(event.Delivery{Count: 1, Max: 1, Subject: "orders.created"})
	obs := &fsObs{}
	dlq := event.NewMemoryDLQ()

	got := event.Settle(obs.ctx(ctx), event.SettleInput{
		SubscriptionCtx: context.Background(),
		Evt:             &fsEvent{},
		Handler:         "h",
		Err:             &event.DeadLetteredError{Cause: fsPermanent{msg: "bad"}, Reason: event.ReasonTerminal},
		Publisher:       dlq,
	})

	if got.Disposition != event.DispositionAck {
		t.Fatalf("Disposition = %v, want ack", got.Disposition)
	}
	if dlq.Len() != 0 {
		t.Errorf("published %d records, want 0 (the chain already dead-lettered)", dlq.Len())
	}
	if n := len(obs.Failures()); n != 1 {
		t.Errorf("Failure called %d times, want 1", n)
	}
	if n := len(obs.DeadLetters()); n != 0 {
		t.Errorf("DeadLettered signals = %d, want 0 (the chain already published)", n)
	}
}

// fsLogs captures slog records written through the default logger.
type fsLogs struct {
	mu   sync.Mutex
	recs []fsLogRecord
}

type fsLogRecord struct {
	level slog.Level
	msg   string
	attrs map[string]string
}

type fsLogHandler struct{ logs *fsLogs }

func (h fsLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h fsLogHandler) Handle(_ context.Context, r slog.Record) error {
	rec := fsLogRecord{level: r.Level, msg: r.Message, attrs: map[string]string{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.String()
		return true
	})
	h.logs.mu.Lock()
	h.logs.recs = append(h.logs.recs, rec)
	h.logs.mu.Unlock()
	return nil
}
func (h fsLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h fsLogHandler) WithGroup(string) slog.Handler      { return h }

// fsCaptureLogs installs a capturing default logger until the test ends.
func fsCaptureLogs(t *testing.T) *fsLogs {
	t.Helper()
	logs := &fsLogs{}
	prev := slog.Default()
	slog.SetDefault(slog.New(fsLogHandler{logs: logs}))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return logs
}

// forHandler returns the records whose handler attribute is name.
func (l *fsLogs) forHandler(name string) []fsLogRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []fsLogRecord
	for _, r := range l.recs {
		if r.attrs["handler"] == name {
			out = append(out, r)
		}
	}
	return out
}

func fsSettle(ctx context.Context, d event.Delivery, in event.SettleInput) event.Settlement {
	if in.SubscriptionCtx == nil {
		in.SubscriptionCtx = context.Background()
	}
	return event.Settle(event.WithDelivery(ctx, d), in)
}

func TestSettle_DroppedWithoutPublisher(t *testing.T) {
	logs := fsCaptureLogs(t)
	evt := &fsEvent{}
	obs := &fsObs{}
	err := &event.FailureError{Cause: fsPermanent{msg: "bad"}, Reason: event.ReasonTerminal, Attempts: 1}

	got := fsSettle(obs.ctx(context.Background()), event.Delivery{Count: 1, Max: 1, Subject: "s"}, event.SettleInput{
		Evt: evt, Handler: "fs-dropped", Err: err,
	})

	if got.Disposition != event.DispositionTerminal || got.Outcome != event.OutcomeDropped {
		t.Fatalf("got %v / %q, want terminal / dropped", got.Disposition, got.Outcome)
	}
	if calls := obs.causes(); len(calls) != 1 || calls[0] != error(err) {
		t.Errorf("observer Failure causes = %v, want the identical error once", calls)
	}
	if f := obs.Failures()[0]; f.Outcome != event.OutcomeDropped || f.Handler != "fs-dropped" || f.Event != event.Event(evt) {
		t.Errorf("Failure = outcome %q handler %q event %v, want dropped, fs-dropped and the event", f.Outcome, f.Handler, f.Event)
	}
	if calls := evt.calls(); len(calls) != 1 || calls[0] != error(err) {
		t.Errorf("event OnErr calls = %v, want the identical error once", calls)
	}
	recs := logs.forHandler("fs-dropped")
	if len(recs) != 1 || recs[0].level != slog.LevelError || recs[0].attrs["outcome"] != "dropped" {
		t.Errorf("logs = %+v, want one Error record with outcome dropped", recs)
	}
}

func TestSettle_PublishFailureNeverTerminal(t *testing.T) {
	cause := fsPermanent{msg: "bad"}
	chainErr := &event.FailureError{Cause: cause, Reason: event.ReasonTerminal, Attempts: 1}

	t.Run("non-final", func(t *testing.T) {
		logs := fsCaptureLogs(t)
		obs := &fsObs{}
		pub := &fsPublisher{failUntil: -1}
		got := fsSettle(obs.ctx(context.Background()), event.Delivery{Count: 1, Max: 3}, event.SettleInput{
			Evt: &fsEvent{}, Handler: "fs-pf-nonfinal", Err: chainErr, Publisher: pub,
		})
		if got.Disposition != event.DispositionRetryDelayed {
			t.Fatalf("Disposition = %v, want retry_delayed", got.Disposition)
		}
		if pub.calls.Load() != 1 {
			t.Errorf("publish calls = %d, want 1", pub.calls.Load())
		}
		if len(obs.Failures()) != 0 {
			t.Errorf("Failure called %d times on a non-final delivery, want 0", len(obs.Failures()))
		}
		if f := obs.DLQPublishFailures(); len(f) != 1 || f[0].Final || f[0].Delivery.Raw != nil {
			t.Errorf("DLQPublishFailed signals = %+v, want one non-final", f)
		}
		recs := logs.forHandler("fs-pf-nonfinal")
		if len(recs) != 1 || recs[0].level != slog.LevelError {
			t.Errorf("logs = %+v, want one Error record", recs)
		}
	})

	t.Run("final", func(t *testing.T) {
		fsCaptureLogs(t)
		evt := &fsEvent{}
		obs := &fsObs{}
		pub := &fsPublisher{failUntil: -1}
		got := fsSettle(obs.ctx(context.Background()), event.Delivery{Count: 3, Max: 3}, event.SettleInput{
			Evt: evt, Handler: "fs-pf-final", Err: chainErr, Publisher: pub,
		})
		if got.Disposition != event.DispositionRetryDelayed || got.Outcome != event.OutcomeDLQPublishFailed {
			t.Fatalf("got %v / %q, want retry_delayed / dlq_publish_failed", got.Disposition, got.Outcome)
		}
		if pub.calls.Load() != 3 {
			t.Errorf("publish calls = %d, want 3", pub.calls.Load())
		}
		for name, calls := range map[string][]error{"observer": obs.causes(), "event": evt.calls()} {
			if len(calls) != 1 || !errors.Is(calls[0], event.ErrDeadLetterFailed) || !errors.Is(calls[0], cause) {
				t.Errorf("%s reports = %v, want one error matching ErrDeadLetterFailed and the cause", name, calls)
			}
		}
		if f := obs.Failures()[0]; f.Outcome != event.OutcomeDLQPublishFailed {
			t.Errorf("Failure outcome = %q, want dlq_publish_failed", f.Outcome)
		}
	})
}

func TestSettle_ChainDeadLetterFailed(t *testing.T) {
	failed := &event.DeadLetterFailedError{
		Cause: fsPermanent{msg: "bad"}, Reason: event.ReasonTerminal, PublishErr: errors.New("down"),
	}
	for _, c := range []struct {
		name       string
		d          event.Delivery
		wantReport int
	}{
		{"non-final", event.Delivery{Count: 1, Max: 3}, 0},
		{"final", event.Delivery{Count: 3, Max: 3}, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			fsCaptureLogs(t)
			obs := &fsObs{}
			dlq := event.NewMemoryDLQ()
			got := fsSettle(obs.ctx(context.Background()), c.d, event.SettleInput{
				Evt: &fsEvent{}, Handler: "fs-chain-dlf", Err: failed, Publisher: dlq,
			})
			if got.Disposition != event.DispositionRetryDelayed {
				t.Errorf("Disposition = %v, want retry_delayed", got.Disposition)
			}
			if dlq.Len() != 0 {
				t.Errorf("published %d records, want 0 (the chain already tried)", dlq.Len())
			}
			if n := len(obs.Failures()); n != c.wantReport {
				t.Errorf("Failure called %d times, want %d", n, c.wantReport)
			}
		})
	}
}

func TestSettle_InterruptedNeverDeadLettered(t *testing.T) {
	sub, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("non-final", func(t *testing.T) {
		fsCaptureLogs(t)
		obs := &fsObs{}
		dlq := event.NewMemoryDLQ()
		got := fsSettle(obs.ctx(context.Background()), event.Delivery{Count: 1, Max: 3}, event.SettleInput{
			SubscriptionCtx: sub, Evt: &fsEvent{}, Handler: "fs-int", Err: context.Canceled, Publisher: dlq,
		})
		if got.Disposition != event.DispositionRetry || got.Outcome != "" {
			t.Errorf("got %v / %q, want retry / no outcome", got.Disposition, got.Outcome)
		}
		if dlq.Len() != 0 || len(obs.Failures()) != 0 {
			t.Errorf("published %d, reported %d; want 0 and 0", dlq.Len(), len(obs.Failures()))
		}
	})

	t.Run("final", func(t *testing.T) {
		fsCaptureLogs(t)
		evt := &fsEvent{}
		obs := &fsObs{}
		dlq := event.NewMemoryDLQ()
		got := fsSettle(obs.ctx(context.Background()), event.Delivery{Count: 3, Max: 3}, event.SettleInput{
			SubscriptionCtx: sub, Evt: evt, Handler: "fs-int", Err: context.Canceled, Publisher: dlq,
		})
		if got.Disposition != event.DispositionRetry || got.Outcome != event.OutcomeInterrupted {
			t.Errorf("got %v / %q, want retry / interrupted", got.Disposition, got.Outcome)
		}
		if dlq.Len() != 0 {
			t.Errorf("published %d records, want 0 (never dead-lettered, D-07)", dlq.Len())
		}
		if len(obs.Failures()) != 1 || len(evt.calls()) != 1 {
			t.Errorf("reported observer %d, event %d; want 1 and 1", len(obs.Failures()), len(evt.calls()))
		}
		if f := obs.Failures()[0]; f.Outcome != event.OutcomeInterrupted {
			t.Errorf("Failure outcome = %q, want interrupted", f.Outcome)
		}
	})
}

func TestSettle_HandlerTimeoutIsTransient(t *testing.T) {
	fsCaptureLogs(t)
	dlq := event.NewMemoryDLQ()
	got := fsSettle(context.Background(), event.Delivery{Count: 3, Max: 3}, event.SettleInput{
		Evt: &fsEvent{}, Handler: "fs-timeout", Err: context.DeadlineExceeded, Publisher: dlq,
	})
	if got.Disposition != event.DispositionTerminal || got.Outcome != event.OutcomeDeadLettered {
		t.Fatalf("got %v / %q, want terminal / dead_lettered", got.Disposition, got.Outcome)
	}
	recs := dlq.Records()
	if len(recs) != 1 || recs[0].Reason != event.ReasonDeliveryExhausted {
		t.Errorf("records = %+v, want one with reason delivery_exhausted", recs)
	}
}

func TestSettle_DuplicateIsQuiet(t *testing.T) {
	logs := fsCaptureLogs(t)
	obs := &fsObs{}
	dlq := event.NewMemoryDLQ()
	got := fsSettle(obs.ctx(context.Background()), event.Delivery{Count: 3, Max: 3}, event.SettleInput{
		Evt: &fsEvent{}, Handler: "fs-dup", Err: event.ErrDuplicate, Publisher: dlq,
	})
	if got.Disposition != event.DispositionAck {
		t.Errorf("Disposition = %v, want ack", got.Disposition)
	}
	if dlq.Len() != 0 || len(obs.All()) != 0 || len(logs.forHandler("fs-dup")) != 0 {
		t.Errorf("published %d, signals %d, logged %d; want all 0", dlq.Len(), len(obs.All()), len(logs.forHandler("fs-dup")))
	}
}

func TestSettle_DecodeFailureEvtNil(t *testing.T) {
	logs := fsCaptureLogs(t)
	obs := &fsObs{}
	dlq := event.NewMemoryDLQ()
	raw := []byte(`{"name":"nope"}`)
	got := fsSettle(obs.ctx(context.Background()), event.Delivery{Count: 1, Max: 1, Subject: "nope", Raw: raw}, event.SettleInput{
		Evt: nil, Handler: "fs-decode", Err: &event.DecodeError{Name: "nope", Err: event.ErrUnknownEvent},
		Publisher: dlq,
	})
	if got.Disposition != event.DispositionTerminal || got.Outcome != event.OutcomeDeadLettered {
		t.Fatalf("got %v / %q, want terminal / dead_lettered", got.Disposition, got.Outcome)
	}
	recs := dlq.Records()
	if len(recs) != 1 || recs[0].Evt != nil || recs[0].Reason != event.ReasonDecodeUnknownEvent || string(recs[0].Raw) != string(raw) {
		t.Errorf("records = %+v, want one with nil Evt, reason decode_unknown_event and the raw bytes", recs)
	}
	failures := obs.Failures()
	if len(failures) != 1 || failures[0].Event != nil || failures[0].Outcome != event.OutcomeDecodeFailed {
		t.Errorf("Failure calls = %+v, want 1 with a nil event and outcome decode_failed", failures)
	}
	decodes := obs.DecodeFailures()
	if len(decodes) != 1 || !decodes[0].Final || decodes[0].Delivery.Raw != nil || decodes[0].Reason != event.ReasonDecodeUnknownEvent {
		t.Errorf("DecodeFailed signals = %+v, want one final, redacted, decode_unknown_event", decodes)
	}
	if len(logs.forHandler("fs-decode")) == 0 {
		t.Error("decode failure was not logged")
	}
}

func TestSettle_UnknownVersionRetriedThenDeadLettered(t *testing.T) {
	err := &event.DecodeError{Name: "orders.created", Version: 9, Err: event.ErrUnknownVersion}

	t.Run("non-final retries with a warning", func(t *testing.T) {
		logs := fsCaptureLogs(t)
		obs := &fsObs{}
		dlq := event.NewMemoryDLQ()
		got := fsSettle(obs.ctx(context.Background()), event.Delivery{Count: 1, Max: 3}, event.SettleInput{
			Handler: "fs-uv-nonfinal", Err: err, Publisher: dlq,
		})
		if got.Disposition != event.DispositionRetryDelayed || dlq.Len() != 0 {
			t.Errorf("got %v with %d records, want retry_delayed and 0", got.Disposition, dlq.Len())
		}
		recs := logs.forHandler("fs-uv-nonfinal")
		if len(recs) != 1 || recs[0].level != slog.LevelWarn {
			t.Errorf("logs = %+v, want one Warn record", recs)
		}
		if d := obs.DecodeFailures(); len(d) != 1 || d[0].Final || len(obs.Failures()) != 0 {
			t.Errorf("DecodeFailed %+v Failures %d, want one non-final DecodeFailed and no Failure", d, len(obs.Failures()))
		}
	})

	t.Run("final is dead-lettered", func(t *testing.T) {
		fsCaptureLogs(t)
		dlq := event.NewMemoryDLQ()
		got := fsSettle(context.Background(), event.Delivery{Count: 3, Max: 3}, event.SettleInput{
			Handler: "fs-uv-final", Err: err, Publisher: dlq,
		})
		recs := dlq.Records()
		if got.Disposition != event.DispositionTerminal || len(recs) != 1 || recs[0].Reason != event.ReasonUnknownVersion {
			t.Errorf("got %v with records %+v, want terminal and one unknown_version record", got.Disposition, recs)
		}
	})
}

func TestSettle_InProgressOnFinal(t *testing.T) {
	fsCaptureLogs(t)
	dlq := event.NewMemoryDLQ()
	got := fsSettle(context.Background(), event.Delivery{Count: 3, Max: 3}, event.SettleInput{
		Evt: &fsEvent{}, Handler: "fs-inprogress", Err: event.ErrInProgress, Publisher: dlq,
	})
	recs := dlq.Records()
	if got.Disposition != event.DispositionTerminal || len(recs) != 1 {
		t.Fatalf("got %v with %d records, want terminal and 1", got.Disposition, len(recs))
	}
	if recs[0].Reason != event.ReasonInProgress || recs[0].Attempts != 0 {
		t.Errorf("record reason %q attempts %d, want in_progress and 0", recs[0].Reason, recs[0].Attempts)
	}
}
