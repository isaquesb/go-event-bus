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

// fsBusErr records every bus OnErr callback.
type fsBusErr struct {
	mu   sync.Mutex
	errs []error
	evts []event.Event
}

func (b *fsBusErr) fn(_ context.Context, evt event.Event, err error, _ string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.errs = append(b.errs, err)
	b.evts = append(b.evts, evt)
}
func (b *fsBusErr) calls() []error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]error(nil), b.errs...)
}

func fsMessageCtx(d event.Delivery) context.Context {
	ctx := event.WithMetadata(context.Background(), event.Metadata{ID: "evt-1", Name: "orders.created"})
	ctx = event.WithDelivery(ctx, d)
	return event.WithExecution(ctx, event.Execution{Handler: "h"})
}

// Tracer: Delivery in ctx -> Dispose -> NewDLQRecord -> PublishDeadLetter ->
// MemoryDLQ -> DeadLetteredError -> OnErr, end to end.
func TestSettle_TerminalDeadLetteredOnce(t *testing.T) {
	raw := []byte(`{"id":"evt-1","name":"orders.created","version":1}`)
	ctx := fsMessageCtx(event.Delivery{Count: 1, Max: 1, Subject: "orders.created", Raw: raw})
	cause := fsPermanent{msg: "bad order"}
	evt := &fsEvent{}
	bus := &fsBusErr{}
	dlq := event.NewMemoryDLQ()

	got := event.Settle(ctx, event.SettleInput{
		SubscriptionCtx: context.Background(),
		Evt:             evt,
		Handler:         "h",
		Err:             &event.FailureError{Cause: cause, Reason: event.ReasonTerminal, Attempts: 2},
		Publisher:       dlq,
		OnErr:           bus.fn,
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

	for name, calls := range map[string][]error{"bus OnErr": bus.calls(), "event OnErr": evt.calls()} {
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
	bus := &fsBusErr{}
	dlq := event.NewMemoryDLQ()

	got := event.Settle(ctx, event.SettleInput{
		SubscriptionCtx: context.Background(),
		Evt:             &fsEvent{},
		Handler:         "h",
		Err:             &event.DeadLetteredError{Cause: fsPermanent{msg: "bad"}, Reason: event.ReasonTerminal},
		Publisher:       dlq,
		OnErr:           bus.fn,
	})

	if got.Disposition != event.DispositionAck {
		t.Fatalf("Disposition = %v, want ack", got.Disposition)
	}
	if dlq.Len() != 0 {
		t.Errorf("published %d records, want 0 (the chain already dead-lettered)", dlq.Len())
	}
	if n := len(bus.calls()); n != 1 {
		t.Errorf("OnErr called %d times, want 1", n)
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
	bus := &fsBusErr{}
	err := &event.FailureError{Cause: fsPermanent{msg: "bad"}, Reason: event.ReasonTerminal, Attempts: 1}

	got := fsSettle(context.Background(), event.Delivery{Count: 1, Max: 1, Subject: "s"}, event.SettleInput{
		Evt: evt, Handler: "fs-dropped", Err: err, OnErr: bus.fn,
	})

	if got.Disposition != event.DispositionTerminal || got.Outcome != event.OutcomeDropped {
		t.Fatalf("got %v / %q, want terminal / dropped", got.Disposition, got.Outcome)
	}
	if calls := bus.calls(); len(calls) != 1 || calls[0] != error(err) {
		t.Errorf("bus OnErr calls = %v, want the identical error once", calls)
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
		bus := &fsBusErr{}
		pub := &fsPublisher{failUntil: -1}
		got := fsSettle(context.Background(), event.Delivery{Count: 1, Max: 3}, event.SettleInput{
			Evt: &fsEvent{}, Handler: "fs-pf-nonfinal", Err: chainErr, Publisher: pub, OnErr: bus.fn,
		})
		if got.Disposition != event.DispositionRetryDelayed {
			t.Fatalf("Disposition = %v, want retry_delayed", got.Disposition)
		}
		if pub.calls.Load() != 1 {
			t.Errorf("publish calls = %d, want 1", pub.calls.Load())
		}
		if len(bus.calls()) != 0 {
			t.Errorf("OnErr called %d times on a non-final delivery, want 0", len(bus.calls()))
		}
		recs := logs.forHandler("fs-pf-nonfinal")
		if len(recs) != 1 || recs[0].level != slog.LevelError {
			t.Errorf("logs = %+v, want one Error record", recs)
		}
	})

	t.Run("final", func(t *testing.T) {
		fsCaptureLogs(t)
		evt := &fsEvent{}
		bus := &fsBusErr{}
		pub := &fsPublisher{failUntil: -1}
		got := fsSettle(context.Background(), event.Delivery{Count: 3, Max: 3}, event.SettleInput{
			Evt: evt, Handler: "fs-pf-final", Err: chainErr, Publisher: pub, OnErr: bus.fn,
		})
		if got.Disposition != event.DispositionRetryDelayed || got.Outcome != event.OutcomeDLQPublishFailed {
			t.Fatalf("got %v / %q, want retry_delayed / dlq_publish_failed", got.Disposition, got.Outcome)
		}
		if pub.calls.Load() != 3 {
			t.Errorf("publish calls = %d, want 3", pub.calls.Load())
		}
		for name, calls := range map[string][]error{"bus": bus.calls(), "event": evt.calls()} {
			if len(calls) != 1 || !errors.Is(calls[0], event.ErrDeadLetterFailed) || !errors.Is(calls[0], cause) {
				t.Errorf("%s OnErr calls = %v, want one error matching ErrDeadLetterFailed and the cause", name, calls)
			}
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
			bus := &fsBusErr{}
			dlq := event.NewMemoryDLQ()
			got := fsSettle(context.Background(), c.d, event.SettleInput{
				Evt: &fsEvent{}, Handler: "fs-chain-dlf", Err: failed, Publisher: dlq, OnErr: bus.fn,
			})
			if got.Disposition != event.DispositionRetryDelayed {
				t.Errorf("Disposition = %v, want retry_delayed", got.Disposition)
			}
			if dlq.Len() != 0 {
				t.Errorf("published %d records, want 0 (the chain already tried)", dlq.Len())
			}
			if n := len(bus.calls()); n != c.wantReport {
				t.Errorf("OnErr called %d times, want %d", n, c.wantReport)
			}
		})
	}
}

func TestSettle_InterruptedNeverDeadLettered(t *testing.T) {
	sub, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("non-final", func(t *testing.T) {
		fsCaptureLogs(t)
		bus := &fsBusErr{}
		dlq := event.NewMemoryDLQ()
		got := fsSettle(context.Background(), event.Delivery{Count: 1, Max: 3}, event.SettleInput{
			SubscriptionCtx: sub, Evt: &fsEvent{}, Handler: "fs-int", Err: context.Canceled, Publisher: dlq, OnErr: bus.fn,
		})
		if got.Disposition != event.DispositionRetry || got.Outcome != "" {
			t.Errorf("got %v / %q, want retry / no outcome", got.Disposition, got.Outcome)
		}
		if dlq.Len() != 0 || len(bus.calls()) != 0 {
			t.Errorf("published %d, reported %d; want 0 and 0", dlq.Len(), len(bus.calls()))
		}
	})

	t.Run("final", func(t *testing.T) {
		fsCaptureLogs(t)
		evt := &fsEvent{}
		bus := &fsBusErr{}
		dlq := event.NewMemoryDLQ()
		got := fsSettle(context.Background(), event.Delivery{Count: 3, Max: 3}, event.SettleInput{
			SubscriptionCtx: sub, Evt: evt, Handler: "fs-int", Err: context.Canceled, Publisher: dlq, OnErr: bus.fn,
		})
		if got.Disposition != event.DispositionRetry || got.Outcome != event.OutcomeInterrupted {
			t.Errorf("got %v / %q, want retry / interrupted", got.Disposition, got.Outcome)
		}
		if dlq.Len() != 0 {
			t.Errorf("published %d records, want 0 (never dead-lettered, D-07)", dlq.Len())
		}
		if len(bus.calls()) != 1 || len(evt.calls()) != 1 {
			t.Errorf("reported bus %d, event %d; want 1 and 1", len(bus.calls()), len(evt.calls()))
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
	bus := &fsBusErr{}
	dlq := event.NewMemoryDLQ()
	got := fsSettle(context.Background(), event.Delivery{Count: 3, Max: 3}, event.SettleInput{
		Evt: &fsEvent{}, Handler: "fs-dup", Err: event.ErrDuplicate, Publisher: dlq, OnErr: bus.fn,
	})
	if got.Disposition != event.DispositionAck {
		t.Errorf("Disposition = %v, want ack", got.Disposition)
	}
	if dlq.Len() != 0 || len(bus.calls()) != 0 || len(logs.forHandler("fs-dup")) != 0 {
		t.Errorf("published %d, reported %d, logged %d; want all 0", dlq.Len(), len(bus.calls()), len(logs.forHandler("fs-dup")))
	}
}

func TestSettle_DecodeFailureEvtNil(t *testing.T) {
	logs := fsCaptureLogs(t)
	bus := &fsBusErr{}
	dlq := event.NewMemoryDLQ()
	raw := []byte(`{"name":"nope"}`)
	got := fsSettle(context.Background(), event.Delivery{Count: 1, Max: 1, Subject: "nope", Raw: raw}, event.SettleInput{
		Evt: nil, Handler: "fs-decode", Err: &event.DecodeError{Name: "nope", Err: event.ErrUnknownEvent},
		Publisher: dlq, OnErr: bus.fn,
	})
	if got.Disposition != event.DispositionTerminal || got.Outcome != event.OutcomeDeadLettered {
		t.Fatalf("got %v / %q, want terminal / dead_lettered", got.Disposition, got.Outcome)
	}
	recs := dlq.Records()
	if len(recs) != 1 || recs[0].Evt != nil || recs[0].Reason != event.ReasonDecodeUnknownEvent || string(recs[0].Raw) != string(raw) {
		t.Errorf("records = %+v, want one with nil Evt, reason decode_unknown_event and the raw bytes", recs)
	}
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if len(bus.errs) != 1 || bus.evts[0] != nil {
		t.Errorf("bus OnErr calls = %d (evt %v), want 1 with a nil event", len(bus.errs), bus.evts)
	}
	if len(logs.forHandler("fs-decode")) == 0 {
		t.Error("decode failure was not logged")
	}
}

func TestSettle_UnknownVersionRetriedThenDeadLettered(t *testing.T) {
	err := &event.DecodeError{Name: "orders.created", Version: 9, Err: event.ErrUnknownVersion}

	t.Run("non-final retries with a warning", func(t *testing.T) {
		logs := fsCaptureLogs(t)
		dlq := event.NewMemoryDLQ()
		got := fsSettle(context.Background(), event.Delivery{Count: 1, Max: 3}, event.SettleInput{
			Handler: "fs-uv-nonfinal", Err: err, Publisher: dlq,
		})
		if got.Disposition != event.DispositionRetryDelayed || dlq.Len() != 0 {
			t.Errorf("got %v with %d records, want retry_delayed and 0", got.Disposition, dlq.Len())
		}
		recs := logs.forHandler("fs-uv-nonfinal")
		if len(recs) != 1 || recs[0].level != slog.LevelWarn {
			t.Errorf("logs = %+v, want one Warn record", recs)
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
