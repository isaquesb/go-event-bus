package event_test

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
)

// obLogCapture is a slog.Handler that keeps every record it receives.
type obLogCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *obLogCapture) Enabled(context.Context, slog.Level) bool { return true }
func (h *obLogCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}
func (h *obLogCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *obLogCapture) WithGroup(string) slog.Handler      { return h }

func (h *obLogCapture) panicRecords() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []slog.Record
	for _, r := range h.records {
		if r.Level == slog.LevelError && r.Message == "eventbus: observer panic recovered" {
			out = append(out, r)
		}
	}
	return out
}

// obCaptureSlog installs a capturing default logger and restores the previous
// one when the test ends.
func obCaptureSlog(t *testing.T) *obLogCapture {
	t.Helper()
	prev := slog.Default()
	h := &obLogCapture{}
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return h
}

var obAllMethods = []string{
	"RetryAttempt", "BreakerTransition", "Failure", "DeadLettered", "HandlerDone",
	"DecodeFailed", "DLQPublishFailed", "StoreError", "IdempotencyStale", "InternalError",
}

// obEmitAll calls the ten Observer methods in order with distinct payloads.
func obEmitAll(ctx context.Context, o event.Observer) {
	at := time.Unix(1700000000, 0).UTC()
	o.RetryAttempt(ctx, event.RetryAttemptInfo{Handler: "h1", Attempt: 1, Class: event.AttemptTransient, At: at})
	o.BreakerTransition(ctx, event.BreakerTransition{Handler: "h2", From: event.BreakerClosed, To: event.BreakerOpen, Seq: 7})
	o.Failure(ctx, event.FailureContext{Handler: "h3", Outcome: event.OutcomeDropped, Attempts: 3})
	o.DeadLettered(ctx, event.DeadLetterInfo{Handler: "h4", Reason: event.ReasonTerminal, Size: 42})
	o.HandlerDone(ctx, event.HandlerResult{Handler: "h5", Outcome: event.OutcomeOK, Duration: time.Second})
	o.DecodeFailed(ctx, event.DecodeFailure{Handler: "h6", Subject: "s6", Final: true})
	o.DLQPublishFailed(ctx, event.DLQPublishFailure{Handler: "h7", Err: errors.New("publish")})
	o.StoreError(ctx, event.StoreErrorInfo{Handler: "h8", Store: "idempotency", Op: "begin"})
	o.IdempotencyStale(ctx, event.IdempotencyStaleInfo{Handler: "h9", Age: time.Minute})
	o.InternalError(ctx, event.InternalErrorInfo{Component: event.ComponentTransport, Op: event.OpAck})
}

func TestObserver_MultiFanOutAndPanicIsolation(t *testing.T) {
	logs := obCaptureSlog(t)
	rec1, rec2 := &obsRecorder{}, &obsRecorder{}
	ctx := event.WithObserver(context.Background(), event.MultiObserver(rec1, obPanicObserver{}, rec2))
	obs := event.ObserverFromContext(ctx)

	obEmitAll(ctx, obs)

	for name, rec := range map[string]*obsRecorder{"rec1": rec1, "rec2": rec2} {
		if got := rec.Calls(); !reflect.DeepEqual(got, obAllMethods) {
			t.Fatalf("%s calls = %v, want %v", name, got, obAllMethods)
		}
	}
	if got := rec1.Retries(); len(got) != 1 || got[0].Handler != "h1" || got[0].Attempt != 1 {
		t.Fatalf("RetryAttempt payload = %+v", got)
	}
	if got := rec2.Transitions(); len(got) != 1 || got[0].Seq != 7 || got[0].To != event.BreakerOpen {
		t.Fatalf("BreakerTransition payload = %+v", got)
	}
	if got := rec1.Failures(); len(got) != 1 || got[0].Outcome != event.OutcomeDropped || got[0].Attempts != 3 {
		t.Fatalf("Failure payload = %+v", got)
	}
	if got := rec2.DeadLetters(); len(got) != 1 || got[0].Size != 42 {
		t.Fatalf("DeadLettered payload = %+v", got)
	}
	if got := rec1.HandlerDones(); len(got) != 1 || got[0].Duration != time.Second {
		t.Fatalf("HandlerDone payload = %+v", got)
	}
	if got := rec2.DecodeFailures(); len(got) != 1 || !got[0].Final || got[0].Subject != "s6" {
		t.Fatalf("DecodeFailed payload = %+v", got)
	}
	if got := rec1.DLQPublishFailures(); len(got) != 1 || got[0].Err == nil {
		t.Fatalf("DLQPublishFailed payload = %+v", got)
	}
	if got := rec2.StoreErrors(); len(got) != 1 || got[0].Store != "idempotency" {
		t.Fatalf("StoreError payload = %+v", got)
	}
	if got := rec1.Stales(); len(got) != 1 || got[0].Age != time.Minute {
		t.Fatalf("IdempotencyStale payload = %+v", got)
	}
	if got := rec2.InternalErrors(); len(got) != 1 || got[0].Op != event.OpAck {
		t.Fatalf("InternalError payload = %+v", got)
	}
	for _, c := range rec1.All() {
		if c.Ctx != ctx {
			t.Fatalf("%s received a different context than the one passed", c.Method)
		}
	}

	if got := len(logs.panicRecords()); got != 10 {
		t.Fatalf("panic log records = %d, want 10 (one per method)", got)
	}
	seen := map[string]bool{}
	for _, r := range logs.panicRecords() {
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "method" {
				seen[a.Value.String()] = true
			}
			if a.Key == "observer" && a.Value.String() != "event_test.obPanicObserver" {
				t.Errorf("observer attribute = %q", a.Value.String())
			}
			return true
		})
	}
	for _, m := range obAllMethods {
		if !seen[m] {
			t.Errorf("no panic record for method %s", m)
		}
	}

	obEmitAll(ctx, obs)
	if got := len(logs.panicRecords()); got != 10 {
		t.Fatalf("panic log records after the second round = %d, want still 10", got)
	}
	if got := len(rec1.Calls()); got != 20 {
		t.Fatalf("rec1 calls after two rounds = %d, want 20", got)
	}
}

// obPartial overrides Failure only; every other method comes from NopObserver.
type obPartial struct {
	event.NopObserver
	failures int
}

func (p *obPartial) Failure(context.Context, event.FailureContext) { p.failures++ }

func TestObserver_NopEmbedding(t *testing.T) {
	var _ event.Observer = event.NopObserver{}
	var _ event.Observer = &event.NopObserver{}
	var _ event.Observer = &obPartial{}

	p := &obPartial{}
	var o event.Observer = p
	obEmitAll(context.Background(), o)
	if p.failures != 1 {
		t.Fatalf("Failure called %d times, want 1", p.failures)
	}
}

func TestObserver_ContextFallback(t *testing.T) {
	if got := event.ObserverFromContext(context.Background()); got != (event.NopObserver{}) {
		t.Fatalf("empty ctx observer = %#v, want NopObserver{}", got)
	}
	ctx := event.WithObserver(context.Background(), nil)
	if got := event.ObserverFromContext(ctx); got != (event.NopObserver{}) {
		t.Fatalf("nil observer = %#v, want NopObserver{}", got)
	}
	rec := &obsRecorder{}
	if got := event.ObserverFromContext(event.WithObserver(context.Background(), rec)); got != event.Observer(rec) {
		t.Fatalf("observer = %#v, want the installed recorder", got)
	}
}

func TestMultiObserver_FlattenAndSkip(t *testing.T) {
	if got := event.MultiObserver(); got != (event.NopObserver{}) {
		t.Fatalf("MultiObserver() = %#v, want NopObserver{}", got)
	}
	if got := event.MultiObserver(nil, event.NopObserver{}, &event.NopObserver{}); got != (event.NopObserver{}) {
		t.Fatalf("MultiObserver(nil, Nop...) = %#v, want NopObserver{}", got)
	}

	rec1, rec2 := &obsRecorder{}, &obsRecorder{}
	inner := event.MultiObserver(rec1)
	outer := event.MultiObserver(inner, nil, event.NopObserver{}, rec2)
	outer.Failure(context.Background(), event.FailureContext{Handler: "h"})
	if got := rec1.Calls(); !reflect.DeepEqual(got, []string{"Failure"}) {
		t.Fatalf("rec1 calls = %v, want exactly one Failure", got)
	}
	if got := rec2.Calls(); !reflect.DeepEqual(got, []string{"Failure"}) {
		t.Fatalf("rec2 calls = %v, want exactly one Failure", got)
	}
}

func TestMultiObserver_SingleChildIsIsolated(t *testing.T) {
	logs := obCaptureSlog(t)
	o := event.MultiObserver(obPanicObserver{})
	o.Failure(context.Background(), event.FailureContext{})
	if got := len(logs.panicRecords()); got != 1 {
		t.Fatalf("panic log records = %d, want 1", got)
	}
}
