package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	natsgo "github.com/nats-io/nats.go"
)

// noRecorder is the recording Observer of the NATS tests: it embeds
// event.NopObserver and records the signals the bus emits, under a mutex.
type noRecorder struct {
	event.NopObserver

	mu        sync.Mutex
	fails     []event.FailureContext
	dones     []event.HandlerResult
	decodes   []event.DecodeFailure
	dlqFails  []event.DLQPublishFailure
	internals []event.InternalErrorInfo
}

var _ event.Observer = (*noRecorder)(nil)

func (r *noRecorder) Failure(_ context.Context, f event.FailureContext) {
	r.mu.Lock()
	r.fails = append(r.fails, f)
	r.mu.Unlock()
}

func (r *noRecorder) HandlerDone(_ context.Context, d event.HandlerResult) {
	r.mu.Lock()
	r.dones = append(r.dones, d)
	r.mu.Unlock()
}

func (r *noRecorder) DecodeFailed(_ context.Context, d event.DecodeFailure) {
	r.mu.Lock()
	r.decodes = append(r.decodes, d)
	r.mu.Unlock()
}

func (r *noRecorder) DLQPublishFailed(_ context.Context, d event.DLQPublishFailure) {
	r.mu.Lock()
	r.dlqFails = append(r.dlqFails, d)
	r.mu.Unlock()
}

func (r *noRecorder) InternalError(_ context.Context, i event.InternalErrorInfo) {
	r.mu.Lock()
	r.internals = append(r.internals, i)
	r.mu.Unlock()
}

func (r *noRecorder) failures() []event.FailureContext {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]event.FailureContext(nil), r.fails...)
}

func (r *noRecorder) handlerDones() []event.HandlerResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]event.HandlerResult(nil), r.dones...)
}

func (r *noRecorder) decodeFailures() []event.DecodeFailure {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]event.DecodeFailure(nil), r.decodes...)
}

func (r *noRecorder) dlqPublishFailures() []event.DLQPublishFailure {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]event.DLQPublishFailure(nil), r.dlqFails...)
}

func (r *noRecorder) internalErrors() []event.InternalErrorInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]event.InternalErrorInfo(nil), r.internals...)
}

// noFailureAt returns the event and the cause of the i-th Failure, failing the
// test when fewer were recorded.
func noFailureAt(t *testing.T, r *noRecorder, i int) (event.Event, error) {
	t.Helper()
	fails := r.failures()
	if i >= len(fails) {
		t.Fatalf("Failure was called %d times, want index %d", len(fails), i)
	}
	return fails[i].Event, fails[i].Cause
}

// noPanicReported reports whether an InternalError with Op panic carries a
// *event.PanicError whose value prints as text.
func noPanicReported(r *noRecorder, text string) bool {
	for _, in := range r.internalErrors() {
		var pe *event.PanicError
		if in.Op == event.OpPanic && errors.As(in.Err, &pe) && fmt.Sprint(pe.Value) == text {
			return true
		}
	}
	return false
}

// noTerminal is a terminal handler error.
type noTerminal struct{ msg string }

func (e *noTerminal) Error() string  { return e.msg }
func (e *noTerminal) Terminal() bool { return true }

// --- tracer ---

// TestNatsSubscribe_FailureReachesObserverOnce is the tracer of plan 10-10: a
// failing NATS Core subscription reports once to the bus Observer through the
// observer and FailureScope the callback installs before Decode.
func TestNatsSubscribe_FailureReachesObserverOnce(t *testing.T) {
	conn := newFsLoopConn()
	reg := newMdRegistry("no.order")
	dlq := event.NewMemoryDLQ()
	rec := &noRecorder{}
	bus := &Bus{nc: conn, reg: reg, timeout: time.Second, dlq: dlq, observer: rec}

	boom := &noTerminal{msg: "no: boom"}
	if err := bus.Subscribe(context.Background(), "no.order", func(context.Context, event.Event) error {
		return boom
	}, event.WithHandlerName("h")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	raw := fsRequestBytes(t, reg, "no.order")
	conn.deliver(t, "no.order", raw)

	fails := rec.failures()
	if len(fails) != 1 {
		t.Fatalf("Failure calls = %d, want 1", len(fails))
	}
	f := fails[0]
	if f.Outcome != event.OutcomeDeadLettered {
		t.Errorf("Outcome = %q, want %q", f.Outcome, event.OutcomeDeadLettered)
	}
	if f.Handler != "h" {
		t.Errorf("Handler = %q, want h", f.Handler)
	}
	if f.Event == nil {
		t.Error("Event = nil, want the decoded event")
	}
	if f.Delivery.System != "nats" {
		t.Errorf("Delivery.System = %q, want nats", f.Delivery.System)
	}
	if f.Delivery.Count != 1 {
		t.Errorf("Delivery.Count = %d, want 1", f.Delivery.Count)
	}
	if f.Delivery.Raw != nil {
		t.Errorf("Delivery.Raw = %q, want nil (no payload in signals)", f.Delivery.Raw)
	}
	if !errors.Is(f.Cause, event.ErrDeadLettered) || !errors.Is(f.Cause, boom) {
		t.Errorf("Cause = %v, want ErrDeadLettered wrapping the handler error", f.Cause)
	}

	dones := rec.handlerDones()
	if len(dones) != 1 {
		t.Fatalf("HandlerDone calls = %d, want 1", len(dones))
	}
	if dones[0].Outcome != event.OutcomeDeadLettered {
		t.Errorf("HandlerDone outcome = %q, want %q", dones[0].Outcome, event.OutcomeDeadLettered)
	}
	if n := dlq.Len(); n != 1 {
		t.Errorf("DLQ records = %d, want 1", n)
	}
}

// --- exactly-once matrix ---

// noWant is what one matrix row expects from the recorder.
type noWant struct {
	fails      int
	outcome    event.Outcome // of the Failure, when fails > 0
	reason     string        // of the Failure, when non-empty
	hasEvent   bool          // the Failure carries the decoded event
	handler    string        // of the Failure, when fails > 0
	dones      int
	doneResult event.Outcome // of the HandlerDone, when dones > 0
	decodes    int
	dlqFails   int
	replyCode  string // Eventbus-Error reason of the sync reply, "" for none
}

type noRow struct {
	name    string
	reg     func() event.Registry
	dlq     event.DLQPublisher
	inv     event.Invoker
	handler event.HandleFn
	want    noWant
}

// isFlowControlReason reports whether reason is one of the flow-control reasons
// whose HandlerDone is deferred to the settled outcome on a final delivery.
func isFlowControlReason(reason string) bool {
	switch reason {
	case event.ReasonRateLimited, event.ReasonCircuitOpen, event.ReasonInProgress:
		return true
	}
	return false
}

func noOK(context.Context, event.Event) error { return nil }

func noPanic(context.Context, event.Event) error { panic("no: handler boom") }

func noTerminalHandler(context.Context, event.Event) error { return &noTerminal{msg: "no: terminal"} }

func noMatrixRows() []noRow {
	rows := []noRow{
		{
			name: "success", handler: noOK,
			want: noWant{dones: 1, doneResult: event.OutcomeOK},
		},
		{
			name: "duplicate", handler: noOK, dlq: event.NewMemoryDLQ(), inv: fsInvoker{err: event.ErrDuplicate},
			want: noWant{dones: 1, doneResult: event.OutcomeDuplicate, replyCode: event.ReasonDuplicate},
		},
		{
			name: "dead lettered", handler: noTerminalHandler, dlq: event.NewMemoryDLQ(),
			want: noWant{
				fails: 1, outcome: event.OutcomeDeadLettered, reason: event.ReasonTerminal, hasEvent: true, handler: "h",
				dones: 1, doneResult: event.OutcomeDeadLettered, replyCode: event.ReasonTerminal,
			},
		},
		{
			name: "dropped without publisher", handler: noTerminalHandler,
			want: noWant{
				fails: 1, outcome: event.OutcomeDropped, reason: event.ReasonTerminal, hasEvent: true, handler: "h",
				dones: 1, doneResult: event.OutcomeDropped, replyCode: event.ReasonTerminal,
			},
		},
		{
			name: "dlq publish failed", handler: noTerminalHandler, dlq: &fsFailingPublisher{},
			want: noWant{
				fails: 1, outcome: event.OutcomeDLQPublishFailed, hasEvent: true, handler: "h",
				dones: 1, doneResult: event.OutcomeFailed, dlqFails: 1, replyCode: event.ReasonTerminal,
			},
		},
		{
			name: "panic", handler: noPanic, dlq: event.NewMemoryDLQ(),
			want: noWant{
				fails: 1, outcome: event.OutcomeDeadLettered, reason: event.ReasonPanic, hasEvent: true, handler: "h",
				dones: 1, doneResult: event.OutcomeDeadLettered, replyCode: event.ReasonPanic,
			},
		},
		{
			name: "decode failed", handler: noOK, dlq: event.NewMemoryDLQ(),
			reg: func() event.Registry { return fsDecodeFailRegistry{} },
			want: noWant{
				fails: 1, outcome: event.OutcomeDecodeFailed, reason: event.ReasonDecodeUnknownEvent, handler: "h",
				decodes: 1, replyCode: event.ReasonDecodeUnknownEvent,
			},
		},
	}
	return append(rows, noFlowControlRows()...)
}

// noFlowControlRows pins D-22 on NATS Core, where every delivery is the final
// one: a rate_limited, circuit_open or in_progress handler result is lost, so
// the single Failure and the HandlerDone carry the same settled outcome
// (dropped without a DLQ publisher, dead_lettered with one).
func noFlowControlRows() []noRow {
	var rows []noRow
	for _, fc := range []struct {
		name   string
		err    error
		reason string
	}{
		{"rate limited", event.ErrRateLimited, event.ReasonRateLimited},
		{"circuit open", event.ErrCircuitOpen, event.ReasonCircuitOpen},
		{"in progress", event.ErrInProgress, event.ReasonInProgress},
	} {
		rows = append(rows,
			noRow{
				name: fc.name + " dropped without publisher", handler: noOK, inv: fsInvoker{err: fc.err},
				want: noWant{
					fails: 1, outcome: event.OutcomeDropped, reason: fc.reason, hasEvent: true, handler: "h",
					dones: 1, doneResult: event.OutcomeDropped, replyCode: fc.reason,
				},
			},
			noRow{
				name: fc.name + " dead lettered", handler: noOK, dlq: event.NewMemoryDLQ(), inv: fsInvoker{err: fc.err},
				want: noWant{
					fails: 1, outcome: event.OutcomeDeadLettered, reason: fc.reason, hasEvent: true, handler: "h",
					dones: 1, doneResult: event.OutcomeDeadLettered, replyCode: fc.reason,
				},
			},
		)
	}
	return rows
}

// noRun delivers one message for row through Subscribe (sync false) or
// SubscribeSync (sync true) and returns the recorder, the error of EmitSync and
// the Eventbus-Error code of the reply.
func noRun(t *testing.T, row noRow, sync bool) (*noRecorder, string) {
	t.Helper()
	conn := newFsLoopConn()
	var reg event.Registry = newMdRegistry("no.order")
	if row.reg != nil {
		reg = row.reg()
	}
	rec := &noRecorder{}
	bus := &Bus{nc: conn, reg: reg, timeout: time.Second, dlq: row.dlq, invoker: row.inv, observer: rec}

	if sync {
		if err := bus.SubscribeSync(context.Background(), "no.order", row.handler, event.WithHandlerName("h")); err != nil {
			t.Fatalf("SubscribeSync: %v", err)
		}
		_, _ = bus.EmitSync(context.Background(), &mdEvent{name: "no.order", EventID: "no-1"})
		conn.mu.Lock()
		defer conn.mu.Unlock()
		if len(conn.replies) == 0 {
			return rec, ""
		}
		return rec, conn.replies[len(conn.replies)-1].Header.Get(HeaderError)
	}

	if err := bus.Subscribe(context.Background(), "no.order", row.handler, event.WithHandlerName("h")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	data := []byte("garbage-bytes")
	if m, ok := reg.(*mdRegistry); ok {
		data = fsRequestBytes(t, m, "no.order")
	}
	conn.deliver(t, "no.order", data)
	return rec, ""
}

func TestNatsObserver_FailureMatrix(t *testing.T) {
	for _, row := range noMatrixRows() {
		for _, mode := range []struct {
			name string
			sync bool
		}{{"Subscribe", false}, {"SubscribeSync", true}} {
			t.Run(row.name+"/"+mode.name, func(t *testing.T) {
				rec, code := noRun(t, row, mode.sync)
				w := row.want

				fails := rec.failures()
				if len(fails) != w.fails {
					t.Fatalf("Failure calls = %d, want %d (%+v)", len(fails), w.fails, fails)
				}
				if w.fails == 1 {
					f := fails[0]
					if f.Outcome != w.outcome {
						t.Errorf("Failure outcome = %q, want %q", f.Outcome, w.outcome)
					}
					if w.reason != "" && f.Reason != w.reason {
						t.Errorf("Failure reason = %q, want %q", f.Reason, w.reason)
					}
					if (f.Event != nil) != w.hasEvent {
						t.Errorf("Failure event = %v, want non-nil %v", f.Event, w.hasEvent)
					}
					if f.Handler != w.handler {
						t.Errorf("Failure handler = %q, want %q", f.Handler, w.handler)
					}
					if f.Cause == nil {
						t.Error("Failure cause = nil")
					}
					if f.Delivery.System != "nats" || f.Delivery.Raw != nil {
						t.Errorf("Failure delivery system/raw = %q/%q, want nats/nil", f.Delivery.System, f.Delivery.Raw)
					}
				}

				dones := rec.handlerDones()
				if len(dones) != w.dones {
					t.Fatalf("HandlerDone calls = %d, want %d (%+v)", len(dones), w.dones, dones)
				}
				if w.dones == 1 && dones[0].Outcome != w.doneResult {
					t.Errorf("HandlerDone outcome = %q, want %q", dones[0].Outcome, w.doneResult)
				}
				// D-22: a flow-control loss on the final (only) delivery reports the
				// same settled outcome on the Failure and on the HandlerDone.
				if w.fails == 1 && w.dones == 1 && isFlowControlReason(w.reason) && dones[0].Outcome != fails[0].Outcome {
					t.Errorf("HandlerDone outcome = %q, Failure outcome = %q, want them equal (D-22)", dones[0].Outcome, fails[0].Outcome)
				}

				decodes := rec.decodeFailures()
				if len(decodes) != w.decodes {
					t.Fatalf("DecodeFailed calls = %d, want %d", len(decodes), w.decodes)
				}
				if w.decodes == 1 && (!decodes[0].Final || decodes[0].Delivery.Raw != nil) {
					t.Errorf("DecodeFailed final/raw = %v/%q, want true/nil", decodes[0].Final, decodes[0].Delivery.Raw)
				}

				dlqFails := rec.dlqPublishFailures()
				if len(dlqFails) != w.dlqFails {
					t.Fatalf("DLQPublishFailed calls = %d, want %d", len(dlqFails), w.dlqFails)
				}
				if w.dlqFails == 1 && !dlqFails[0].Final {
					t.Error("DLQPublishFailed.Final = false, want true on NATS Core")
				}

				if mode.sync && code != w.replyCode {
					t.Errorf("sync reply %s = %q, want %q", HeaderError, code, w.replyCode)
				}
			})
		}
	}
}

// TestNatsObserver_InterruptedDeliveryReportedOnce covers a message delivered
// after the forced shutdown cancel: one interrupted Failure with a nil event,
// no HandlerDone, on Subscribe and SubscribeSync.
func TestNatsObserver_InterruptedDeliveryReportedOnce(t *testing.T) {
	for _, sync := range []bool{false, true} {
		name := "Subscribe"
		if sync {
			name = "SubscribeSync"
		}
		t.Run(name, func(t *testing.T) {
			conn := newFsLoopConn()
			reg := newMdRegistry("no.late")
			rec := &noRecorder{}
			dlq := event.NewMemoryDLQ()
			bus := &Bus{nc: conn, reg: reg, timeout: time.Second, dlq: dlq, observer: rec}
			handler := func(context.Context, event.Event) error {
				t.Error("handler ran for a rejected delivery")
				return nil
			}
			var err error
			if sync {
				err = bus.SubscribeSync(context.Background(), "no.late", handler, event.WithHandlerName("h"))
			} else {
				err = bus.Subscribe(context.Background(), "no.late", handler, event.WithHandlerName("h"))
			}
			if err != nil {
				t.Fatalf("subscribe: %v", err)
			}
			bus.life.BeginShutdown()
			bus.life.Interrupt()

			conn.mu.Lock()
			cb := conn.handlers["no.late"]
			conn.mu.Unlock()
			msg := &natsgo.Msg{Subject: "no.late", Data: fsRequestBytes(t, reg, "no.late")}
			if sync {
				msg.Reply = fsInbox
			}
			cb(msg)

			fails := rec.failures()
			if len(fails) != 1 {
				t.Fatalf("Failure calls = %d, want 1", len(fails))
			}
			if fails[0].Outcome != event.OutcomeInterrupted || fails[0].Event != nil {
				t.Errorf("Failure outcome/event = %q/%v, want interrupted/nil", fails[0].Outcome, fails[0].Event)
			}
			if !errors.Is(fails[0].Cause, event.ErrShutdownInterrupted) {
				t.Errorf("Failure cause = %v, want ErrShutdownInterrupted", fails[0].Cause)
			}
			if n := len(rec.handlerDones()); n != 0 {
				t.Errorf("HandlerDone calls = %d, want 0", n)
			}
			if n := dlq.Len(); n != 0 {
				t.Errorf("DLQ records = %d, want 0 (an interruption is never dead-lettered)", n)
			}
			if sync {
				if v := conn.lastReply(t).Header.Get(HeaderError); v != event.ReasonInterrupted {
					t.Errorf("reply %s = %q, want %q", HeaderError, v, event.ReasonInterrupted)
				}
			}
		})
	}
}

// --- zero-configuration logging ---

// noLogs is a slog.Handler capturing every record message.
type noLogs struct {
	mu   sync.Mutex
	msgs []string
}

func (l *noLogs) Enabled(context.Context, slog.Level) bool { return true }

func (l *noLogs) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	l.msgs = append(l.msgs, r.Message)
	l.mu.Unlock()
	return nil
}

func (l *noLogs) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *noLogs) WithGroup(string) slog.Handler      { return l }

func (l *noLogs) count(msg string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, m := range l.msgs {
		if strings.Contains(m, msg) {
			n++
		}
	}
	return n
}

// TestNatsBus_DefaultObserverLogs builds the bus through the code NewNatsBus
// uses with empty BusOptions: a dropped message is logged once by the default
// SlogObserver.
func TestNatsBus_DefaultObserverLogs(t *testing.T) {
	logs := &noLogs{}
	prev := slog.Default()
	slog.SetDefault(slog.New(logs))
	t.Cleanup(func() { slog.SetDefault(prev) })

	conn := newFsLoopConn()
	reg := newMdRegistry("no.order")
	bus := newBus(conn, reg, BusOptions{})
	if err := bus.Subscribe(context.Background(), "no.order", noTerminalHandler, event.WithHandlerName("h")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	conn.deliver(t, "no.order", fsRequestBytes(t, reg, "no.order"))

	if n := logs.count("eventbus: message failed"); n != 1 {
		t.Errorf("failure log records = %d, want 1 (%v)", n, logs.msgs)
	}
}

// TestNatsBus_NopObserverSilences confirms event.NopObserver{} silences the
// default logging.
func TestNatsBus_NopObserverSilences(t *testing.T) {
	logs := &noLogs{}
	prev := slog.Default()
	slog.SetDefault(slog.New(logs))
	t.Cleanup(func() { slog.SetDefault(prev) })

	conn := newFsLoopConn()
	reg := newMdRegistry("no.order")
	bus := newBus(conn, reg, BusOptions{Observer: event.NopObserver{}})
	if err := bus.Subscribe(context.Background(), "no.order", noTerminalHandler); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	conn.deliver(t, "no.order", fsRequestBytes(t, reg, "no.order"))

	if n := logs.count("eventbus: message failed"); n != 0 {
		t.Errorf("failure log records = %d, want 0 with NopObserver", n)
	}
}
