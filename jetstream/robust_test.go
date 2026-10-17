package jetstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/nats-io/nats.go/jetstream"
)

// rbPanicInvoker is an Invoker that panics before calling next.
type rbPanicInvoker struct{}

func (rbPanicInvoker) Invoke(context.Context, event.Event, string, func(context.Context) error) error {
	panic("rb: invoker exploded")
}

// rbPolicy is the delivery policy of the panic tests: three deliveries.
func rbPolicy() deliveryPolicy {
	return deliveryPolicy{maxDeliver: 3, backOff: []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}}
}

// rbEmit emits an mdEvent named rb.order through bus and returns the payload.
func rbEmit(t *testing.T, bus *Bus, js *mdJS) []byte {
	t.Helper()
	if err := bus.Emit(context.Background(), &mdEvent{name: "rb.order"}, event.WithID("id-rb")); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	return js.payload(t, 0).data
}

func TestJetStreamBus_PanicDeadLetteredThenTermed(t *testing.T) {
	js := &mdJS{}
	dlq := event.NewMemoryDLQ()
	var errs dispErrCounter
	bus := newMdBus(js, newMdRegistry("rb.order"), BusOptions{DLQPublisher: dlq, OnErr: errs.onErr})
	payload := rbEmit(t, bus, js)

	msg := &mdMsg{data: payload, numDelivered: 1}
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		panic("rb: handler exploded")
	}, rbPolicy())(msg)

	got := msg.snapshot()
	if want := []string{"dead_lettered: panic"}; !slices.Equal(got.termReasons, want) {
		t.Errorf("termReasons = %q, want %q", got.termReasons, want)
	}
	if got.acks != 0 || got.naks != 0 || got.plainTerms != 0 || len(got.nakDelays) != 0 {
		t.Errorf("snapshot = %+v, want only a TermWithReason", got)
	}

	recs := dlq.Records()
	if len(recs) != 1 || recs[0].Reason != event.ReasonPanic {
		t.Fatalf("records = %+v, want one with reason panic", recs)
	}
	if stack := recs[0].FailureHeaders()[event.HeaderDLQPanicStack]; stack == "" {
		t.Error("DLQ record has no panic stack header")
	}

	if n := errs.calls.Load(); n != 1 {
		t.Fatalf("OnErr calls = %d, want 1", n)
	}
	last := errs.lastErr()
	if !errors.Is(last, event.ErrDeadLettered) || !errors.Is(last, event.ErrHandlerPanic) {
		t.Errorf("OnErr err = %v, want to match ErrDeadLettered and ErrHandlerPanic", last)
	}
}

func TestJetStreamBus_PanicWithoutPublisherDropped(t *testing.T) {
	js := &mdJS{}
	var errs dispErrCounter
	bus := newMdBus(js, newMdRegistry("rb.order"), BusOptions{OnErr: errs.onErr})
	payload := rbEmit(t, bus, js)

	msg := &mdMsg{data: payload, numDelivered: 1}
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		panic("rb: handler exploded")
	}, rbPolicy())(msg)

	got := msg.snapshot()
	if want := []string{"dropped: panic"}; !slices.Equal(got.termReasons, want) {
		t.Errorf("termReasons = %q, want %q", got.termReasons, want)
	}
	if got.acks != 0 || got.naks != 0 || got.plainTerms != 0 {
		t.Errorf("snapshot = %+v, want only a TermWithReason", got)
	}
	if n := errs.calls.Load(); n != 1 {
		t.Fatalf("OnErr calls = %d, want 1", n)
	}
	if last := errs.lastErr(); !errors.Is(last, event.ErrHandlerPanic) {
		t.Errorf("OnErr err = %v, want to match ErrHandlerPanic", last)
	}
}

func TestJetStreamBus_InvokerPanicRecovered(t *testing.T) {
	js := &mdJS{}
	bus := newMdBus(js, newMdRegistry("rb.order"), BusOptions{
		Invoker:      rbPanicInvoker{},
		DLQPublisher: event.NewMemoryDLQ(),
	})
	payload := rbEmit(t, bus, js)

	ran := false
	msg := &mdMsg{data: payload, numDelivered: 1}
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		ran = true
		return nil
	}, rbPolicy())(msg)

	if ran {
		t.Error("handler ran although the invoker panicked before next")
	}
	got := msg.snapshot()
	if len(got.termReasons) != 1 || !strings.HasSuffix(got.termReasons[0], "panic") {
		t.Errorf("termReasons = %q, want one reason ending in panic", got.termReasons)
	}
}

// --- shutdown fakes ---

// rbConsumeCtx is a jetstream.ConsumeContext that counts Drain and Stop and
// reports Closed only when the test calls finish.
type rbConsumeCtx struct {
	mu     sync.Mutex
	drains int
	stops  int
	closed chan struct{}
	once   sync.Once
}

func newRbConsumeCtx() *rbConsumeCtx { return &rbConsumeCtx{closed: make(chan struct{})} }

func (c *rbConsumeCtx) Drain() {
	c.mu.Lock()
	c.drains++
	c.mu.Unlock()
}

func (c *rbConsumeCtx) Stop() {
	c.mu.Lock()
	c.stops++
	c.mu.Unlock()
}

func (c *rbConsumeCtx) Closed() <-chan struct{} { return c.closed }
func (c *rbConsumeCtx) finish()                 { c.once.Do(func() { close(c.closed) }) }

func (c *rbConsumeCtx) counts() (drains, stops int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.drains, c.stops
}

// rbConsumer is a mockConsumer whose Consume captures the MessageHandler and
// returns the controllable rbConsumeCtx.
type rbConsumer struct {
	mockConsumer
	cc      *rbConsumeCtx
	handler jetstream.MessageHandler
}

func (c *rbConsumer) Consume(h jetstream.MessageHandler, _ ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	c.handler = h
	return c.cc, nil
}

// rbJS is an mdJS that hands out one rbConsumer.
type rbJS struct {
	mdJS
	cons *rbConsumer
}

func (j *rbJS) CreateOrUpdateConsumer(context.Context, string, jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	return j.cons, nil
}

// rbErrs records every OnErr call.
type rbErrs struct {
	mu   sync.Mutex
	evts []event.Event
	errs []error
}

func (r *rbErrs) onErr(_ context.Context, evt event.Event, err error, _ string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evts = append(r.evts, evt)
	r.errs = append(r.errs, err)
}

func (r *rbErrs) snapshot() ([]event.Event, []error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.evts), slices.Clone(r.errs)
}

// rbGate is a channel closed at most once.
type rbGate struct {
	ch   chan struct{}
	once sync.Once
}

func newRbGate() *rbGate { return &rbGate{ch: make(chan struct{})} }
func (g *rbGate) open()  { g.once.Do(func() { close(g.ch) }) }

// rbShutdownBus builds a bus over an rbJS, subscribes fn to rb.order with
// MaxDeliver 3 and emits one event. It returns the bus, the consumer, the js and
// the recorded payload of the emitted event.
func rbShutdownBus(t *testing.T, opts BusOptions, fn event.HandleFn) (*Bus, *rbConsumer, *rbJS, []byte) {
	t.Helper()
	cons := &rbConsumer{cc: newRbConsumeCtx()}
	js := &rbJS{cons: cons}
	bus := newMdBus(js, newMdRegistry("rb.order", "rb.child"), opts)
	err := bus.Subscribe(context.Background(), "rb.order", fn,
		event.WithStream("S"),
		event.WithConsumer("C"),
		event.WithMaxDeliver(3),
		event.WithBackOff([]time.Duration{time.Second}),
	)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Emit(context.Background(), &mdEvent{name: "rb.order"}, event.WithID("id-rb")); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	return bus, cons, js, js.payload(t, 0).data
}

// rbDeliver delivers msg through the captured MessageHandler from a goroutine
// and returns a channel closed when the handler returns.
func rbDeliver(cons *rbConsumer, msg *mdMsg) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		cons.handler(msg)
	}()
	return done
}

// rbShutdown runs bus.Shutdown(ctx) in a goroutine and returns its result channel.
func rbShutdown(bus *Bus, ctx context.Context) <-chan error {
	res := make(chan error, 1)
	go func() { res <- bus.Shutdown(ctx) }()
	return res
}

func TestJetStreamBus_ShutdownDrainsAndHandsBack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := newRbGate()
		defer gate.open()
		started := make(chan struct{}, 4)
		var runs int
		var mu sync.Mutex
		bus, cons, js, payload := rbShutdownBus(t, BusOptions{}, func(context.Context, event.Event) error {
			mu.Lock()
			runs++
			mu.Unlock()
			started <- struct{}{}
			<-gate.ch
			return nil
		})
		defer cons.cc.finish()

		first := &mdMsg{data: payload, numDelivered: 1}
		firstDone := rbDeliver(cons, first)
		synctest.Wait()
		<-started

		res := rbShutdown(bus, context.Background())
		synctest.Wait()
		select {
		case err := <-res:
			t.Fatalf("Shutdown returned %v while a handler was in flight", err)
		default:
		}
		if drains, _ := cons.cc.counts(); drains != 1 {
			t.Errorf("Drain calls = %d, want 1", drains)
		}

		if err := bus.Emit(context.Background(), &mdEvent{name: "rb.order"}); !errors.Is(err, event.ErrBusClosed) {
			t.Errorf("Emit while closing = %v, want ErrBusClosed", err)
		}
		err := bus.Subscribe(context.Background(), "rb.other", noopHandler,
			event.WithStream("S"), event.WithConsumer("C2"), event.WithMaxDeliver(3),
			event.WithBackOff([]time.Duration{time.Second}))
		if !errors.Is(err, event.ErrBusClosed) {
			t.Errorf("Subscribe while closing = %v, want ErrBusClosed", err)
		}

		// A message delivered while closing goes back to the server unstarted.
		second := &mdMsg{data: js.payload(t, 0).data, numDelivered: 1}
		<-rbDeliver(cons, second)
		if got := second.snapshot(); got.naks != 1 || got.acks != 0 || len(got.termReasons) != 0 {
			t.Errorf("second snapshot = %+v, want a single Nak", got)
		}
		mu.Lock()
		if runs != 1 {
			t.Errorf("handler runs = %d, want 1 (the hand-back never runs the handler)", runs)
		}
		mu.Unlock()

		gate.open()
		<-firstDone
		cons.cc.finish()
		if err := <-res; err != nil {
			t.Errorf("Shutdown = %v, want nil", err)
		}
		if got := first.snapshot(); got.acks != 1 {
			t.Errorf("first snapshot = %+v, want a single Ack", got)
		}
	})
}

func TestJetStreamBus_NestedEmitDuringDrainAccepted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := newRbGate()
		defer gate.open()
		started := make(chan struct{}, 1)
		nested := make(chan error, 1)
		var bus *Bus
		bus, cons, js, payload := rbShutdownBus(t, BusOptions{}, func(ctx context.Context, _ event.Event) error {
			started <- struct{}{}
			<-gate.ch
			nested <- bus.Emit(ctx, &mdEvent{name: "rb.child"})
			return nil
		})
		defer cons.cc.finish()

		msg := &mdMsg{data: payload, numDelivered: 1}
		done := rbDeliver(cons, msg)
		synctest.Wait()
		<-started

		res := rbShutdown(bus, context.Background())
		synctest.Wait()

		if err := bus.Emit(context.Background(), &mdEvent{name: "rb.child"}); !errors.Is(err, event.ErrBusClosed) {
			t.Errorf("external Emit during the drain = %v, want ErrBusClosed", err)
		}
		if n := js.count(); n != 1 {
			t.Fatalf("published = %d before the handler emits, want 1", n)
		}

		gate.open()
		if err := <-nested; err != nil {
			t.Errorf("nested Emit during the drain = %v, want nil", err)
		}
		<-done
		cons.cc.finish()
		if err := <-res; err != nil {
			t.Errorf("Shutdown = %v, want nil", err)
		}
		if n := js.count(); n != 2 {
			t.Errorf("published = %d, want 2 (the nested emit was accepted)", n)
		}
	})
}

func TestJetStreamBus_ShutdownDeadlineInterrupts(t *testing.T) {
	for _, tc := range []struct {
		name      string
		delivered uint64
		wantOnErr int
	}{
		{"non-final delivery", 1, 0},
		{"final delivery", 3, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var errs rbErrs
				dlq := event.NewMemoryDLQ()
				cause := make(chan error, 1)
				started := make(chan struct{}, 1)
				bus, cons, _, payload := rbShutdownBus(t, BusOptions{DLQPublisher: dlq, OnErr: errs.onErr},
					func(ctx context.Context, _ event.Event) error {
						started <- struct{}{}
						<-ctx.Done()
						cause <- context.Cause(ctx)
						return ctx.Err()
					})

				msg := &mdMsg{data: payload, numDelivered: tc.delivered}
				done := rbDeliver(cons, msg)
				synctest.Wait()
				<-started

				sctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				err := bus.Shutdown(sctx)
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Shutdown = %v, want DeadlineExceeded", err)
				}
				<-done
				if got := <-cause; !errors.Is(got, event.ErrShutdownInterrupted) {
					t.Errorf("handler cause = %v, want ErrShutdownInterrupted", got)
				}
				if _, stops := cons.cc.counts(); stops != 1 {
					t.Errorf("Stop calls = %d, want 1", stops)
				}
				if got := msg.snapshot(); got.naks != 1 || got.acks != 0 || got.plainTerms != 0 || len(got.termReasons) != 0 || len(got.nakDelays) != 0 {
					t.Errorf("snapshot = %+v, want a single plain Nak", got)
				}
				if dlq.Len() != 0 {
					t.Errorf("DLQ records = %d, want 0 (an interrupted message is never dead-lettered)", dlq.Len())
				}
				_, errList := errs.snapshot()
				if len(errList) != tc.wantOnErr {
					t.Fatalf("OnErr calls = %d, want %d", len(errList), tc.wantOnErr)
				}
				if tc.wantOnErr == 1 && !errors.Is(errList[0], event.ErrShutdownInterrupted) {
					t.Errorf("OnErr err = %v, want ErrShutdownInterrupted", errList[0])
				}
			})
		})
	}
}

func TestJetStreamBus_HandBackFinalDeliveryReported(t *testing.T) {
	for _, tc := range []struct {
		name      string
		delivered uint64
		wantOnErr int
	}{
		{"non-final delivery", 1, 0},
		{"final delivery", 3, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var errs rbErrs
				dlq := event.NewMemoryDLQ()
				ran := false
				bus, cons, _, payload := rbShutdownBus(t, BusOptions{DLQPublisher: dlq, OnErr: errs.onErr},
					func(context.Context, event.Event) error {
						ran = true
						return nil
					})
				defer cons.cc.finish()

				res := rbShutdown(bus, context.Background())
				synctest.Wait()

				msg := &mdMsg{data: payload, numDelivered: tc.delivered}
				<-rbDeliver(cons, msg)

				if ran {
					t.Error("handler ran for a message delivered while closing")
				}
				if got := msg.snapshot(); got.naks != 1 || got.acks != 0 || got.plainTerms != 0 || len(got.termReasons) != 0 {
					t.Errorf("snapshot = %+v, want a single Nak", got)
				}
				if dlq.Len() != 0 {
					t.Errorf("DLQ records = %d, want 0", dlq.Len())
				}
				evts, errList := errs.snapshot()
				if len(errList) != tc.wantOnErr {
					t.Fatalf("OnErr calls = %d, want %d", len(errList), tc.wantOnErr)
				}
				if tc.wantOnErr == 1 {
					if evts[0] != nil {
						t.Errorf("OnErr evt = %v, want nil", evts[0])
					}
					if !errors.Is(errList[0], event.ErrShutdownInterrupted) {
						t.Errorf("OnErr err = %v, want ErrShutdownInterrupted", errList[0])
					}
				}

				cons.cc.finish()
				if err := <-res; err != nil {
					t.Errorf("Shutdown = %v, want nil", err)
				}
			})
		})
	}
}

func TestJetStreamBus_CloseBoundedWhenDrainHangs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus, cons, _, _ := rbShutdownBus(t, BusOptions{ShutdownTimeout: time.Second}, noopHandler)

		start := time.Now()
		err := bus.Close()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close = %v, want DeadlineExceeded", err)
		}
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Errorf("Close took %v of fake time, want exactly the 1s ShutdownTimeout", elapsed)
		}
		if _, stops := cons.cc.counts(); stops != 1 {
			t.Errorf("Stop calls = %d, want 1", stops)
		}
	})
}

func TestJetStreamBus_CloseWithImmediateDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := newTestBus(&mockJS{})
		err := bus.Subscribe(context.Background(), "rb.order", noopHandler,
			event.WithStream("S"), event.WithConsumer("C"), event.WithMaxDeliver(3),
			event.WithBackOff([]time.Duration{time.Second}))
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if err := bus.Close(); err != nil {
			t.Errorf("Close = %v, want nil", err)
		}
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Errorf("second Shutdown = %v, want nil", err)
		}
		if err := bus.Emit(context.Background(), &fakeEvent{name: "rb.order"}); !errors.Is(err, event.ErrBusClosed) {
			t.Errorf("Emit after Close = %v, want ErrBusClosed", err)
		}
	})
}

// --- callback panic recovery (WR-01) ---

// wrRecord is one captured log record.
type wrRecord struct {
	level slog.Level
	attrs map[string]string
}

// wrLogs is a slog.Handler that captures every record.
type wrLogs struct {
	mu   sync.Mutex
	recs []wrRecord
}

func (l *wrLogs) Enabled(context.Context, slog.Level) bool { return true }
func (l *wrLogs) WithAttrs([]slog.Attr) slog.Handler       { return l }
func (l *wrLogs) WithGroup(string) slog.Handler            { return l }

func (l *wrLogs) Handle(_ context.Context, r slog.Record) error {
	rec := wrRecord{level: r.Level, attrs: map[string]string{"msg": r.Message}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.String()
		return true
	})
	l.mu.Lock()
	l.recs = append(l.recs, rec)
	l.mu.Unlock()
	return nil
}

func (l *wrLogs) snapshot() []wrRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.recs)
}

// panicAt returns the attrs["panic"] of the Error-level recovered-panic record.
func (l *wrLogs) panicAt() (string, bool) {
	for _, r := range l.snapshot() {
		if r.level == slog.LevelError && r.attrs["msg"] == "eventbus: panic in message handler recovered" {
			return r.attrs["panic"], true
		}
	}
	return "", false
}

// wrInstallLogs captures the default logger and restores it on cleanup.
func wrInstallLogs(t *testing.T) *wrLogs {
	t.Helper()
	logs := &wrLogs{}
	prev := slog.Default()
	slog.SetDefault(slog.New(logs))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return logs
}

// wrPanicRegistry panics in Decode; Encode and EncodeWith are promoted.
type wrPanicRegistry struct{ *mdRegistry }

func (wrPanicRegistry) Decode(context.Context, []byte) (context.Context, event.Event, error) {
	panic("wr: decode boom")
}

// wrPanicDLQ is a DLQPublisher whose Publish panics.
type wrPanicDLQ struct{}

func (wrPanicDLQ) Publish(context.Context, event.DLQRecord) error { panic("wr: dlq boom") }

// wrPanicEvent is an rb.order event whose OnErr method panics.
type wrPanicEvent struct {
	EventID string `json:"event_id"`
	Payload string `json:"payload"`
}

func (*wrPanicEvent) Name() string                         { return "rb.order" }
func (*wrPanicEvent) OnErr(context.Context, error, string) { panic("wr: event onerr boom") }

func wrPanicEventRegistry() *mdRegistry {
	reg := newMdRegistry()
	reg.Register("rb.order", func() event.Event { return &wrPanicEvent{} }, 1)
	return reg
}

func wrOnErrPanics(context.Context, event.Event, error, string) { panic("wr: onerr boom") }

func wrFails(context.Context, event.Event) error { return errors.New("wr: failed") }

func wrPermanent(context.Context, event.Event) error { return dispPermanent{} }

// TestJetStreamBus_CallbackPanicRecoveredAndNaked covers the hand-back with
// NakWithDelay: a non-final delivery with no settlement, and the final delivery
// after settlement (the bus OnErr row). The final delivery without settlement is
// reported and termed instead, see
// TestJetStreamBus_CallbackPanicOnFinalDeliveryReported.
func TestJetStreamBus_CallbackPanicRecoveredAndNaked(t *testing.T) {
	for _, row := range []struct {
		name      string
		delivered uint64
		wantDelay time.Duration
		wantPanic string
		reg       func() event.Registry
		opts      BusOptions
		fn        event.HandleFn
	}{
		{
			name: "bus OnErr", delivered: 3, wantDelay: 20 * time.Millisecond, wantPanic: "wr: onerr boom",
			reg:  func() event.Registry { return newMdRegistry("rb.order") },
			opts: BusOptions{OnErr: wrOnErrPanics}, fn: wrFails,
		},
		{
			name: "DLQ publisher, first delivery", delivered: 1, wantDelay: 10 * time.Millisecond, wantPanic: "wr: dlq boom",
			reg:  func() event.Registry { return newMdRegistry("rb.order") },
			opts: BusOptions{DLQPublisher: wrPanicDLQ{}}, fn: wrPermanent,
		},
		{
			name: "decode", delivered: 2, wantDelay: 20 * time.Millisecond, wantPanic: "wr: decode boom",
			reg: func() event.Registry { return wrPanicRegistry{newMdRegistry("rb.order")} },
			fn:  func(context.Context, event.Event) error { t.Error("handler ran although Decode panicked"); return nil },
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			logs := wrInstallLogs(t)
			synctest.Test(t, func(t *testing.T) {
				js := &mdJS{}
				// The payload is emitted through a plain registry; the bus under
				// test decodes with the row's registry.
				payload := rbEmit(t, newMdBus(js, newMdRegistry("rb.order"), BusOptions{}), js)
				bus := newMdBus(js, row.reg(), row.opts)

				msg := &mdMsg{data: payload, numDelivered: row.delivered}
				bus.msgHandler(context.Background(), "h", row.fn, rbPolicy())(msg)

				got := msg.snapshot()
				if want := []time.Duration{row.wantDelay}; !slices.Equal(got.nakDelays, want) {
					t.Errorf("nakDelays = %v, want %v", got.nakDelays, want)
				}
				if got.naks != 0 || got.acks != 0 || got.plainTerms != 0 || len(got.termReasons) != 0 {
					t.Errorf("snapshot = %+v, want only one NakWithDelay", got)
				}
				if p, ok := logs.panicAt(); !ok || p != row.wantPanic {
					t.Errorf("panic record = %q (found %v), want %q", p, ok, row.wantPanic)
				}

				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if err := bus.Shutdown(ctx); err != nil {
					t.Errorf("Shutdown = %v, want nil (the in-flight count was released)", err)
				}
			})
		})
	}
}

// TestJetStreamBus_CallbackPanicOnFinalDeliveryReported covers G-09-1: a panic
// outside the handler on the final delivery, before Settle dead-lettered or
// reported the message, reaches the bus OnErr once and ends the message with
// TermWithReason. The server would not redeliver it, so a hand-back would lose it.
// The decode rows also stand for an upcaster panic, since upcasters run inside
// registry.Decode.
func TestJetStreamBus_CallbackPanicOnFinalDeliveryReported(t *testing.T) {
	const reportPanicMsg = "eventbus: panic in OnErr while reporting a panic on the final delivery"
	for _, row := range []struct {
		name           string
		wantPanic      string
		reg            func() event.Registry
		opts           func(errs *rbErrs) BusOptions
		fn             event.HandleFn
		wantOnErr      int
		wantOnErrPanic string
	}{
		{
			name: "decode", wantPanic: "wr: decode boom", wantOnErr: 1,
			reg:  func() event.Registry { return wrPanicRegistry{newMdRegistry("rb.order")} },
			opts: func(e *rbErrs) BusOptions { return BusOptions{OnErr: e.onErr} },
			fn:   func(context.Context, event.Event) error { t.Error("handler ran although Decode panicked"); return nil },
		},
		{
			name: "DLQ publisher", wantPanic: "wr: dlq boom", wantOnErr: 1,
			reg:  func() event.Registry { return newMdRegistry("rb.order") },
			opts: func(e *rbErrs) BusOptions { return BusOptions{DLQPublisher: wrPanicDLQ{}, OnErr: e.onErr} },
			fn:   wrFails,
		},
		{
			name: "decode, OnErr panics while reporting", wantPanic: "wr: decode boom", wantOnErr: 0,
			wantOnErrPanic: "wr: onerr boom",
			reg:            func() event.Registry { return wrPanicRegistry{newMdRegistry("rb.order")} },
			opts:           func(*rbErrs) BusOptions { return BusOptions{OnErr: wrOnErrPanics} },
			fn:             func(context.Context, event.Event) error { t.Error("handler ran although Decode panicked"); return nil },
		},
		{
			name: "decode, no OnErr", wantPanic: "wr: decode boom", wantOnErr: 0,
			reg:  func() event.Registry { return wrPanicRegistry{newMdRegistry("rb.order")} },
			opts: func(*rbErrs) BusOptions { return BusOptions{} },
			fn:   func(context.Context, event.Event) error { t.Error("handler ran although Decode panicked"); return nil },
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			logs := wrInstallLogs(t)
			synctest.Test(t, func(t *testing.T) {
				errs := &rbErrs{}
				js := &mdJS{}
				payload := rbEmit(t, newMdBus(js, newMdRegistry("rb.order"), BusOptions{}), js)
				bus := newMdBus(js, row.reg(), row.opts(errs))

				// numDelivered equals rbPolicy().maxDeliver: the final delivery.
				msg := &mdMsg{data: payload, numDelivered: 3}
				bus.msgHandler(context.Background(), "h", row.fn, rbPolicy())(msg)

				got := msg.snapshot()
				if want := []string{termReasonPanicOnFinal}; !slices.Equal(got.termReasons, want) {
					t.Errorf("termReasons = %q, want %q", got.termReasons, want)
				}
				if got.naks != 0 || got.acks != 0 || got.plainTerms != 0 || len(got.nakDelays) != 0 {
					t.Errorf("snapshot = %+v, want only one TermWithReason", got)
				}
				if p, ok := logs.panicAt(); !ok || p != row.wantPanic {
					t.Errorf("panic record = %q (found %v), want %q", p, ok, row.wantPanic)
				}

				evts, errList := errs.snapshot()
				if len(errList) != row.wantOnErr {
					t.Fatalf("OnErr calls = %d, want %d", len(errList), row.wantOnErr)
				}
				if row.wantOnErr == 1 {
					if evts[0] != nil {
						t.Errorf("OnErr evt = %v, want nil", evts[0])
					}
					var pe *event.PanicError
					if !errors.As(errList[0], &pe) {
						t.Fatalf("OnErr err = %v, want a *event.PanicError", errList[0])
					}
					if fmt.Sprint(pe.Value) != row.wantPanic {
						t.Errorf("PanicError value = %v, want %q", pe.Value, row.wantPanic)
					}
					if !errors.Is(errList[0], event.ErrHandlerPanic) {
						t.Errorf("OnErr err = %v, want to match ErrHandlerPanic", errList[0])
					}
				}
				if row.wantOnErrPanic != "" {
					found := false
					for _, r := range logs.snapshot() {
						if r.level == slog.LevelError && r.attrs["msg"] == reportPanicMsg && r.attrs["panic"] == row.wantOnErrPanic {
							found = true
						}
					}
					if !found {
						t.Errorf("no Error record %q with panic=%q", reportPanicMsg, row.wantOnErrPanic)
					}
				}

				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if err := bus.Shutdown(ctx); err != nil {
					t.Errorf("Shutdown = %v, want nil", err)
				}
			})
		})
	}
}

func TestJetStreamBus_CallbackPanicAfterSettleTermed(t *testing.T) {
	for _, row := range []struct {
		name      string
		wantPanic string
		reg       func() event.Registry
		opts      func(dlq *event.MemoryDLQ) BusOptions
		fn        event.HandleFn
		wantDLQ   int
		wantAttrs map[string]string
	}{
		{
			name: "dead-lettered, bus OnErr", wantPanic: "wr: onerr boom", wantDLQ: 1,
			reg:  func() event.Registry { return newMdRegistry("rb.order") },
			opts: func(d *event.MemoryDLQ) BusOptions { return BusOptions{DLQPublisher: d, OnErr: wrOnErrPanics} },
			fn:   wrPermanent,
		},
		{
			name: "dead-lettered, event OnErr", wantPanic: "wr: event onerr boom", wantDLQ: 1,
			reg:  func() event.Registry { return wrPanicEventRegistry() },
			opts: func(d *event.MemoryDLQ) BusOptions { return BusOptions{DLQPublisher: d} },
			fn:   wrPermanent,
		},
		{
			name: "chain dead-lettered, bus OnErr", wantPanic: "wr: onerr boom", wantDLQ: 0,
			reg:  func() event.Registry { return newMdRegistry("rb.order") },
			opts: func(d *event.MemoryDLQ) BusOptions { return BusOptions{DLQPublisher: d, OnErr: wrOnErrPanics} },
			fn: func(context.Context, event.Event) error {
				return &event.DeadLetteredError{Cause: dispPermanent{}, Reason: event.ReasonTerminal}
			},
		},
		{
			name: "dropped, bus OnErr", wantPanic: "wr: onerr boom", wantDLQ: 0,
			reg:       func() event.Registry { return newMdRegistry("rb.order") },
			opts:      func(*event.MemoryDLQ) BusOptions { return BusOptions{OnErr: wrOnErrPanics} },
			fn:        wrPermanent,
			wantAttrs: map[string]string{"outcome": "dropped"},
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			logs := wrInstallLogs(t)
			synctest.Test(t, func(t *testing.T) {
				js := &mdJS{}
				payload := rbEmit(t, newMdBus(js, newMdRegistry("rb.order"), BusOptions{}), js)
				dlq := event.NewMemoryDLQ()
				bus := newMdBus(js, row.reg(), row.opts(dlq))

				msg := &mdMsg{data: payload, numDelivered: 1}
				bus.msgHandler(context.Background(), "h", row.fn, rbPolicy())(msg)

				got := msg.snapshot()
				if want := []string{termReasonPanicAfterSettle}; !slices.Equal(got.termReasons, want) {
					t.Errorf("termReasons = %q, want %q", got.termReasons, want)
				}
				if got.naks != 0 || got.acks != 0 || got.plainTerms != 0 || len(got.nakDelays) != 0 {
					t.Errorf("snapshot = %+v, want only a TermWithReason (no redelivery)", got)
				}
				if dlq.Len() != row.wantDLQ {
					t.Errorf("DLQ records = %d, want %d", dlq.Len(), row.wantDLQ)
				}
				if p, ok := logs.panicAt(); !ok || p != row.wantPanic {
					t.Errorf("panic record = %q (found %v), want %q", p, ok, row.wantPanic)
				}
				for k, v := range row.wantAttrs {
					found := false
					for _, r := range logs.snapshot() {
						if r.attrs[k] == v {
							found = true
						}
					}
					if !found {
						t.Errorf("no log record with %s=%q", k, v)
					}
				}

				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if err := bus.Shutdown(ctx); err != nil {
					t.Errorf("Shutdown = %v, want nil", err)
				}
			})
		})
	}
}

func TestJetStreamBus_HandBackOnErrPanicNakedOnce(t *testing.T) {
	logs := wrInstallLogs(t)
	synctest.Test(t, func(t *testing.T) {
		bus, cons, _, payload := rbShutdownBus(t, BusOptions{OnErr: wrOnErrPanics}, noopHandler)
		defer cons.cc.finish()

		res := rbShutdown(bus, context.Background())
		synctest.Wait()

		msg := &mdMsg{data: payload, numDelivered: 3}
		<-rbDeliver(cons, msg)

		got := msg.snapshot()
		if got.naks != 1 || got.acks != 0 || got.plainTerms != 0 || len(got.termReasons) != 0 || len(got.nakDelays) != 0 {
			t.Errorf("snapshot = %+v, want exactly one plain Nak", got)
		}
		if p, ok := logs.panicAt(); !ok || p != "wr: onerr boom" {
			t.Errorf("panic record = %q (found %v), want wr: onerr boom", p, ok)
		}

		cons.cc.finish()
		if err := <-res; err != nil {
			t.Errorf("Shutdown = %v, want nil", err)
		}
	})
}

// --- concurrent Shutdown (WR-02) ---

func TestJetStreamBus_ConcurrentShutdownWaitsForDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus, cons, _, _ := rbShutdownBus(t, BusOptions{}, noopHandler)
		defer cons.cc.finish()

		first := rbShutdown(bus, context.Background())
		synctest.Wait()
		second := rbShutdown(bus, context.Background())
		synctest.Wait()

		for i, res := range []<-chan error{first, second} {
			select {
			case err := <-res:
				t.Fatalf("Shutdown #%d returned %v while a consumer was still draining", i+1, err)
			default:
			}
		}
		if drains, _ := cons.cc.counts(); drains != 1 {
			t.Errorf("Drain calls = %d, want 1", drains)
		}

		cons.cc.finish()
		for i, res := range []<-chan error{first, second} {
			if err := <-res; err != nil {
				t.Errorf("Shutdown #%d = %v, want nil", i+1, err)
			}
		}
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Errorf("third Shutdown = %v, want nil", err)
		}
		if drains, stops := cons.cc.counts(); drains != 1 || stops != 0 {
			t.Errorf("Drain/Stop calls = %d/%d, want 1/0", drains, stops)
		}
	})
}

func TestJetStreamBus_ConcurrentShutdownPeerDeadlineForces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cause := make(chan error, 1)
		started := make(chan struct{}, 1)
		bus, cons, _, payload := rbShutdownBus(t, BusOptions{}, func(ctx context.Context, _ event.Event) error {
			started <- struct{}{}
			<-ctx.Done()
			cause <- context.Cause(ctx)
			return ctx.Err()
		})
		defer cons.cc.finish()

		msg := &mdMsg{data: payload, numDelivered: 1}
		done := rbDeliver(cons, msg)
		synctest.Wait()
		<-started

		first := rbShutdown(bus, context.Background())
		synctest.Wait()

		sctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		start := time.Now()
		err := bus.Shutdown(sctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second Shutdown = %v, want DeadlineExceeded", err)
		}
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Errorf("second Shutdown took %v of fake time, want exactly 1s", elapsed)
		}

		<-done
		if got := <-cause; !errors.Is(got, event.ErrShutdownInterrupted) {
			t.Errorf("handler cause = %v, want ErrShutdownInterrupted", got)
		}
		if err := <-first; err != nil {
			t.Errorf("first Shutdown = %v, want nil once no handler is in flight", err)
		}
		if got := msg.snapshot(); got.naks != 1 || got.acks != 0 || got.plainTerms != 0 || len(got.termReasons) != 0 || len(got.nakDelays) != 0 {
			t.Errorf("snapshot = %+v, want a single plain Nak", got)
		}
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Errorf("later Shutdown = %v, want nil", err)
		}
		if drains, stops := cons.cc.counts(); drains != 1 || stops != 1 {
			t.Errorf("Drain/Stop calls = %d/%d, want 1/1", drains, stops)
		}
	})
}
