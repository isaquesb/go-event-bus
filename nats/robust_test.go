package nats

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/isaquesb/go-event-bus"
	natsgo "github.com/nats-io/nats.go"
)

// --- fakes ---

const rbInbox = "_INBOX.rb"

// rbConn is a loopback natsConn guarded by a mutex. Subscribe stores the
// callback per subject and returns a nil *nats.Subscription (never dereferenced
// by the bus). Request runs the stored callback synchronously and returns the
// last message PublishMsg'd to the inbox.
type rbConn struct {
	mu        sync.Mutex
	handlers  map[string]natsgo.MsgHandler
	published []*natsgo.Msg // Publish
	replies   []*natsgo.Msg // PublishMsg
	closes    int
}

func newRbConn() *rbConn { return &rbConn{handlers: map[string]natsgo.MsgHandler{}} }

func (c *rbConn) Subscribe(subj string, cb natsgo.MsgHandler) (*natsgo.Subscription, error) {
	c.mu.Lock()
	c.handlers[subj] = cb
	c.mu.Unlock()
	return nil, nil
}

func (c *rbConn) Publish(subj string, data []byte) error {
	c.mu.Lock()
	c.published = append(c.published, &natsgo.Msg{Subject: subj, Data: data})
	c.mu.Unlock()
	return nil
}

func (c *rbConn) PublishMsg(m *natsgo.Msg) error {
	c.mu.Lock()
	c.replies = append(c.replies, m)
	c.mu.Unlock()
	return nil
}

func (c *rbConn) Request(subj string, data []byte, _ time.Duration) (*natsgo.Msg, error) {
	c.mu.Lock()
	cb := c.handlers[subj]
	start := len(c.replies)
	c.mu.Unlock()
	if cb == nil {
		return nil, natsgo.ErrNoResponders
	}
	cb(&natsgo.Msg{Subject: subj, Data: data, Reply: rbInbox})

	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.replies) - 1; i >= start; i-- {
		if c.replies[i].Subject == rbInbox {
			return c.replies[i], nil
		}
	}
	return nil, natsgo.ErrTimeout
}

func (c *rbConn) Close() {
	c.mu.Lock()
	c.closes++
	c.mu.Unlock()
}

func (c *rbConn) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes
}

func (c *rbConn) publishedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.published)
}

func (c *rbConn) callback(t *testing.T, subj string) natsgo.MsgHandler {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	cb := c.handlers[subj]
	if cb == nil {
		t.Fatalf("no callback subscribed to %q", subj)
	}
	return cb
}

func (c *rbConn) lastReply(t *testing.T) *natsgo.Msg {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.replies) == 0 {
		t.Fatal("no reply was published")
	}
	return c.replies[len(c.replies)-1]
}

// rbPayload encodes an rb.order envelope with the given id.
func rbPayload(t *testing.T, reg *mdRegistry, id string) []byte {
	t.Helper()
	data, err := reg.Encode(context.Background(), &mdEvent{name: "rb.order", EventID: id, Payload: "p"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return data
}

func rbPanicHandler(context.Context, event.Event) error { panic("rb boom") }

// --- panic recovery (CORE-01) ---

func TestNatsSubscribe_PanicRecovered(t *testing.T) {
	conn := newRbConn()
	reg := newMdRegistry("rb.order")
	dlq := event.NewMemoryDLQ()
	rec := &noRecorder{}
	bus := &Bus{nc: conn, reg: reg, timeout: time.Second, dlq: dlq, observer: rec}

	if err := bus.Subscribe(context.Background(), "rb.order", rbPanicHandler); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// The callback returns normally: the panic does not escape.
	conn.callback(t, "rb.order")(&natsgo.Msg{Subject: "rb.order", Data: rbPayload(t, reg, "rb-1")})

	recs := dlq.Records()
	if len(recs) != 1 {
		t.Fatalf("DLQ records = %d, want 1", len(recs))
	}
	if recs[0].Reason != event.ReasonPanic {
		t.Errorf("DLQ reason = %q, want %q", recs[0].Reason, event.ReasonPanic)
	}
	if len(rec.failures()) != 1 {
		t.Fatalf("OnErr calls = %d, want 1", len(rec.failures()))
	}
	_, err := noFailureAt(t, rec, 0)
	if !errors.Is(err, event.ErrDeadLettered) || !errors.Is(err, event.ErrHandlerPanic) {
		t.Errorf("OnErr error = %v, want ErrDeadLettered and ErrHandlerPanic", err)
	}
}

func TestNatsSync_PanicRepliesEventbusError(t *testing.T) {
	conn := newRbConn()
	reg := newMdRegistry("rb.order")
	dlq := event.NewMemoryDLQ()
	bus := &Bus{nc: conn, reg: reg, timeout: time.Second, dlq: dlq}

	if err := bus.SubscribeSync(context.Background(), "rb.order", rbPanicHandler); err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	got, err := bus.EmitSync(context.Background(), &mdEvent{name: "rb.order", EventID: "rb-2"})
	if got != nil {
		t.Errorf("EmitSync event = %v, want nil", got)
	}
	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("EmitSync error = %v, want *RemoteError", err)
	}
	if re.Reason != event.ReasonPanic {
		t.Errorf("RemoteError.Reason = %q, want %q", re.Reason, event.ReasonPanic)
	}
	if !errors.Is(err, ErrRemoteHandler) || !errors.Is(err, event.ErrHandlerPanic) {
		t.Errorf("EmitSync error = %v, want ErrRemoteHandler and ErrHandlerPanic", err)
	}

	reply := conn.lastReply(t)
	if v := reply.Header.Get(HeaderError); v != event.ReasonPanic {
		t.Errorf("reply %s = %q, want %q", HeaderError, v, event.ReasonPanic)
	}
	if len(reply.Data) != 0 {
		t.Errorf("reply data = %q, want empty", reply.Data)
	}
	if dlq.Len() != 1 {
		t.Errorf("DLQ records = %d, want 1", dlq.Len())
	}
}

// --- shutdown (CORE-02) ---

// rbGate is a one-shot latch.
type rbGate struct {
	once sync.Once
	ch   chan struct{}
}

func newRbGate() *rbGate { return &rbGate{ch: make(chan struct{})} }

func (g *rbGate) open() { g.once.Do(func() { close(g.ch) }) }

// rbDeliver runs the callback subscribed to subj in a goroutine of the bubble
// and returns a channel closed when the callback returned.
func rbDeliver(t *testing.T, conn *rbConn, subj string, msg *natsgo.Msg) <-chan struct{} {
	t.Helper()
	cb := conn.callback(t, subj)
	done := make(chan struct{})
	go func() {
		defer close(done)
		cb(msg)
	}()
	return done
}

// rbWait waits for ch to close, failing the test after a minute of fake time so a
// regression fails cleanly instead of deadlocking the bubble.
func rbWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Minute):
		t.Fatal("timed out waiting for a callback to return")
	}
}

func TestNatsBus_ShutdownWaitsForInFlightHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newRbConn()
		reg := newMdRegistry("rb.order")
		bus := &Bus{nc: conn, reg: reg, timeout: time.Second}
		ctx := context.Background()

		gate := newRbGate()
		defer gate.open()
		entered := make(chan struct{})
		err := bus.Subscribe(ctx, "rb.order", func(context.Context, event.Event) error {
			close(entered)
			<-gate.ch
			return nil
		})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		cbDone := rbDeliver(t, conn, "rb.order", &natsgo.Msg{Subject: "rb.order", Data: rbPayload(t, reg, "rb-1")})
		<-entered

		shutDone := make(chan error, 1)
		go func() { shutDone <- bus.Shutdown(ctx) }()
		synctest.Wait()

		select {
		case err := <-shutDone:
			t.Fatalf("Shutdown returned %v while a handler was in flight", err)
		default:
		}
		if n := conn.closeCount(); n != 0 {
			t.Errorf("conn closed %d times during the drain, want 0", n)
		}

		evt := &mdEvent{name: "rb.order", EventID: "rb-x"}
		if err := bus.Emit(ctx, evt); !errors.Is(err, event.ErrBusClosed) {
			t.Errorf("Emit during drain = %v, want ErrBusClosed", err)
		}
		if got, err := bus.EmitSync(ctx, evt); got != nil || !errors.Is(err, event.ErrBusClosed) {
			t.Errorf("EmitSync during drain = (%v, %v), want (nil, ErrBusClosed)", got, err)
		}
		if err := bus.Subscribe(ctx, "rb.other", noopHandler); !errors.Is(err, event.ErrBusClosed) {
			t.Errorf("Subscribe during drain = %v, want ErrBusClosed", err)
		}
		if err := bus.SubscribeSync(ctx, "rb.other", noopHandler); !errors.Is(err, event.ErrBusClosed) {
			t.Errorf("SubscribeSync during drain = %v, want ErrBusClosed", err)
		}

		gate.open()
		if err := <-shutDone; err != nil {
			t.Errorf("Shutdown = %v, want nil", err)
		}
		rbWait(t, cbDone)
		if n := conn.closeCount(); n != 1 {
			t.Errorf("conn closed %d times, want 1", n)
		}
	})
}

func TestNatsBus_NestedEmitDuringDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newRbConn()
		reg := newMdRegistry("rb.order", "rb.child")
		bus := &Bus{nc: conn, reg: reg, timeout: time.Second}
		ctx := context.Background()

		gate := newRbGate()
		defer gate.open()
		entered := make(chan struct{})
		var nestedErr error
		err := bus.Subscribe(ctx, "rb.order", func(hctx context.Context, _ event.Event) error {
			close(entered)
			<-gate.ch
			nestedErr = bus.Emit(hctx, &mdEvent{name: "rb.child", EventID: "rb-child"})
			return nil
		})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		cbDone := rbDeliver(t, conn, "rb.order", &natsgo.Msg{Subject: "rb.order", Data: rbPayload(t, reg, "rb-1")})
		<-entered

		shutDone := make(chan error, 1)
		go func() { shutDone <- bus.Shutdown(ctx) }()
		synctest.Wait()

		if err := bus.Emit(ctx, &mdEvent{name: "rb.child", EventID: "rb-ext"}); !errors.Is(err, event.ErrBusClosed) {
			t.Errorf("external Emit during drain = %v, want ErrBusClosed", err)
		}
		if n := conn.publishedCount(); n != 0 {
			t.Fatalf("published = %d before the handler emitted, want 0", n)
		}

		gate.open()
		if err := <-shutDone; err != nil {
			t.Errorf("Shutdown = %v, want nil", err)
		}
		rbWait(t, cbDone)
		if nestedErr != nil {
			t.Errorf("nested Emit during drain = %v, want nil", nestedErr)
		}
		if n := conn.publishedCount(); n != 1 {
			t.Errorf("published = %d, want 1 (the nested emit)", n)
		}
	})
}

// rbStuck wires a Subscribe handler that blocks until its ctx is cancelled (or
// the gate opens) and delivers one message to it. It returns the channel closed
// when the callback returned and a func reading the cause the handler saw.
func rbStuck(t *testing.T, bus *Bus, conn *rbConn, reg *mdRegistry, gate *rbGate) (<-chan struct{}, func() error) {
	t.Helper()
	entered := make(chan struct{})
	var cause error
	err := bus.Subscribe(context.Background(), "rb.order", func(ctx context.Context, _ event.Event) error {
		close(entered)
		select {
		case <-ctx.Done():
			cause = context.Cause(ctx)
			return ctx.Err()
		case <-gate.ch:
			return nil
		}
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	done := rbDeliver(t, conn, "rb.order", &natsgo.Msg{Subject: "rb.order", Data: rbPayload(t, reg, "rb-1")})
	<-entered
	return done, func() error { return cause }
}

func TestNatsBus_ShutdownDeadlineInterruptsHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newRbConn()
		reg := newMdRegistry("rb.order")
		dlq := event.NewMemoryDLQ()
		rec := &noRecorder{}
		bus := &Bus{nc: conn, reg: reg, timeout: time.Second, dlq: dlq, observer: rec}
		gate := newRbGate()
		defer gate.open()

		cbDone, cause := rbStuck(t, bus, conn, reg, gate)

		sctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := bus.Shutdown(sctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Shutdown = %v, want context.DeadlineExceeded", err)
		}
		rbWait(t, cbDone)

		if got := cause(); !errors.Is(got, event.ErrShutdownInterrupted) {
			t.Errorf("handler saw cause %v, want ErrShutdownInterrupted", got)
		}
		if len(rec.failures()) != 1 {
			t.Fatalf("OnErr calls = %d, want 1", len(rec.failures()))
		}
		if _, err := noFailureAt(t, rec, 0); !errors.Is(err, event.ErrShutdownInterrupted) {
			t.Errorf("OnErr error = %v, want ErrShutdownInterrupted", err)
		}
		if n := dlq.Len(); n != 0 {
			t.Errorf("DLQ records = %d, want 0 (an interruption is never dead-lettered)", n)
		}
		if n := conn.closeCount(); n != 1 {
			t.Errorf("conn closed %d times, want 1", n)
		}
	})
}

func TestNatsBus_LateDeliveryAfterDeadlineReported(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newRbConn()
		reg := newMdRegistry("rb.order", "rb.late")
		dlq := event.NewMemoryDLQ()
		rec := &noRecorder{}
		bus := &Bus{nc: conn, reg: reg, timeout: time.Second, dlq: dlq, observer: rec}
		gate := newRbGate()
		defer gate.open()

		var runs int
		var mu sync.Mutex
		counting := func(context.Context, event.Event) error {
			mu.Lock()
			runs++
			mu.Unlock()
			return nil
		}
		if err := bus.Subscribe(context.Background(), "rb.late", counting); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if err := bus.SubscribeSync(context.Background(), "rb.late.sync", counting); err != nil {
			t.Fatalf("SubscribeSync: %v", err)
		}

		cbDone, _ := rbStuck(t, bus, conn, reg, gate)
		sctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = bus.Shutdown(sctx)
		rbWait(t, cbDone)
		before := len(rec.failures())

		// A Subscribe delivery after the forced cancel is reported, not run.
		conn.callback(t, "rb.late")(&natsgo.Msg{Subject: "rb.late", Data: rbPayload(t, reg, "rb-late")})
		if len(rec.failures()) != before+1 {
			t.Fatalf("OnErr calls = %d, want %d", len(rec.failures()), before+1)
		}
		evt, err := noFailureAt(t, rec, before)
		if evt != nil {
			t.Errorf("OnErr event = %v, want nil", evt)
		}
		if !errors.Is(err, event.ErrShutdownInterrupted) {
			t.Errorf("OnErr error = %v, want ErrShutdownInterrupted", err)
		}

		// A SubscribeSync request gets an Eventbus-Error reply.
		conn.callback(t, "rb.late.sync")(&natsgo.Msg{
			Subject: "rb.late.sync", Data: rbPayload(t, reg, "rb-late2"), Reply: rbInbox,
		})
		if v := conn.lastReply(t).Header.Get(HeaderError); v != event.ReasonInterrupted {
			t.Errorf("late sync reply %s = %q, want %q", HeaderError, v, event.ReasonInterrupted)
		}

		mu.Lock()
		defer mu.Unlock()
		if runs != 0 {
			t.Errorf("late handlers ran %d times, want 0", runs)
		}
		if n := dlq.Len(); n != 0 {
			t.Errorf("DLQ records = %d, want 0", n)
		}
	})

	// After a graceful Shutdown a straggler is also reported, never dead-lettered.
	synctest.Test(t, func(t *testing.T) {
		conn := newRbConn()
		reg := newMdRegistry("rb.late")
		dlq := event.NewMemoryDLQ()
		rec := &noRecorder{}
		bus := &Bus{nc: conn, reg: reg, timeout: time.Second, dlq: dlq, observer: rec}
		if err := bus.Subscribe(context.Background(), "rb.late", noopHandler); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
		conn.callback(t, "rb.late")(&natsgo.Msg{Subject: "rb.late", Data: rbPayload(t, reg, "rb-late")})
		if len(rec.failures()) != 1 {
			t.Fatalf("OnErr calls = %d, want 1", len(rec.failures()))
		}
		if _, err := noFailureAt(t, rec, 0); !errors.Is(err, event.ErrShutdownInterrupted) {
			t.Errorf("OnErr error = %v, want ErrShutdownInterrupted", err)
		}
		if n := dlq.Len(); n != 0 {
			t.Errorf("DLQ records = %d, want 0", n)
		}
	})
}

func TestNatsBus_CloseIdempotentAndBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newRbConn()
		bus := &Bus{nc: conn, reg: newMdRegistry(), timeout: time.Second}
		if err := bus.Close(); err != nil {
			t.Errorf("first Close = %v, want nil", err)
		}
		if err := bus.Close(); err != nil {
			t.Errorf("second Close = %v, want nil", err)
		}
		if n := conn.closeCount(); n != 1 {
			t.Errorf("conn closed %d times, want 1", n)
		}
	})

	synctest.Test(t, func(t *testing.T) {
		conn := newRbConn()
		reg := newMdRegistry("rb.order")
		bus := &Bus{nc: conn, reg: reg, timeout: time.Second, shutdownTimeout: time.Second}
		gate := newRbGate()
		defer gate.open()
		cbDone, _ := rbStuck(t, bus, conn, reg, gate)

		start := time.Now()
		err := bus.Close()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Close with a stuck handler = %v, want context.DeadlineExceeded", err)
		}
		if got := time.Since(start); got != time.Second {
			t.Errorf("Close took %v of fake time, want 1s", got)
		}
		if n := conn.closeCount(); n != 1 {
			t.Errorf("conn closed %d times, want 1", n)
		}
		rbWait(t, cbDone)
		if err := bus.Close(); err != nil {
			t.Errorf("Close after the handler left = %v, want nil", err)
		}
		if n := conn.closeCount(); n != 1 {
			t.Errorf("conn closed %d times after the second Close, want 1", n)
		}
	})
}

func TestNatsBus_StructLiteralShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := &Bus{nc: newRbConn(), reg: newMdRegistry("rb.order"), timeout: time.Second}
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown = %v, want nil", err)
		}
		err := bus.Emit(context.Background(), &mdEvent{name: "rb.order", EventID: "rb-1"})
		if !errors.Is(err, event.ErrBusClosed) {
			t.Errorf("Emit after Shutdown = %v, want ErrBusClosed", err)
		}
	})
}

// --- callback-level panic recovery (WR-01) ---

// wrPanicObserver records every signal and panics in Failure. WithObserver
// wraps it with MultiObserver, so the panic is logged once and never reaches
// the bus (IN-03); a struct literal Bus still holds it unwrapped.
type wrPanicObserver struct{ *noRecorder }

func (wrPanicObserver) Failure(context.Context, event.FailureContext) { panic("wr: onerr boom") }

// wrOnErrEvent is an event whose own OnErr method panics: a panic in the
// report, outside the handler. Settle contains it and reports an InternalError
// (a raw Observer panic cannot reach the bus any more: WithObserver isolates
// it), so the callback guard and the release accounting stay covered.
type wrOnErrEvent struct{ mdEvent }

func (*wrOnErrEvent) OnErr(context.Context, error, string) { panic("wr: onerr boom") }

func wrOnErrFactory() event.Event { return &wrOnErrEvent{mdEvent{name: "rb.order"}} }

// wrOnErrRegistry decodes "rb.order" into a wrOnErrEvent.
func wrOnErrRegistry() *mdRegistry {
	reg := newMdRegistry()
	reg.Register("rb.order", wrOnErrFactory, 1)
	return reg
}

// wrPanicRegistry panics in Decode.
type wrPanicRegistry struct{ *mdRegistry }

func (wrPanicRegistry) Decode(context.Context, []byte) (context.Context, event.Event, error) {
	panic("wr: decode boom")
}

// wrPanicDLQ panics in Publish.
type wrPanicDLQ struct{}

func (wrPanicDLQ) Publish(context.Context, event.DLQRecord) error { panic("wr: dlq boom") }

// wrPanicReplyConn panics when a reply is published.
type wrPanicReplyConn struct{ *rbConn }

func (wrPanicReplyConn) PublishMsg(*natsgo.Msg) error { panic("wr: reply boom") }

func wrFailHandler(context.Context, event.Event) error { return errors.New("wr: failed") }

func TestNatsBus_CallbackPanicRecovered(t *testing.T) {
	rows := []struct {
		name  string
		panic string
		build func(conn *rbConn, reg *mdRegistry, rec *noRecorder) *Bus
	}{
		{"event OnErr", "wr: onerr boom", func(conn *rbConn, reg *mdRegistry, rec *noRecorder) *Bus {
			reg.Register("rb.order", wrOnErrFactory, 1)
			return &Bus{nc: conn, reg: reg, timeout: time.Second, observer: rec}
		}},
		{"DLQ publisher", "wr: dlq boom", func(conn *rbConn, reg *mdRegistry, rec *noRecorder) *Bus {
			return &Bus{nc: conn, reg: reg, timeout: time.Second, dlq: wrPanicDLQ{}, observer: rec}
		}},
		{"decode", "wr: decode boom", func(conn *rbConn, reg *mdRegistry, rec *noRecorder) *Bus {
			return &Bus{nc: conn, reg: wrPanicRegistry{reg}, timeout: time.Second, observer: rec}
		}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				conn := newRbConn()
				reg := newMdRegistry("rb.order")
				rec := &noRecorder{}
				bus := row.build(conn, reg, rec)
				if err := bus.Subscribe(context.Background(), "rb.order", wrFailHandler, event.WithHandlerName("h")); err != nil {
					t.Fatalf("Subscribe: %v", err)
				}

				// The callback returns normally: a propagated panic aborts the binary.
				conn.callback(t, "rb.order")(&natsgo.Msg{Subject: "rb.order", Data: rbPayload(t, reg, "wr-1")})

				if !noPanicReported(rec, row.panic) {
					t.Errorf("no InternalError panic signal with value %q", row.panic)
				}
				sctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if err := bus.Shutdown(sctx); err != nil {
					t.Errorf("Shutdown = %v, want nil (the in-flight count must be released)", err)
				}
			})
		})
	}
}

func TestNatsSync_CallbackPanicRepliesPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newRbConn()
		reg := newMdRegistry("rb.order")
		rec := &noRecorder{}
		bus := &Bus{nc: conn, reg: wrPanicRegistry{reg}, timeout: time.Second, observer: rec}
		if err := bus.SubscribeSync(context.Background(), "rb.order", wrFailHandler); err != nil {
			t.Fatalf("SubscribeSync: %v", err)
		}

		got, err := bus.EmitSync(context.Background(), &mdEvent{name: "rb.order", EventID: "wr-s1"})
		if got != nil {
			t.Errorf("EmitSync event = %v, want nil", got)
		}
		var re *RemoteError
		if !errors.As(err, &re) {
			t.Fatalf("EmitSync error = %v, want *RemoteError", err)
		}
		if re.Reason != event.ReasonPanic {
			t.Errorf("RemoteError.Reason = %q, want %q", re.Reason, event.ReasonPanic)
		}
		if !errors.Is(err, event.ErrHandlerPanic) {
			t.Errorf("EmitSync error = %v, want ErrHandlerPanic", err)
		}

		conn.mu.Lock()
		defer conn.mu.Unlock()
		var inbox []*natsgo.Msg
		for _, m := range conn.replies {
			if m.Subject == rbInbox {
				inbox = append(inbox, m)
			}
		}
		if len(inbox) != 1 {
			t.Fatalf("messages published to %s = %d, want 1", rbInbox, len(inbox))
		}
		if len(inbox[0].Data) != 0 {
			t.Errorf("reply data = %q, want empty", inbox[0].Data)
		}
	})
}

func TestNatsSync_PanicReplyPanicContained(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reg := wrOnErrRegistry()
		conn := &wrPanicReplyConn{rbConn: newRbConn()}
		rec := &noRecorder{}
		bus := &Bus{nc: conn, reg: reg, timeout: time.Second, observer: rec}
		if err := bus.SubscribeSync(context.Background(), "rb.order", wrFailHandler, event.WithHandlerName("h")); err != nil {
			t.Fatalf("SubscribeSync: %v", err)
		}

		// The callback returns normally: a propagated panic aborts the binary.
		conn.callback(t, "rb.order")(&natsgo.Msg{Subject: "rb.order", Data: rbPayload(t, reg, "wr-s2"), Reply: rbInbox})

		if !noPanicReported(rec, "wr: onerr boom") {
			t.Errorf("no InternalError panic signal with value %q", "wr: onerr boom")
		}
		if !noPanicReported(rec, "wr: reply boom") {
			t.Errorf("no InternalError panic signal with value %q", "wr: reply boom")
		}
		sctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := bus.Shutdown(sctx); err != nil {
			t.Errorf("Shutdown = %v, want nil", err)
		}
	})
}

// --- concurrent Shutdown (WR-02) ---

// wrTrack injects one tracked subscription into bus.subs, before any goroutine
// starts. Its drain only counts calls; the test closes done to finish it.
func wrTrack(bus *Bus) (done chan struct{}, drains *atomic.Int32) {
	done = make(chan struct{})
	drains = &atomic.Int32{}
	bus.subs = append(bus.subs, trackedSub{
		subject: "wr.sub",
		drain:   func() error { drains.Add(1); return nil },
		done:    done,
	})
	return done, drains
}

func TestNatsBus_ConcurrentShutdownWaitsForDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newRbConn()
		bus := &Bus{nc: conn, reg: newMdRegistry(), timeout: time.Second}
		done, drains := wrTrack(bus)

		res := make(chan error, 3)
		go func() { res <- bus.Shutdown(context.Background()) }()
		synctest.Wait()
		go func() { res <- bus.Shutdown(context.Background()) }()
		synctest.Wait()

		select {
		case err := <-res:
			t.Fatalf("a Shutdown returned %v while the subscription was draining", err)
		default:
		}
		if n := drains.Load(); n != 1 {
			t.Errorf("drain ran %d times, want 1", n)
		}
		if n := conn.closeCount(); n != 0 {
			t.Errorf("conn closed %d times during the drain, want 0", n)
		}

		close(done)
		for i := 0; i < 2; i++ {
			if err := <-res; err != nil {
				t.Errorf("Shutdown #%d = %v, want nil", i+1, err)
			}
		}
		if n := conn.closeCount(); n != 1 {
			t.Errorf("conn closed %d times, want 1", n)
		}

		go func() { res <- bus.Shutdown(context.Background()) }()
		if err := <-res; err != nil {
			t.Errorf("third Shutdown = %v, want nil", err)
		}
		if n := conn.closeCount(); n != 1 {
			t.Errorf("conn closed %d times after the third Shutdown, want 1", n)
		}
		if n := drains.Load(); n != 1 {
			t.Errorf("drain ran %d times in total, want 1", n)
		}
	})
}

func TestNatsBus_ConcurrentShutdownPeerDeadlineForces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newRbConn()
		reg := newMdRegistry("rb.order")
		bus := &Bus{nc: conn, reg: reg, timeout: time.Second}
		_, drains := wrTrack(bus) // never finishes
		gate := newRbGate()
		defer gate.open()
		cbDone, cause := rbStuck(t, bus, conn, reg, gate)

		owner := make(chan error, 1)
		go func() { owner <- bus.Shutdown(context.Background()) }()
		synctest.Wait()

		peer := make(chan error, 1)
		start := time.Now()
		go func() {
			sctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			peer <- bus.Shutdown(sctx)
		}()
		if err := <-peer; !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("peer Shutdown = %v, want context.DeadlineExceeded", err)
		}
		if got := time.Since(start); got != time.Second {
			t.Errorf("peer Shutdown took %v of fake time, want 1s", got)
		}

		rbWait(t, cbDone)
		if got := cause(); !errors.Is(got, event.ErrShutdownInterrupted) {
			t.Errorf("handler saw cause %v, want ErrShutdownInterrupted", got)
		}
		if err := <-owner; err != nil {
			t.Errorf("owner Shutdown = %v, want nil", err)
		}
		if n := conn.closeCount(); n != 1 {
			t.Errorf("conn closed %d times, want 1", n)
		}
		if n := drains.Load(); n != 1 {
			t.Errorf("drain ran %d times, want 1", n)
		}
	})
}

// --- recover before release (G-09-2) ---

// wrGatedReplyConn holds the first reply published to rbInbox until gate opens.
// entered is closed when that reply arrives; closesAtReply records how many times
// the connection had been closed at that moment.
type wrGatedReplyConn struct {
	*rbConn
	entered       chan struct{}
	once          sync.Once
	gate          *rbGate
	closesAtReply atomic.Int32
}

func newWrGatedReplyConn() *wrGatedReplyConn {
	return &wrGatedReplyConn{rbConn: newRbConn(), entered: make(chan struct{}), gate: newRbGate()}
}

func (c *wrGatedReplyConn) PublishMsg(m *natsgo.Msg) error {
	if m.Subject == rbInbox {
		c.once.Do(func() { close(c.entered) })
		<-c.gate.ch
		c.closesAtReply.Store(int32(c.closeCount()))
	}
	return c.rbConn.PublishMsg(m)
}

func TestNatsSync_PanicReplyBeforeShutdownCloses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newWrGatedReplyConn()
		defer conn.gate.open()
		reg := wrOnErrRegistry()
		handlerGate := newRbGate()
		defer handlerGate.open()
		entered := make(chan struct{})
		rec := &noRecorder{}
		bus := &Bus{nc: conn, reg: reg, timeout: time.Second, observer: rec}

		err := bus.SubscribeSync(context.Background(), "rb.order", func(context.Context, event.Event) error {
			close(entered)
			<-handlerGate.ch
			return errors.New("wr: failed")
		}, event.WithHandlerName("h"))
		if err != nil {
			t.Fatalf("SubscribeSync: %v", err)
		}

		cbDone := rbDeliver(t, conn.rbConn, "rb.order", &natsgo.Msg{Subject: "rb.order", Data: rbPayload(t, reg, "wr-g1"), Reply: rbInbox})
		<-entered

		shutDone := make(chan error, 1)
		go func() { shutDone <- bus.Shutdown(context.Background()) }()
		synctest.Wait()
		select {
		case err := <-shutDone:
			t.Fatalf("Shutdown returned %v while a handler was in flight", err)
		default:
		}
		if n := conn.closeCount(); n != 0 {
			t.Fatalf("conn closed %d times during the drain, want 0", n)
		}

		// The handler fails; the OnErr panic outside the handler is contained by
		// Settle and the failure reply blocks on the gate.
		handlerGate.open()
		<-conn.entered
		synctest.Wait()
		select {
		case err := <-shutDone:
			t.Fatalf("Shutdown returned %v before the panic reply went out", err)
		default:
		}
		if n := conn.closeCount(); n != 0 {
			t.Fatalf("conn closed %d times before the panic reply went out, want 0", n)
		}

		conn.gate.open()
		if err := <-shutDone; err != nil {
			t.Errorf("Shutdown = %v, want nil", err)
		}
		rbWait(t, cbDone)

		if n := conn.closesAtReply.Load(); n != 0 {
			t.Errorf("conn had been closed %d times when the reply went out, want 0", n)
		}
		if n := conn.closeCount(); n != 1 {
			t.Errorf("conn closed %d times, want 1", n)
		}
		conn.mu.Lock()
		var inbox []*natsgo.Msg
		for _, m := range conn.replies {
			if m.Subject == rbInbox {
				inbox = append(inbox, m)
			}
		}
		conn.mu.Unlock()
		if len(inbox) != 1 {
			t.Fatalf("messages published to %s = %d, want 1", rbInbox, len(inbox))
		}
		if v := inbox[0].Header.Get(HeaderError); v != event.ReasonDeliveryExhausted {
			t.Errorf("reply %s = %q, want %q", HeaderError, v, event.ReasonDeliveryExhausted)
		}
		if len(inbox[0].Data) != 0 {
			t.Errorf("reply data = %q, want empty", inbox[0].Data)
		}
		if !noPanicReported(rec, "wr: onerr boom") {
			t.Errorf("no InternalError panic signal with value %q", "wr: onerr boom")
		}
	})
}

func TestNatsBus_RejectedDeliveryPanicRecovered(t *testing.T) {
	rows := []struct {
		name string
		sync bool
	}{
		{"Subscribe", false},
		{"SubscribeSync", true},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				conn := newRbConn()
				reg := newMdRegistry("rb.order")
				rec := &noRecorder{}
				bus := &Bus{nc: conn, reg: reg, timeout: time.Second, observer: wrPanicObserver{rec}}
				handler := func(context.Context, event.Event) error {
					t.Error("handler ran for a rejected delivery")
					return nil
				}
				var err error
				if row.sync {
					err = bus.SubscribeSync(context.Background(), "rb.order", handler, event.WithHandlerName("h"))
				} else {
					err = bus.Subscribe(context.Background(), "rb.order", handler, event.WithHandlerName("h"))
				}
				if err != nil {
					t.Fatalf("subscribe: %v", err)
				}
				if err := bus.Shutdown(context.Background()); err != nil {
					t.Fatalf("Shutdown = %v, want nil", err)
				}

				// Track refuses the message now; the observer panics inside rejectInbound.
				// The callback returns normally: a propagated panic aborts the binary.
				msg := &natsgo.Msg{Subject: "rb.order", Data: rbPayload(t, reg, "wr-r1")}
				if row.sync {
					msg.Reply = rbInbox
				}
				conn.callback(t, "rb.order")(msg)

				// WithObserver isolates the raw observer, so its panic never reaches
				// the callback: the rejection reply goes out as for any observer.
				if noPanicReported(rec, "wr: onerr boom") {
					t.Errorf("InternalError panic signal for an isolated observer panic")
				}
				conn.mu.Lock()
				var inbox []*natsgo.Msg
				for _, m := range conn.replies {
					if m.Subject == rbInbox {
						inbox = append(inbox, m)
					}
				}
				conn.mu.Unlock()
				if row.sync {
					if len(inbox) != 1 {
						t.Fatalf("messages published to %s = %d, want 1", rbInbox, len(inbox))
					}
					if v := inbox[0].Header.Get(HeaderError); v != event.ReasonInterrupted {
						t.Errorf("reply %s = %q, want %q", HeaderError, v, event.ReasonInterrupted)
					}
				} else if len(inbox) != 0 {
					t.Errorf("messages published to %s = %d, want 0", rbInbox, len(inbox))
				}
				if err := bus.Shutdown(context.Background()); err != nil {
					t.Errorf("second Shutdown = %v, want nil", err)
				}
			})
		})
	}
}

func TestNatsBus_PanicDuringShutdownLoggedBeforeReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newRbConn()
		reg := wrOnErrRegistry()
		gate := newRbGate()
		defer gate.open()
		entered := make(chan struct{})
		rec := &noRecorder{}
		bus := &Bus{nc: conn, reg: reg, timeout: time.Second, observer: rec}

		err := bus.Subscribe(context.Background(), "rb.order", func(context.Context, event.Event) error {
			close(entered)
			<-gate.ch
			return errors.New("wr: failed")
		}, event.WithHandlerName("h"))
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		cbDone := rbDeliver(t, conn, "rb.order", &natsgo.Msg{Subject: "rb.order", Data: rbPayload(t, reg, "wr-d1")})
		<-entered

		shutDone := make(chan error, 1)
		go func() { shutDone <- bus.Shutdown(context.Background()) }()
		synctest.Wait()
		select {
		case err := <-shutDone:
			t.Fatalf("Shutdown returned %v while a handler was in flight", err)
		default:
		}

		gate.open()
		if err := <-shutDone; err != nil {
			t.Errorf("Shutdown = %v, want nil", err)
		}
		// The record is logged before release, and release happens before Wait returns.
		if !noPanicReported(rec, "wr: onerr boom") {
			t.Errorf("no InternalError panic signal with value %q", "wr: onerr boom")
		}
		rbWait(t, cbDone)
		if n := conn.closeCount(); n != 1 {
			t.Errorf("conn closed %d times, want 1", n)
		}
	})
}

// --- drain/unsubscribe InternalError and default observer flush (D-25 WR-04, WR-05) ---

// dcLog is one captured slog record.
type dcLog struct {
	msg   string
	attrs map[string]any
}

// dcCapture is a minimal slog.Handler recording every record with its attrs.
type dcCapture struct {
	mu   *sync.Mutex
	recs *[]dcLog
}

func newDcCapture() dcCapture { return dcCapture{mu: &sync.Mutex{}, recs: &[]dcLog{}} }

func (c dcCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c dcCapture) Handle(_ context.Context, r slog.Record) error {
	rec := dcLog{msg: r.Message, attrs: map[string]any{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.Any()
		return true
	})
	c.mu.Lock()
	*c.recs = append(*c.recs, rec)
	c.mu.Unlock()
	return nil
}

func (c dcCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c dcCapture) WithGroup(string) slog.Handler      { return c }

func (c dcCapture) records() []dcLog {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]dcLog(nil), *c.recs...)
}

// dcInstallDefault installs a capturing default logger until the test ends. A
// test that uses it must not run in parallel.
func dcInstallDefault(t *testing.T) dcCapture {
	t.Helper()
	c := newDcCapture()
	prev := slog.Default()
	slog.SetDefault(slog.New(c))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return c
}

// dcSummaries returns the dedup summary records of capt.
func dcSummaries(capt dcCapture) []dcLog {
	var out []dcLog
	for _, r := range capt.records() {
		if r.msg == "eventbus: repeated log records suppressed" {
			out = append(out, r)
		}
	}
	return out
}

func TestNatsBus_DrainFailureIsInternalError(t *testing.T) {
	capt := dcInstallDefault(t)
	rec := &noRecorder{}
	bus := &Bus{nc: newRbConn(), reg: newMdRegistry("dc.order"), timeout: time.Second, observer: rec}
	bus.subs = append(bus.subs, trackedSub{
		subject: "dc.sub",
		drain:   func() error { return errors.New("dc: drain boom") },
		done:    make(chan struct{}),
	})

	if err := bus.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown = %v, want nil", err)
	}
	ies := rec.internalErrors()
	if len(ies) != 1 {
		t.Fatalf("InternalError calls = %d, want 1: %+v", len(ies), ies)
	}
	if ies[0].Op != event.OpDrain || ies[0].Subject != "dc.sub" || ies[0].Component != event.ComponentTransport {
		t.Errorf("InternalError = %+v, want Op %q on subject dc.sub", ies[0], event.OpDrain)
	}
	if ies[0].Err == nil {
		t.Error("InternalError.Err = nil")
	}
	if n := len(capt.records()); n != 0 {
		t.Errorf("direct slog records = %d, want 0: %+v", n, capt.records())
	}
}

func TestNatsBus_DrainFailureSilencedByNopObserver(t *testing.T) {
	capt := dcInstallDefault(t)
	bus := &Bus{nc: newRbConn(), reg: newMdRegistry("dc.order"), timeout: time.Second, observer: event.NopObserver{}}
	bus.subs = append(bus.subs, trackedSub{
		subject: "dc.sub",
		drain:   func() error { return errors.New("dc: drain boom") },
		done:    make(chan struct{}),
	})

	if err := bus.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown = %v, want nil", err)
	}
	if recs := capt.records(); len(recs) != 0 {
		t.Errorf("slog records with NopObserver = %+v, want none", recs)
	}
}

func TestNatsBus_UnsubscribeAfterCloseIsInternalError(t *testing.T) {
	capt := dcInstallDefault(t)
	rec := &noRecorder{}
	bus := &Bus{nc: newRbConn(), reg: newMdRegistry("dc.order"), timeout: time.Second, observer: rec}
	bus.life.BeginShutdown()

	// A subscription with no connection: Unsubscribe fails with a connection error.
	sub := &natsgo.Subscription{Subject: "dc.late"}
	if err := bus.trackSub(context.Background(), sub); !errors.Is(err, event.ErrBusClosed) {
		t.Fatalf("trackSub = %v, want event.ErrBusClosed", err)
	}
	ies := rec.internalErrors()
	if len(ies) != 1 {
		t.Fatalf("InternalError calls = %d, want 1: %+v", len(ies), ies)
	}
	if ies[0].Op != event.OpUnsubscribe || ies[0].Subject != "dc.late" || ies[0].Err == nil {
		t.Errorf("InternalError = %+v, want Op %q on subject dc.late with an error", ies[0], event.OpUnsubscribe)
	}
	if n := len(capt.records()); n != 0 {
		t.Errorf("direct slog records = %d, want 0: %+v", n, capt.records())
	}
}

// dcThreeDrops delivers the same terminal failure three times.
func dcThreeDrops(t *testing.T, conn *fsLoopConn, reg *mdRegistry) {
	t.Helper()
	for i := 0; i < 3; i++ {
		conn.deliver(t, "dc.order", fsRequestBytes(t, reg, "dc.order"))
	}
}

func dcCheckSummary(t *testing.T, capt dcCapture) {
	t.Helper()
	sums := dcSummaries(capt)
	if len(sums) != 1 {
		t.Fatalf("summary records = %d, want 1: %+v", len(sums), capt.records())
	}
	if got := sums[0].attrs["suppressed"]; got != int64(2) {
		t.Errorf("summary suppressed = %v, want 2", got)
	}
}

// TestNatsBus_ShutdownFlushesDefaultObserver: the default SlogObserver of a bus
// built without Observer logs its pending dedup summary on Shutdown and Close
// (WR-05).
func TestNatsBus_ShutdownFlushesDefaultObserver(t *testing.T) {
	for _, closeIt := range []bool{false, true} {
		name := "Shutdown"
		if closeIt {
			name = "Close"
		}
		t.Run(name, func(t *testing.T) {
			capt := dcInstallDefault(t)
			conn := newFsLoopConn()
			reg := newMdRegistry("dc.order")
			bus := newBus(conn, reg, BusOptions{})
			if err := bus.Subscribe(context.Background(), "dc.order", noTerminalHandler, event.WithHandlerName("h")); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			dcThreeDrops(t, conn, reg)

			if n := len(dcSummaries(capt)); n != 0 {
				t.Fatalf("summaries before Shutdown = %d, want 0", n)
			}
			var err error
			if closeIt {
				err = bus.Close()
			} else {
				err = bus.Shutdown(context.Background())
			}
			if err != nil {
				t.Fatalf("%s = %v", name, err)
			}
			dcCheckSummary(t, capt)
		})
	}
}

// TestNatsBus_ShutdownFlushesOnExpiredContext: the flush also runs when the
// Shutdown ctx has already expired.
func TestNatsBus_ShutdownFlushesOnExpiredContext(t *testing.T) {
	capt := dcInstallDefault(t)
	conn := newFsLoopConn()
	reg := newMdRegistry("dc.order")
	bus := newBus(conn, reg, BusOptions{})
	if err := bus.Subscribe(context.Background(), "dc.order", noTerminalHandler, event.WithHandlerName("h")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	dcThreeDrops(t, conn, reg)
	// A tracked subscription that never finishes draining forces the ctx path.
	bus.subs = append(bus.subs, trackedSub{
		subject: "dc.stuck",
		drain:   func() error { return nil },
		done:    make(chan struct{}),
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bus.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown = %v, want context.Canceled", err)
	}
	dcCheckSummary(t, capt)
}

// TestNatsBus_ShutdownLeavesUserObserver: an Observer supplied by the caller is
// never flushed by the bus.
func TestNatsBus_ShutdownLeavesUserObserver(t *testing.T) {
	capt := newDcCapture()
	conn := newFsLoopConn()
	reg := newMdRegistry("dc.order")
	bus := newBus(conn, reg, BusOptions{Observer: event.NewSlogObserver(slog.New(capt))})
	if err := bus.Subscribe(context.Background(), "dc.order", noTerminalHandler, event.WithHandlerName("h")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	dcThreeDrops(t, conn, reg)

	if err := bus.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown = %v", err)
	}
	if n := len(dcSummaries(capt)); n != 0 {
		t.Errorf("summaries after Shutdown = %d, want 0: the bus does not flush a user observer", n)
	}
}
