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

// rbErrs records OnErr reports.
type rbErrs struct {
	mu   sync.Mutex
	errs []error
	evts []event.Event
}

func (r *rbErrs) onErr(_ context.Context, evt event.Event, err error, _ string) {
	r.mu.Lock()
	r.errs = append(r.errs, err)
	r.evts = append(r.evts, evt)
	r.mu.Unlock()
}

func (r *rbErrs) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.errs)
}

func (r *rbErrs) at(t *testing.T, i int) (event.Event, error) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if i >= len(r.errs) {
		t.Fatalf("OnErr was called %d times, want index %d", len(r.errs), i)
	}
	return r.evts[i], r.errs[i]
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
	errs := &rbErrs{}
	bus := &Bus{nc: conn, reg: reg, timeout: time.Second, dlq: dlq, onErr: errs.onErr}

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
	if errs.count() != 1 {
		t.Fatalf("OnErr calls = %d, want 1", errs.count())
	}
	_, err := errs.at(t, 0)
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
		errs := &rbErrs{}
		bus := &Bus{nc: conn, reg: reg, timeout: time.Second, dlq: dlq, onErr: errs.onErr}
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
		if errs.count() != 1 {
			t.Fatalf("OnErr calls = %d, want 1", errs.count())
		}
		if _, err := errs.at(t, 0); !errors.Is(err, event.ErrShutdownInterrupted) {
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
		errs := &rbErrs{}
		bus := &Bus{nc: conn, reg: reg, timeout: time.Second, dlq: dlq, onErr: errs.onErr}
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
		before := errs.count()

		// A Subscribe delivery after the forced cancel is reported, not run.
		conn.callback(t, "rb.late")(&natsgo.Msg{Subject: "rb.late", Data: rbPayload(t, reg, "rb-late")})
		if errs.count() != before+1 {
			t.Fatalf("OnErr calls = %d, want %d", errs.count(), before+1)
		}
		evt, err := errs.at(t, before)
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
		errs := &rbErrs{}
		bus := &Bus{nc: conn, reg: reg, timeout: time.Second, dlq: dlq, onErr: errs.onErr}
		if err := bus.Subscribe(context.Background(), "rb.late", noopHandler); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
		conn.callback(t, "rb.late")(&natsgo.Msg{Subject: "rb.late", Data: rbPayload(t, reg, "rb-late")})
		if errs.count() != 1 {
			t.Fatalf("OnErr calls = %d, want 1", errs.count())
		}
		if _, err := errs.at(t, 0); !errors.Is(err, event.ErrShutdownInterrupted) {
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

// wrRecord is one captured log record.
type wrRecord struct {
	level slog.Level
	msg   string
	attrs map[string]string
}

// wrLogs is a slog.Handler capturing every record.
type wrLogs struct {
	mu   sync.Mutex
	recs []wrRecord
}

func (l *wrLogs) Enabled(context.Context, slog.Level) bool { return true }

func (l *wrLogs) Handle(_ context.Context, r slog.Record) error {
	rec := wrRecord{level: r.Level, msg: r.Message, attrs: map[string]string{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.String()
		return true
	})
	l.mu.Lock()
	l.recs = append(l.recs, rec)
	l.mu.Unlock()
	return nil
}

func (l *wrLogs) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *wrLogs) WithGroup(string) slog.Handler      { return l }

// errorWithPanic reports whether an Error record carries attrs["panic"] == text.
func (l *wrLogs) errorWithPanic(text string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range l.recs {
		if r.level == slog.LevelError && r.attrs["panic"] == text {
			return true
		}
	}
	return false
}

// wrInstallLogs makes a fresh wrLogs the slog default until the test ends. Call
// it outside any synctest bubble.
func wrInstallLogs(t *testing.T) *wrLogs {
	t.Helper()
	logs := &wrLogs{}
	prev := slog.Default()
	slog.SetDefault(slog.New(logs))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return logs
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

func wrOnErrPanic(context.Context, event.Event, error, string) { panic("wr: onerr boom") }

func wrFailHandler(context.Context, event.Event) error { return errors.New("wr: failed") }

func TestNatsBus_CallbackPanicRecovered(t *testing.T) {
	rows := []struct {
		name  string
		panic string
		build func(conn *rbConn, reg *mdRegistry) *Bus
	}{
		{"bus OnErr", "wr: onerr boom", func(conn *rbConn, reg *mdRegistry) *Bus {
			return &Bus{nc: conn, reg: reg, timeout: time.Second, onErr: wrOnErrPanic}
		}},
		{"DLQ publisher", "wr: dlq boom", func(conn *rbConn, reg *mdRegistry) *Bus {
			return &Bus{nc: conn, reg: reg, timeout: time.Second, dlq: wrPanicDLQ{}}
		}},
		{"decode", "wr: decode boom", func(conn *rbConn, reg *mdRegistry) *Bus {
			return &Bus{nc: conn, reg: wrPanicRegistry{reg}, timeout: time.Second}
		}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			logs := wrInstallLogs(t)
			synctest.Test(t, func(t *testing.T) {
				conn := newRbConn()
				reg := newMdRegistry("rb.order")
				bus := row.build(conn, reg)
				if err := bus.Subscribe(context.Background(), "rb.order", wrFailHandler, event.WithHandlerName("h")); err != nil {
					t.Fatalf("Subscribe: %v", err)
				}

				// The callback returns normally: a propagated panic aborts the binary.
				conn.callback(t, "rb.order")(&natsgo.Msg{Subject: "rb.order", Data: rbPayload(t, reg, "wr-1")})

				if !logs.errorWithPanic(row.panic) {
					t.Errorf("no Error record with panic %q", row.panic)
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
		bus := &Bus{nc: conn, reg: reg, timeout: time.Second, onErr: wrOnErrPanic}
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
	logs := wrInstallLogs(t)
	synctest.Test(t, func(t *testing.T) {
		reg := newMdRegistry("rb.order")
		conn := &wrPanicReplyConn{rbConn: newRbConn()}
		bus := &Bus{nc: conn, reg: reg, timeout: time.Second, onErr: wrOnErrPanic}
		if err := bus.SubscribeSync(context.Background(), "rb.order", wrFailHandler, event.WithHandlerName("h")); err != nil {
			t.Fatalf("SubscribeSync: %v", err)
		}

		// The callback returns normally: a propagated panic aborts the binary.
		conn.callback(t, "rb.order")(&natsgo.Msg{Subject: "rb.order", Data: rbPayload(t, reg, "wr-s2"), Reply: rbInbox})

		if !logs.errorWithPanic("wr: onerr boom") {
			t.Error("no Error record with panic \"wr: onerr boom\"")
		}
		if !logs.errorWithPanic("wr: reply boom") {
			t.Error("no Error record with panic \"wr: reply boom\"")
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
