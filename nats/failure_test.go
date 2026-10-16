package nats

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	natsgo "github.com/nats-io/nats.go"
)

// --- fakes ---

// fsLoopConn is a loopback natsConn with request/reply: Request invokes the
// callback subscribed to the subject synchronously and returns the last message
// the callback sent with PublishMsg to the inbox.
type fsLoopConn struct {
	mu       sync.Mutex
	handlers map[string]natsgo.MsgHandler
	requests [][]byte
	replies  []*natsgo.Msg
}

const fsInbox = "_INBOX.fs"

func newFsLoopConn() *fsLoopConn { return &fsLoopConn{handlers: map[string]natsgo.MsgHandler{}} }

func (c *fsLoopConn) Subscribe(subj string, cb natsgo.MsgHandler) (*natsgo.Subscription, error) {
	c.mu.Lock()
	c.handlers[subj] = cb
	c.mu.Unlock()
	return nil, nil
}

func (c *fsLoopConn) Publish(string, []byte) error { return nil }

func (c *fsLoopConn) Request(subj string, data []byte, _ time.Duration) (*natsgo.Msg, error) {
	c.mu.Lock()
	cb := c.handlers[subj]
	c.requests = append(c.requests, data)
	start := len(c.replies)
	c.mu.Unlock()
	if cb == nil {
		return nil, natsgo.ErrNoResponders
	}
	cb(&natsgo.Msg{Subject: subj, Data: data, Reply: fsInbox})

	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.replies) - 1; i >= start; i-- {
		if c.replies[i].Subject == fsInbox {
			return c.replies[i], nil
		}
	}
	return nil, natsgo.ErrTimeout
}

func (c *fsLoopConn) PublishMsg(m *natsgo.Msg) error {
	c.mu.Lock()
	c.replies = append(c.replies, m)
	c.mu.Unlock()
	return nil
}

func (c *fsLoopConn) Close() {}

// deliver hands data to the callback subscribed to subj, as an async publish.
func (c *fsLoopConn) deliver(t *testing.T, subj string, data []byte) {
	t.Helper()
	c.mu.Lock()
	cb := c.handlers[subj]
	c.mu.Unlock()
	if cb == nil {
		t.Fatalf("no handler subscribed to %q", subj)
	}
	cb(&natsgo.Msg{Subject: subj, Data: data})
}

func (c *fsLoopConn) lastReply(t *testing.T) *natsgo.Msg {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.replies) == 0 {
		t.Fatal("no reply was sent")
	}
	return c.replies[len(c.replies)-1]
}

func (c *fsLoopConn) lastRequest(t *testing.T) []byte {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.requests) == 0 {
		t.Fatal("no request was sent")
	}
	return c.requests[len(c.requests)-1]
}

// fsPermanent is a terminal error: retrying cannot help.
type fsPermanent struct{ msg string }

func (e *fsPermanent) Error() string  { return e.msg }
func (e *fsPermanent) Terminal() bool { return true }

// fsOnErr counts OnErr calls and keeps the last error and event.
type fsOnErr struct {
	mu    sync.Mutex
	n     int
	err   error
	evt   event.Event
	hName string
}

func (o *fsOnErr) fn(_ context.Context, evt event.Event, err error, handler string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.n++
	o.err = err
	o.evt = evt
	o.hName = handler
}

func (o *fsOnErr) snapshot() (int, error, event.Event, string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.n, o.err, o.evt, o.hName
}

// --- tracer ---

func TestNatsSync_TerminalFailureRepliesEventbusError(t *testing.T) {
	conn := newFsLoopConn()
	dlq := event.NewMemoryDLQ()
	var oe fsOnErr
	bus := &Bus{
		nc:      conn,
		reg:     newMdRegistry("quote.get"),
		timeout: time.Second,
		dlq:     dlq,
		onErr:   oe.fn,
	}

	err := bus.SubscribeSync(context.Background(), "quote.get", func(context.Context, event.Event) error {
		return &fsPermanent{msg: "boom: secret internal detail"}
	}, event.WithHandlerName("h"))
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	got, err := bus.EmitSync(context.Background(), &mdEvent{name: "quote.get", EventID: "q-1"})
	if got != nil {
		t.Errorf("EmitSync event = %v, want nil", got)
	}
	var remote *RemoteError
	if !errors.As(err, &remote) {
		t.Fatalf("EmitSync error = %v (%T), want *RemoteError", err, err)
	}
	if remote.Reason != event.ReasonTerminal {
		t.Errorf("Reason = %q, want %q", remote.Reason, event.ReasonTerminal)
	}
	if remote.Message != "" {
		t.Errorf("Message = %q, want empty by default (D-30)", remote.Message)
	}
	for _, target := range []error{ErrRemoteFailure, ErrRemoteHandler, event.ErrSendToDLQ} {
		if !errors.Is(err, target) {
			t.Errorf("errors.Is(err, %v) = false, want true", target)
		}
	}
	if errors.Is(err, ErrRemoteDecode) {
		t.Error("errors.Is(err, ErrRemoteDecode) = true, want false")
	}

	reply := conn.lastReply(t)
	if len(reply.Data) != 0 {
		t.Errorf("error reply body = %q, want empty", reply.Data)
	}
	if v := reply.Header.Get(HeaderError); v != "terminal" {
		t.Errorf("%s = %q, want terminal", HeaderError, v)
	}

	recs := dlq.Records()
	if len(recs) != 1 {
		t.Fatalf("DLQ records = %d, want 1", len(recs))
	}
	if !bytes.Equal(recs[0].Raw, conn.lastRequest(t)) {
		t.Errorf("DLQ Raw = %q, want the request bytes %q", recs[0].Raw, conn.lastRequest(t))
	}
	if recs[0].Reason != event.ReasonTerminal || recs[0].Handler != "h" {
		t.Errorf("DLQ reason/handler = %q/%q, want terminal/h", recs[0].Reason, recs[0].Handler)
	}

	n, oerr, _, _ := oe.snapshot()
	if n != 1 {
		t.Fatalf("OnErr calls = %d, want 1", n)
	}
	if !errors.Is(oerr, event.ErrDeadLettered) {
		t.Errorf("OnErr error = %v, want it to match event.ErrDeadLettered", oerr)
	}
}

// --- more fakes ---

// fsDecodeFailRegistry fails every Decode with an unknown-event DecodeError.
type fsDecodeFailRegistry struct{}

func (fsDecodeFailRegistry) Encode(context.Context, event.Event) ([]byte, error) {
	return []byte("not-an-envelope"), nil
}

func (fsDecodeFailRegistry) Decode(ctx context.Context, _ []byte) (context.Context, event.Event, error) {
	return ctx, nil, &event.DecodeError{Name: "x", Version: 1, Err: event.ErrUnknownEvent}
}

func (fsDecodeFailRegistry) Register(string, event.Factory, int) {}

// fsInvoker returns err without calling next.
type fsInvoker struct{ err error }

func (i fsInvoker) Invoke(context.Context, event.Event, string, func(context.Context) error) error {
	return i.err
}

// fsFailingPublisher counts calls and always fails.
type fsFailingPublisher struct {
	mu sync.Mutex
	n  int
}

func (p *fsFailingPublisher) Publish(context.Context, event.DLQRecord) error {
	p.mu.Lock()
	p.n++
	p.mu.Unlock()
	return errors.New("dlq down")
}

func (p *fsFailingPublisher) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n
}

func fsBus(conn *fsLoopConn, reg event.Registry, dlq event.DLQPublisher, oe *fsOnErr, inv event.Invoker) *Bus {
	b := &Bus{nc: conn, reg: reg, timeout: time.Second, dlq: dlq, invoker: inv}
	if oe != nil {
		b.onErr = oe.fn
	}
	return b
}

func fsRequestBytes(t *testing.T, reg *mdRegistry, name string) []byte {
	t.Helper()
	data, err := reg.EncodeWith(context.Background(), &mdEvent{name: name, EventID: "r-1"},
		event.Metadata{ID: "r-1", Timestamp: time.Now().UTC()})
	if err != nil {
		t.Fatalf("EncodeWith: %v", err)
	}
	return data
}

// --- outcome matrix ---

func TestNatsSubscribe_DecodeFailureDeadLettered(t *testing.T) {
	conn := newFsLoopConn()
	dlq := event.NewMemoryDLQ()
	var oe fsOnErr
	bus := fsBus(conn, fsDecodeFailRegistry{}, dlq, &oe, nil)

	called := false
	if err := bus.Subscribe(context.Background(), "x.created", func(context.Context, event.Event) error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	raw := []byte("garbage-bytes")
	conn.deliver(t, "x.created", raw)

	if called {
		t.Error("handler ran for a message that failed to decode")
	}
	recs := dlq.Records()
	if len(recs) != 1 {
		t.Fatalf("DLQ records = %d, want 1", len(recs))
	}
	if !bytes.Equal(recs[0].Raw, raw) {
		t.Errorf("DLQ Raw = %q, want %q", recs[0].Raw, raw)
	}
	if recs[0].Evt != nil {
		t.Errorf("DLQ Evt = %v, want nil", recs[0].Evt)
	}
	if recs[0].Reason != event.ReasonDecodeUnknownEvent {
		t.Errorf("DLQ Reason = %q, want %q", recs[0].Reason, event.ReasonDecodeUnknownEvent)
	}
	n, err, evt, _ := oe.snapshot()
	if n != 1 {
		t.Fatalf("OnErr calls = %d, want 1", n)
	}
	if evt != nil {
		t.Errorf("OnErr evt = %v, want nil", evt)
	}
	var de *event.DecodeError
	if !errors.As(err, &de) || !errors.Is(err, event.ErrDeadLettered) {
		t.Errorf("OnErr error = %v, want a dead-lettered *DecodeError", err)
	}
}

func TestNatsSync_DecodeFailureRepliesEventbusError(t *testing.T) {
	conn := newFsLoopConn()
	dlq := event.NewMemoryDLQ()
	var oe fsOnErr
	bus := fsBus(conn, fsDecodeFailRegistry{}, dlq, &oe, nil)

	if err := bus.SubscribeSync(context.Background(), "x.get", func(context.Context, event.Event) error {
		t.Error("handler ran for a message that failed to decode")
		return nil
	}); err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	got, err := bus.EmitSync(context.Background(), &mdEvent{name: "x.get"})
	if got != nil {
		t.Errorf("EmitSync event = %v, want nil", got)
	}
	for _, target := range []error{ErrRemoteFailure, ErrRemoteDecode, event.ErrUnknownEvent} {
		if !errors.Is(err, target) {
			t.Errorf("errors.Is(err, %v) = false (err = %v)", target, err)
		}
	}
	if errors.Is(err, ErrRemoteHandler) {
		t.Error("a decode failure must not match ErrRemoteHandler")
	}
	if v := conn.lastReply(t).Header.Get(HeaderError); v != event.ReasonDecodeUnknownEvent {
		t.Errorf("%s = %q, want %q", HeaderError, v, event.ReasonDecodeUnknownEvent)
	}
	if dlq.Len() != 1 {
		t.Errorf("DLQ records = %d, want 1", dlq.Len())
	}
	if n, _, _, _ := oe.snapshot(); n != 1 {
		t.Errorf("OnErr calls = %d, want 1", n)
	}
}

func TestNatsSubscribe_DroppedKeepsErrorIdentity(t *testing.T) {
	conn := newFsLoopConn()
	reg := newMdRegistry("order.created")
	var oe fsOnErr
	bus := fsBus(conn, reg, nil, &oe, nil)

	boom := &fsPermanent{msg: "boom"}
	if err := bus.Subscribe(context.Background(), "order.created", func(context.Context, event.Event) error {
		return boom
	}, event.WithHandlerName("h")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	conn.deliver(t, "order.created", fsRequestBytes(t, reg, "order.created"))

	n, err, evt, handler := oe.snapshot()
	if n != 1 {
		t.Fatalf("OnErr calls = %d, want 1", n)
	}
	if err != error(boom) {
		t.Errorf("OnErr error = %v, want the identical handler error without a publisher", err)
	}
	if evt == nil || handler != "h" {
		t.Errorf("OnErr evt/handler = %v/%q, want the event and h", evt, handler)
	}
}

func TestNatsSubscribe_DuplicateNotReported(t *testing.T) {
	conn := newFsLoopConn()
	reg := newMdRegistry("order.created")
	dlq := event.NewMemoryDLQ()
	var oe fsOnErr
	bus := fsBus(conn, reg, dlq, &oe, fsInvoker{err: event.ErrDuplicate})

	if err := bus.Subscribe(context.Background(), "order.created", noopHandler); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	conn.deliver(t, "order.created", fsRequestBytes(t, reg, "order.created"))

	if n, _, _, _ := oe.snapshot(); n != 0 {
		t.Errorf("OnErr calls = %d, want 0 for ErrDuplicate", n)
	}
	if dlq.Len() != 0 {
		t.Errorf("DLQ records = %d, want 0 for ErrDuplicate", dlq.Len())
	}
}

func TestNatsSync_DuplicateRepliesDuplicateReason(t *testing.T) {
	conn := newFsLoopConn()
	reg := newMdRegistry("quote.get")
	var oe fsOnErr
	bus := fsBus(conn, reg, event.NewMemoryDLQ(), &oe, fsInvoker{err: event.ErrDuplicate})

	if err := bus.SubscribeSync(context.Background(), "quote.get", noopHandler); err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	_, err := bus.EmitSync(context.Background(), &mdEvent{name: "quote.get"})

	if !errors.Is(err, event.ErrDuplicate) || !errors.Is(err, ErrRemoteHandler) {
		t.Errorf("EmitSync error = %v, want it to match event.ErrDuplicate and ErrRemoteHandler", err)
	}
	if v := conn.lastReply(t).Header.Get(HeaderError); v != event.ReasonDuplicate {
		t.Errorf("%s = %q, want %q", HeaderError, v, event.ReasonDuplicate)
	}
	if n, _, _, _ := oe.snapshot(); n != 0 {
		t.Errorf("OnErr calls = %d, want 0 for ErrDuplicate", n)
	}
}

func TestNatsSubscribe_DeadLetterFailedReported(t *testing.T) {
	conn := newFsLoopConn()
	reg := newMdRegistry("order.created")
	pub := &fsFailingPublisher{}
	var oe fsOnErr
	bus := fsBus(conn, reg, pub, &oe, nil)

	if err := bus.Subscribe(context.Background(), "order.created", func(context.Context, event.Event) error {
		return &fsPermanent{msg: "boom"}
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	conn.deliver(t, "order.created", fsRequestBytes(t, reg, "order.created"))

	if got := pub.calls(); got != 3 {
		t.Errorf("publish calls = %d, want 3 on the final delivery", got)
	}
	n, err, _, _ := oe.snapshot()
	if n != 1 {
		t.Fatalf("OnErr calls = %d, want 1", n)
	}
	if !errors.Is(err, event.ErrDeadLetterFailed) {
		t.Errorf("OnErr error = %v, want it to match event.ErrDeadLetterFailed", err)
	}
}

func TestNatsSync_ReplyErrorMessageOptIn(t *testing.T) {
	conn := newFsLoopConn()
	reg := newMdRegistry("quote.get")
	bus := fsBus(conn, reg, event.NewMemoryDLQ(), nil, nil)
	bus.replyErrMsg = true

	long := string(bytes.Repeat([]byte("x"), 5000))
	if err := bus.SubscribeSync(context.Background(), "quote.get", func(context.Context, event.Event) error {
		return &fsPermanent{msg: long}
	}); err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	_, err := bus.EmitSync(context.Background(), &mdEvent{name: "quote.get"})
	var remote *RemoteError
	if !errors.As(err, &remote) {
		t.Fatalf("EmitSync error = %v, want *RemoteError", err)
	}
	if remote.Reason != event.ReasonTerminal {
		t.Errorf("Reason = %q, want terminal", remote.Reason)
	}
	if remote.Message == "" {
		t.Error("Message is empty, want the truncated error text when ReplyErrorMessage is true")
	}

	const prefix = "terminal: "
	v := conn.lastReply(t).Header.Get(HeaderError)
	if !strings.HasPrefix(v, prefix) {
		t.Fatalf("%s = %q, want prefix %q", HeaderError, v[:min(len(v), 40)], prefix)
	}
	if got := len(v) - len(prefix); got > event.DLQHeaderValueLimit {
		t.Errorf("message part is %d bytes, want at most %d", got, event.DLQHeaderValueLimit)
	}
	if !strings.HasSuffix(v, event.TruncationMarker) {
		t.Errorf("a 5000-byte message must end with the truncation marker")
	}
	if len(remote.Message) > event.DLQHeaderValueLimit {
		t.Errorf("RemoteError.Message is %d bytes, want at most %d", len(remote.Message), event.DLQHeaderValueLimit)
	}
}

func TestNatsSync_SuccessReplyViaPublishMsg(t *testing.T) {
	conn := newFsLoopConn()
	reg := newMdRegistry("quote.get")
	bus := fsBus(conn, reg, nil, nil, nil)

	if err := bus.SubscribeSync(context.Background(), "quote.get", noopHandler); err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	got, err := bus.EmitSync(context.Background(), &mdEvent{name: "quote.get", Payload: "ask"})
	if err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	if _, ok := got.(*mdEvent); !ok {
		t.Fatalf("EmitSync returned %T, want *mdEvent", got)
	}

	conn.mu.Lock()
	defer conn.mu.Unlock()
	if len(conn.replies) != 1 {
		t.Fatalf("replies = %d, want exactly one through PublishMsg", len(conn.replies))
	}
	r := conn.replies[0]
	if r.Subject != fsInbox {
		t.Errorf("reply subject = %q, want %q", r.Subject, fsInbox)
	}
	if v := r.Header.Get(HeaderError); v != "" {
		t.Errorf("success reply carries %s = %q", HeaderError, v)
	}
	if _, evt, decErr := reg.Decode(context.Background(), r.Data); decErr != nil || evt == nil {
		t.Errorf("success reply does not decode: %v", decErr)
	}
}

func TestNatsSubscribe_HandlerCtxCarriesDelivery(t *testing.T) {
	conn := newFsLoopConn()
	reg := newMdRegistry("order.created")
	bus := fsBus(conn, reg, nil, nil, nil)

	var (
		d    event.Delivery
		dOK  bool
		x    event.Execution
		xOK  bool
		seen int
	)
	if err := bus.Subscribe(context.Background(), "order.created", func(ctx context.Context, _ event.Event) error {
		d, dOK = event.DeliveryFromContext(ctx)
		x, xOK = event.ExecutionFromContext(ctx)
		seen++
		return nil
	}, event.WithHandlerName("h")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	raw := fsRequestBytes(t, reg, "order.created")
	conn.deliver(t, "order.created", raw)

	if seen != 1 || !dOK || !xOK {
		t.Fatalf("handler runs=%d, delivery ok=%v, execution ok=%v; want 1, true, true", seen, dOK, xOK)
	}
	if d.Count != 1 || d.Max != 1 || !d.Final() {
		t.Errorf("Delivery Count/Max/Final = %d/%d/%v, want 1/1/true", d.Count, d.Max, d.Final())
	}
	if !bytes.Equal(d.Raw, raw) {
		t.Errorf("Delivery.Raw = %q, want the message bytes", d.Raw)
	}
	if d.Subject != "order.created" {
		t.Errorf("Delivery.Subject = %q, want order.created", d.Subject)
	}
	if x.Handler != "h" || x.StartedAt.IsZero() {
		t.Errorf("Execution = %+v, want handler h and a start time", x)
	}
}
