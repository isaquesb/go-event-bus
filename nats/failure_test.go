package nats

import (
	"bytes"
	"context"
	"errors"
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
