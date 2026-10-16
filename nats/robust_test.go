package nats

import (
	"context"
	"errors"
	"sync"
	"testing"
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
