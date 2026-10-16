//go:build integration

package jetstream

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Integration helpers: reusable by the smoke test and by later phases that
// need a real nats-server -js. Run with:
//
//	NATS_URL=nats://localhost:4222 go test -tags integration -race ./...
//
// A missing server fails the test (connect error); nothing here ever skips.

var nameSeq atomic.Uint64

// natsURL returns NATS_URL from the environment, else nats.DefaultURL.
func natsURL(t testing.TB) string {
	t.Helper()
	if u := os.Getenv("NATS_URL"); u != "" {
		return u
	}
	return nats.DefaultURL
}

// uniqueName returns a per-run name safe for stream and consumer names
// (never contains '.', '*' or '>').
func uniqueName(t testing.TB, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano(), nameSeq.Add(1))
}

// connect opens a NATS connection (closed on cleanup) and a JetStream handle.
func connect(t testing.TB) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	nc, err := nats.Connect(natsURL(t), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("connect to %s: %v", natsURL(t), err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return nc, js
}

// newStream creates a memory-backed stream with a unique name and deletes it
// on cleanup. It returns the stream name.
func newStream(t testing.TB, js jetstream.JetStream, subjects ...string) string {
	t.Helper()
	name := uniqueName(t, "S")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     name,
		Subjects: subjects,
		Storage:  jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("create stream %s: %v", name, err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dcancel()
		_ = js.DeleteStream(dctx, name)
	})
	return name
}

// testRegistry is a minimal event.Registry so jetstream tests do not need a
// dependency on the json module.
type testRegistry struct {
	mu        sync.RWMutex
	factories map[string]event.Factory
}

func newTestRegistry() *testRegistry {
	return &testRegistry{factories: make(map[string]event.Factory)}
}

func (r *testRegistry) Register(name string, factory event.Factory, _ int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[name] = factory
}

// Encode resolves the outbound Metadata from ctx (a parent in ctx becomes the
// causation) and encodes evt with it.
func (r *testRegistry) Encode(ctx context.Context, evt event.Event) ([]byte, error) {
	return r.EncodeWith(ctx, evt, event.ResolveOutbound(ctx, evt, event.EmitOptions{}))
}

// EncodeWith implements event.EnvelopeEncoder: the bus resolves the Metadata
// (id, correlation, causation, headers) and the registry only serializes it.
func (r *testRegistry) EncodeWith(_ context.Context, evt event.Event, md event.Metadata) ([]byte, error) {
	payload, err := json.Marshal(evt)
	if err != nil {
		return nil, err
	}
	ts := md.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	return json.Marshal(event.Envelope{
		Name:          evt.Name(),
		Version:       1,
		Timestamp:     ts,
		ID:            md.ID,
		CorrelationID: md.CorrelationID,
		CausationID:   md.CausationID,
		Headers:       md.Headers,
		Payload:       payload,
	})
}

// Decode returns a ctx that carries the envelope Metadata, and typed
// *event.DecodeError failures so the bus classifies them without a json module.
func (r *testRegistry) Decode(ctx context.Context, data []byte) (context.Context, event.Event, error) {
	var env event.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return ctx, nil, &event.DecodeError{Err: fmt.Errorf("%w: %w", event.ErrMalformedEnvelope, err)}
	}
	r.mu.RLock()
	factory, ok := r.factories[env.Name]
	r.mu.RUnlock()
	if !ok {
		return ctx, nil, &event.DecodeError{Name: env.Name, Version: env.Version, Err: event.ErrUnknownEvent}
	}
	evt := factory()
	if err := json.Unmarshal(env.Payload, evt); err != nil {
		return ctx, nil, &event.DecodeError{
			Name:    env.Name,
			Version: env.Version,
			Err:     fmt.Errorf("%w: %w", event.ErrMalformedEnvelope, err),
		}
	}
	md := event.Metadata{
		ID:            env.ID,
		CorrelationID: env.CorrelationID,
		CausationID:   env.CausationID,
		Name:          env.Name,
		Version:       env.Version,
		Timestamp:     env.Timestamp,
		Headers:       env.Headers,
	}
	return event.WithMetadata(ctx, md), evt, nil
}

// itOrder is the event of the failure-semantics integration tests. Its Id is
// the message id, so the idempotency key is ID + ":" + handler.
type itOrder struct {
	ID string `json:"id"`
}

func (e *itOrder) Name() string { return "it.order" }
func (e *itOrder) Id() string   { return e.ID }

// itRegistry returns a testRegistry that knows itOrder.
func itRegistry() *testRegistry {
	reg := newTestRegistry()
	reg.Register("it.order", func() event.Event { return &itOrder{} }, 1)
	return reg
}

// itNewDLQStream creates a memory DLQ stream on a unique prefix and returns the
// prefix (with its trailing dot, as NewDLQ takes it) and the stream name.
func itNewDLQStream(t testing.TB, js jetstream.JetStream) (prefix, stream string) {
	t.Helper()
	base := uniqueName(t, "dlq")
	return base + ".", newStream(t, js, base+".>")
}

// itStreamMsg returns the raw stream message at seq.
func itStreamMsg(t testing.TB, js jetstream.JetStream, stream string, seq uint64) *jetstream.RawStreamMsg {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("stream %s: %v", stream, err)
	}
	m, err := s.GetMsg(ctx, seq)
	if err != nil {
		t.Fatalf("get message %d of stream %s: %v", seq, stream, err)
	}
	return m
}

// itStreamMsgsErr returns the number of messages currently in stream. Unlike
// itStreamMsgs it reports a failure instead of failing the test, so it is safe
// to call from a handler goroutine.
func itStreamMsgsErr(js jetstream.JetStream, stream string) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		return 0, err
	}
	info, err := s.Info(ctx)
	if err != nil {
		return 0, err
	}
	return info.State.Msgs, nil
}

// itStreamMsgs returns the number of messages currently in stream.
func itStreamMsgs(t testing.TB, js jetstream.JetStream, stream string) uint64 {
	t.Helper()
	n, err := itStreamMsgsErr(js, stream)
	if err != nil {
		t.Fatalf("stream %s info: %v", stream, err)
	}
	return n
}

// itWaitStreamMsgs polls the stream every 50ms for up to 10s until it holds at
// least n messages, and fails the test with the last count on timeout.
func itWaitStreamMsgs(t testing.TB, js jetstream.JetStream, stream string, n uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last uint64
	for {
		last = itStreamMsgs(t, js, stream)
		if last >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %d messages in stream %s; last state: %d messages", n, stream, last)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// itAdvisory is the part of a JetStream consumer advisory the tests assert on.
type itAdvisory struct {
	Reason     string `json:"reason"`
	Deliveries uint64 `json:"deliveries"`
}

// itAdvisories subscribes to subject and returns the payloads it receives. The
// subscription is flushed to the server before it returns, so an advisory
// raised afterwards is never missed. It is removed on cleanup.
func itAdvisories(t testing.TB, nc *nats.Conn, subject string) <-chan []byte {
	t.Helper()
	ch := make(chan []byte, 64)
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		select {
		case ch <- append([]byte(nil), m.Data...):
		default:
		}
	})
	if err != nil {
		t.Fatalf("subscribe %s: %v", subject, err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush after subscribing to %s: %v", subject, err)
	}
	return ch
}

// itNextAdvisory waits up to d for the next advisory on ch and decodes it.
func itNextAdvisory(t testing.TB, ch <-chan []byte, d time.Duration) itAdvisory {
	t.Helper()
	select {
	case data := <-ch:
		var a itAdvisory
		if err := json.Unmarshal(data, &a); err != nil {
			t.Fatalf("decode advisory %q: %v", data, err)
		}
		return a
	case <-time.After(d):
		t.Fatalf("no advisory within %s", d)
		return itAdvisory{}
	}
}

// waitConsumerInfo polls the consumer info every 50ms for up to 5s until cond
// is true, and fails the test with the last info on timeout.
func waitConsumerInfo(
	t testing.TB,
	js jetstream.JetStream,
	stream, consumer string,
	cond func(*jetstream.ConsumerInfo) bool,
) *jetstream.ConsumerInfo {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last *jetstream.ConsumerInfo
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		c, err := js.Consumer(ctx, stream, consumer)
		if err == nil {
			var info *jetstream.ConsumerInfo
			info, err = c.Info(ctx)
			if err == nil {
				last = info
				if cond(info) {
					cancel()
					return info
				}
			}
		}
		cancel()
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for consumer %s/%s condition; last info: %+v", stream, consumer, last)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
