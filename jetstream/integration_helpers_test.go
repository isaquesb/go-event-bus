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

func (r *testRegistry) Encode(_ context.Context, evt event.Event) ([]byte, error) {
	payload, err := json.Marshal(evt)
	if err != nil {
		return nil, err
	}
	return json.Marshal(event.Envelope{
		Name:      evt.Name(),
		Version:   1,
		Timestamp: time.Now(),
		Payload:   payload,
	})
}

func (r *testRegistry) Decode(ctx context.Context, data []byte) (context.Context, event.Event, error) {
	var env event.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return ctx, nil, err
	}
	r.mu.RLock()
	factory, ok := r.factories[env.Name]
	r.mu.RUnlock()
	if !ok {
		return ctx, nil, fmt.Errorf("unknown event %q", env.Name)
	}
	evt := factory()
	if err := json.Unmarshal(env.Payload, evt); err != nil {
		return ctx, nil, err
	}
	return ctx, evt, nil
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
