package jetstream

import (
	"context"
	"errors"
	"math"
	"testing"

	event "github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
	"github.com/nats-io/nats.go/jetstream"
)

// mockJSForDLQ implements jsBackend and records Publish calls.
type mockJSForDLQ struct {
	publishCalls int
	publishErr   error
}

func (m *mockJSForDLQ) Publish(_ context.Context, _ string, _ []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	m.publishCalls++
	return nil, m.publishErr
}

func (m *mockJSForDLQ) CreateOrUpdateConsumer(_ context.Context, _ string, _ jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	return nil, nil
}

// unmarshalableEvent holds a value that json.Marshal cannot encode (math.Inf).
type unmarshalableEvent struct {
	name string
	Bad  float64
}

func newUnmarshalableEvent() *unmarshalableEvent {
	return &unmarshalableEvent{name: "bad.event", Bad: math.Inf(1)}
}

func (e *unmarshalableEvent) Name() string { return e.name }

// marshalableEvent is a normal event for success-path tests.
type marshalableEvent struct {
	name string
}

func (e *marshalableEvent) Name() string { return e.name }

// compile-time checks
var _ event.Event = (*marshalableEvent)(nil)
var _ event.Event = (*unmarshalableEvent)(nil)

func TestDLQ_Publish_MarshalError(t *testing.T) {
	mock := &mockJSForDLQ{}
	dlq := &DLQ{
		js:      mock,
		metrics: &invoker.NoopProvider{},
		prefix:  "dlq.",
	}

	evt := newUnmarshalableEvent()
	origErr := errors.New("handler failed")

	err := dlq.Publish(context.Background(), evt, origErr)

	if err == nil {
		t.Fatal("expected non-nil error when marshal fails, got nil")
	}
	if mock.publishCalls != 0 {
		t.Errorf("expected 0 js.Publish calls when marshal fails, got %d", mock.publishCalls)
	}
}

func TestDLQ_Publish_Success(t *testing.T) {
	mock := &mockJSForDLQ{}
	dlq := &DLQ{
		js:      mock,
		metrics: &invoker.NoopProvider{},
		prefix:  "dlq.",
	}

	evt := &marshalableEvent{name: "ok.event"}
	origErr := errors.New("handler failed")

	err := dlq.Publish(context.Background(), evt, origErr)

	if err != nil {
		t.Fatalf("expected nil error for valid event, got %v", err)
	}
	if mock.publishCalls != 1 {
		t.Errorf("expected 1 js.Publish call, got %d", mock.publishCalls)
	}
}
