package jetstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/isaquesb/go-event-bus"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// pbEvent is the event used by the publish tests. Id() reports ID, so an event
// with an ID is deduplicated by the broker and one without is not.
type pbEvent struct {
	ID   string `json:"id"`
	Note string `json:"note,omitempty"`
}

func (e *pbEvent) Name() string { return "pb.order" }
func (e *pbEvent) Id() string   { return e.ID }

// pbRegistry is an mdRegistry that knows pbEvent.
func pbRegistry() *mdRegistry {
	reg := newMdRegistry()
	reg.Register("pb.order", func() event.Event { return &pbEvent{} }, 1)
	return reg
}

// pbMsgID is the Nats-Msg-Id header of a recorded publish.
func pbMsgID(h natsgo.Header) string { return h.Get(jetstream.MsgIDHeader) }

func TestJetStreamBus_MsgID_FromEventID(t *testing.T) {
	js := &mdJS{}
	reg := pbRegistry()
	bus := NewBus(js, reg, BusOptions{CircuitOpenDelay: 1, RateLimitDelay: 1})

	if err := bus.Emit(context.Background(), &pbEvent{ID: "o-1"}); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	pub := js.payload(t, 0)
	if got := pbMsgID(pub.header); got != "o-1" {
		t.Errorf("Nats-Msg-Id = %q, want o-1", got)
	}
	if env := mdDecodeMetadata(t, reg, pub.data); env.ID != pbMsgID(pub.header) {
		t.Errorf("envelope id = %q, header id = %q, want equal", env.ID, pbMsgID(pub.header))
	}
	if pub.opts != 0 {
		t.Errorf("PublishMsg got %d options, want 0", pub.opts)
	}
}

func TestJetStreamBus_NilInvokerDefaultHandlesMessage(t *testing.T) {
	js := &mdJS{}
	bus := NewBus(js, pbRegistry(), BusOptions{CircuitOpenDelay: 1, RateLimitDelay: 1})

	if err := bus.Emit(context.Background(), &pbEvent{ID: "o-2"}); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	runs := 0
	msg := &mdMsg{data: js.payload(t, 0).data, numDelivered: 1}
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		runs++
		return nil
	}, deliveryPolicy{})(msg)

	if runs != 1 {
		t.Errorf("handler ran %d times, want 1", runs)
	}
	if acks, naks, terms := msg.counts(); acks != 1 || naks != 0 || terms != 0 {
		t.Errorf("acks=%d naks=%d terms=%d, want 1/0/0", acks, naks, terms)
	}
}

// --- scripted PublishMsg backend ---

// pbStep scripts one publish attempt: block until the attempt ctx is done,
// return err, or return ack (a nil ack is an empty PubAck).
type pbStep struct {
	block bool
	err   error
	ack   *jetstream.PubAck
}

// pbAttempt records what one PublishMsg call saw.
type pbAttempt struct {
	msg         *natsgo.Msg
	data        []byte
	msgID       string
	at          time.Time
	ctxDoneAtIn bool
	hasDeadline bool
	deadlineIn  time.Duration
}

// pbNatsDefaultTimeout stands in for the nats.go default publish timeout, which
// applies when the attempt ctx carries no deadline.
const pbNatsDefaultTimeout = 5 * time.Second

// pbJS is a jsBackend whose PublishMsg follows a per-attempt script. The last
// step repeats once the script is exhausted.
type pbJS struct {
	mu       sync.Mutex
	script   []pbStep
	attempts []pbAttempt
}

func (p *pbJS) PublishMsg(ctx context.Context, m *natsgo.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	rec := pbAttempt{
		msg:         m,
		data:        bytes.Clone(m.Data),
		msgID:       m.Header.Get(jetstream.MsgIDHeader),
		at:          time.Now(),
		ctxDoneAtIn: ctx.Err() != nil,
	}
	if dl, ok := ctx.Deadline(); ok {
		rec.hasDeadline = true
		rec.deadlineIn = time.Until(dl)
	}
	p.mu.Lock()
	i := len(p.attempts)
	p.attempts = append(p.attempts, rec)
	step := p.script[min(i, len(p.script)-1)]
	p.mu.Unlock()

	switch {
	case step.block:
		if rec.hasDeadline {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(pbNatsDefaultTimeout):
			return nil, natsgo.ErrTimeout
		}
	case step.err != nil:
		return nil, step.err
	case step.ack != nil:
		return step.ack, nil
	}
	return &jetstream.PubAck{}, nil
}

func (p *pbJS) CreateOrUpdateConsumer(context.Context, string, jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	return &mockConsumer{}, nil
}

func (p *pbJS) recorded() []pbAttempt {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]pbAttempt(nil), p.attempts...)
}

// pbBus builds a bus over js with policy and a nil Invoker.
func pbBus(js jsBackend, policy PublishRetryPolicy) *Bus {
	return NewBus(js, pbRegistry(), BusOptions{CircuitOpenDelay: 1, RateLimitDelay: 1, PublishRetry: policy})
}

// pbSameMessage fails the test unless every attempt saw the same *nats.Msg, the
// same bytes and the same non-empty Nats-Msg-Id.
func pbSameMessage(t *testing.T, got []pbAttempt, wantID string) {
	t.Helper()
	for i, a := range got {
		if a.msg != got[0].msg {
			t.Errorf("attempt %d used a different *nats.Msg", i+1)
		}
		if !bytes.Equal(a.data, got[0].data) {
			t.Errorf("attempt %d carried different data", i+1)
		}
		if a.msgID != wantID {
			t.Errorf("attempt %d Nats-Msg-Id = %q, want %q", i+1, a.msgID, wantID)
		}
		if a.ctxDoneAtIn {
			t.Errorf("attempt %d started after the caller ctx was done", i+1)
		}
	}
}

// pbGap is the time between the start of attempt i and attempt i+1 (0-based i).
func pbGap(got []pbAttempt, i int) time.Duration { return got[i+1].at.Sub(got[i].at) }

// --- retry matrix ---

func TestJetStreamBus_PublishRetry_LostAckThenSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		js := &pbJS{script: []pbStep{{block: true}, {}}}
		bus := pbBus(js, PublishRetryPolicy{})

		if err := bus.Emit(context.Background(), &pbEvent{ID: "o-1"}); err != nil {
			t.Fatalf("Emit = %v, want nil after the retry", err)
		}

		got := js.recorded()
		if len(got) != 2 {
			t.Fatalf("attempts = %d, want 2", len(got))
		}
		pbSameMessage(t, got, "o-1")
		if got[0].deadlineIn != 2*time.Second {
			t.Errorf("per-attempt timeout = %v, want the 2s default", got[0].deadlineIn)
		}
		// The first attempt ends at its 2s timeout; the backoff is 50-100ms.
		if gap := pbGap(got, 0); gap < 2*time.Second+50*time.Millisecond || gap > 2*time.Second+100*time.Millisecond {
			t.Errorf("gap between attempts = %v, want 2s plus 50-100ms", gap)
		}
	})
}

func TestJetStreamBus_PublishRetry_StopsAtCallerDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		js := &pbJS{script: []pbStep{{block: true}}}
		bus := pbBus(js, PublishRetryPolicy{})

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		start := time.Now()
		err := bus.Emit(ctx, &pbEvent{ID: "o-1"})

		if err == nil {
			t.Fatal("Emit = nil, want an error once the caller deadline passes")
		}
		got := js.recorded()
		if len(got) != 2 {
			t.Fatalf("attempts = %d, want exactly 2 (2s attempt, then the rest of the 3s deadline)", len(got))
		}
		pbSameMessage(t, got, "o-1")
		if elapsed := time.Since(start); elapsed > 3*time.Second {
			t.Errorf("Emit took %v, want no more than the 3s caller deadline", elapsed)
		}
	})
}

func TestJetStreamBus_PublishRetry_NoResponders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		noResp := fmt.Errorf("publish: %w", natsgo.ErrNoResponders)
		js := &pbJS{script: []pbStep{{err: noResp}, {err: noResp}, {}}}
		bus := pbBus(js, PublishRetryPolicy{})

		if err := bus.Emit(context.Background(), &pbEvent{ID: "o-1"}); err != nil {
			t.Fatalf("Emit = %v, want nil on the third attempt", err)
		}
		got := js.recorded()
		if len(got) != 3 {
			t.Fatalf("attempts = %d, want 3", len(got))
		}
		pbSameMessage(t, got, "o-1")
		if gap := pbGap(got, 0); gap < 50*time.Millisecond || gap > 100*time.Millisecond {
			t.Errorf("first backoff = %v, want 50-100ms", gap)
		}
		if gap := pbGap(got, 1); gap < 100*time.Millisecond || gap > 200*time.Millisecond {
			t.Errorf("second backoff = %v, want 100-200ms", gap)
		}
	})
}

func TestJetStreamBus_PublishRetry_NoStreamResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		js := &pbJS{script: []pbStep{{err: fmt.Errorf("publish: %w", jetstream.ErrNoStreamResponse)}, {}}}
		bus := pbBus(js, PublishRetryPolicy{})

		if err := bus.Emit(context.Background(), &pbEvent{ID: "o-1"}); err != nil {
			t.Fatalf("Emit = %v, want nil on the second attempt", err)
		}
		got := js.recorded()
		if len(got) != 2 {
			t.Fatalf("attempts = %d, want 2", len(got))
		}
		pbSameMessage(t, got, "o-1")
	})
}

func TestJetStreamBus_PublishRetry_NonRetryableErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"server API error", fmt.Errorf("publish: %w", &jetstream.APIError{Code: 503, ErrorCode: jetstream.JSErrCodeJetStreamNotEnabled, Description: "unavailable"})},
		{"connection closed", fmt.Errorf("publish: %w", natsgo.ErrConnectionClosed)},
	}
	for _, tc := range cases {
		synctest.Test(t, func(t *testing.T) {
			js := &pbJS{script: []pbStep{{err: tc.err}, {}}}
			bus := pbBus(js, PublishRetryPolicy{})

			err := bus.Emit(context.Background(), &pbEvent{ID: "o-1"})
			if !errors.Is(err, tc.err) {
				t.Errorf("%s: Emit = %v, want %v", tc.name, err, tc.err)
			}
			if n := len(js.recorded()); n != 1 {
				t.Errorf("%s: attempts = %d, want 1", tc.name, n)
			}
		})
	}
}

func TestJetStreamBus_PublishRetry_DefaultsAndOptOut(t *testing.T) {
	// A zero policy is 3 attempts, a 2s attempt timeout and a 100ms/1s backoff.
	synctest.Test(t, func(t *testing.T) {
		js := &pbJS{script: []pbStep{{block: true}}}
		err := pbBus(js, PublishRetryPolicy{}).Emit(context.Background(), &pbEvent{ID: "o-1"})
		if err == nil {
			t.Fatal("zero policy: Emit = nil, want the last timeout")
		}
		got := js.recorded()
		if len(got) != 3 {
			t.Fatalf("zero policy: attempts = %d, want 3", len(got))
		}
		if gap := pbGap(got, 1); gap < 2*time.Second+100*time.Millisecond || gap > 2*time.Second+200*time.Millisecond {
			t.Errorf("zero policy: second gap = %v, want 2s plus 100-200ms", gap)
		}
	})

	// MaxAttempts 1 disables the retry.
	synctest.Test(t, func(t *testing.T) {
		js := &pbJS{script: []pbStep{{block: true}}}
		err := pbBus(js, PublishRetryPolicy{MaxAttempts: 1}).Emit(context.Background(), &pbEvent{ID: "o-1"})
		if err == nil {
			t.Fatal("MaxAttempts 1: Emit = nil, want the timeout")
		}
		if n := len(js.recorded()); n != 1 {
			t.Errorf("MaxAttempts 1: attempts = %d, want 1", n)
		}
	})

	// A negative AttemptTimeout leaves the attempt ctx without a deadline.
	synctest.Test(t, func(t *testing.T) {
		js := &pbJS{script: []pbStep{{err: natsgo.ErrTimeout}, {}}}
		if err := pbBus(js, PublishRetryPolicy{AttemptTimeout: -1}).Emit(context.Background(), &pbEvent{ID: "o-1"}); err != nil {
			t.Fatalf("negative AttemptTimeout: Emit = %v, want nil after the retry", err)
		}
		got := js.recorded()
		if len(got) != 2 {
			t.Fatalf("negative AttemptTimeout: attempts = %d, want 2", len(got))
		}
		if got[0].hasDeadline {
			t.Error("negative AttemptTimeout: the attempt ctx has a deadline, want none")
		}
	})

	// A custom policy: the backoff doubles and is clamped to MaxDelay.
	synctest.Test(t, func(t *testing.T) {
		js := &pbJS{script: []pbStep{{err: natsgo.ErrNoResponders}}}
		policy := PublishRetryPolicy{MaxAttempts: 4, BaseDelay: 400 * time.Millisecond, MaxDelay: 500 * time.Millisecond, AttemptTimeout: 500 * time.Millisecond}
		if err := pbBus(js, policy).Emit(context.Background(), &pbEvent{ID: "o-1"}); err == nil {
			t.Fatal("custom policy: Emit = nil, want the last error")
		}
		got := js.recorded()
		if len(got) != 4 {
			t.Fatalf("custom policy: attempts = %d, want 4", len(got))
		}
		if got[0].deadlineIn != 500*time.Millisecond {
			t.Errorf("custom policy: attempt timeout = %v, want 500ms", got[0].deadlineIn)
		}
		if gap := pbGap(got, 0); gap < 200*time.Millisecond || gap > 400*time.Millisecond {
			t.Errorf("custom policy: first gap = %v, want 200-400ms", gap)
		}
		for i := 1; i < 3; i++ {
			if gap := pbGap(got, i); gap < 250*time.Millisecond || gap > 500*time.Millisecond {
				t.Errorf("custom policy: gap %d = %v, want 250-500ms (clamped to MaxDelay)", i+1, gap)
			}
		}
	})
}

func TestJetStreamBus_PublishRetry_CancelledCallerCtx(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		js := &pbJS{script: []pbStep{{block: true}}}
		bus := pbBus(js, PublishRetryPolicy{})

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := bus.Emit(ctx, &pbEvent{ID: "o-1"})

		if !errors.Is(err, context.Canceled) {
			t.Errorf("Emit = %v, want context.Canceled", err)
		}
		got := js.recorded()
		if len(got) > 1 {
			t.Errorf("attempts = %d, want at most 1", len(got))
		}
	})
}

func TestJetStreamBus_DuplicateAckIsNotError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logs bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		defer slog.SetDefault(prev)

		js := &pbJS{script: []pbStep{{ack: &jetstream.PubAck{Stream: "S", Sequence: 7, Duplicate: true}}}}
		bus := pbBus(js, PublishRetryPolicy{})

		if err := bus.Emit(context.Background(), &pbEvent{ID: "o-1"}); err != nil {
			t.Fatalf("Emit = %v, want nil for a Duplicate ack", err)
		}
		if n := len(js.recorded()); n != 1 {
			t.Errorf("attempts = %d, want 1", n)
		}
		if out := logs.String(); !strings.Contains(out, "level=DEBUG") || !strings.Contains(out, "msg_id=o-1") {
			t.Errorf("log = %q, want a debug line with msg_id=o-1", out)
		}
	})
}

func TestJetStreamBus_MsgID_Precedence(t *testing.T) {
	js := &mdJS{}
	bus := pbBus(js, PublishRetryPolicy{})
	ctx := context.Background()

	// WithID beats Event.Id().
	if err := bus.Emit(ctx, &pbEvent{ID: "from-event"}, event.WithID("explicit")); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if got := pbMsgID(js.payload(t, 0).header); got != "explicit" {
		t.Errorf("WithID: Nats-Msg-Id = %q, want explicit", got)
	}

	// Without Id() and WithID the id is generated per call.
	for range 2 {
		if err := bus.Emit(ctx, &pbEvent{}); err != nil {
			t.Fatalf("Emit: %v", err)
		}
	}
	a, b := pbMsgID(js.payload(t, 1).header), pbMsgID(js.payload(t, 2).header)
	if a == "" || b == "" || a == b {
		t.Errorf("generated ids = %q and %q, want two different non-empty ids", a, b)
	}

	// None of them can be a Phase 8 replay id (<id>:replay:<seq>), so a replay
	// is never deduplicated against the original.
	for i := range js.count() {
		if id := pbMsgID(js.payload(t, i).header); strings.Contains(id, ":replay:") {
			t.Errorf("publish %d Nats-Msg-Id %q looks like a replay id", i, id)
		}
	}
}
