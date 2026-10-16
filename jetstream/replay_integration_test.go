//go:build integration

package jetstream

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// itReplayEnv is a dead-lettered message waiting in a real DLQ stream, with the
// original consumer still subscribed so a replay can be handled end to end.
type itReplayEnv struct {
	ctx       context.Context
	js        jetstream.JetStream
	stream    string
	dlqStream string
	subject   string
	consumer  string
	// key is the idempotency key of the message: event id plus handler name.
	key   string
	store *invoker.MemoryIdempotencyStore
	// seen receives the Metadata the handler sees on every call.
	seen chan event.Metadata
	// orig is the data of the original stream message.
	orig []byte
}

// itReplaySetup subscribes a consumer whose handler fails terminally on its
// first call and succeeds afterwards, publishes the original message with a
// Nats-Msg-Id the way Phase 9 will, and waits until it is dead-lettered. Each
// call creates its own streams.
func itReplaySetup(t *testing.T) *itReplayEnv {
	t.Helper()
	_, js := connect(t)

	base := uniqueName(t, "it")
	env := &itReplayEnv{
		js:      js,
		subject: base + ".order",
		store:   invoker.NewMemoryIdempotencyStore(),
		seen:    make(chan event.Metadata, 8),
	}
	env.stream = newStream(t, js, base+".>")
	prefix, dlqStream := itNewDLQStream(t, js)
	env.dlqStream = dlqStream
	env.consumer = uniqueName(t, "C")
	env.key = "o-7:" + env.consumer

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	env.ctx = ctx

	dlq := NewDLQ(js, nil, prefix)
	reg := itRegistry()
	bus := NewBus(js, reg, BusOptions{
		Invoker: invoker.NewChain(
			invoker.NewIdempotency(env.store, invoker.IdempotencyConfig{}, nil),
			invoker.NewDLQ(dlq, nil),
			invoker.NewRetry(invoker.RetryPolicy{MaxAttempts: 1}, nil),
		),
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
		DLQPublisher:     dlq,
	})
	t.Cleanup(func() { _ = bus.Close() })

	var calls atomic.Int32
	handler := func(hctx context.Context, _ event.Event) error {
		md, _ := event.MetadataFromContext(hctx)
		env.seen <- md
		if calls.Add(1) == 1 {
			return invoker.PermanentError{Err: errors.New("boom")}
		}
		return nil
	}
	if err := bus.Subscribe(ctx, env.subject, handler,
		event.WithStream(env.stream),
		event.WithConsumer(env.consumer),
		event.WithMaxDeliver(3),
		event.WithBackOff([]time.Duration{100 * time.Millisecond, 100 * time.Millisecond}),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Publish the original by hand: the bus does not set a Nats-Msg-Id yet
	// (Phase 9), and the replay must survive the stream dedup of one.
	data, err := reg.EncodeWith(ctx, &itOrder{ID: "o-7"}, event.Metadata{
		ID:            "o-7",
		CorrelationID: "corr-7",
		Timestamp:     time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("encode original: %v", err)
	}
	env.orig = data
	if _, err := js.PublishMsg(ctx, &nats.Msg{
		Subject: env.subject,
		Data:    data,
		Header:  nats.Header{jetstream.MsgIDHeader: {"o-7"}, "Tenant": {"acme"}},
	}); err != nil {
		t.Fatalf("publish original: %v", err)
	}

	itWaitStreamMsgs(t, js, env.dlqStream, 1)
	select {
	case md := <-env.seen:
		if md.ID != "o-7" {
			t.Fatalf("first handler call saw id %q, want o-7", md.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not run for the original message")
	}
	env.waitStatus(t, invoker.StatusDeadLettered)
	return env
}

// waitStatus polls the idempotency record of the message until it has status
// want, and fails the test with the last record after 5s.
func (e *itReplayEnv) waitStatus(t *testing.T, want invoker.IdempotencyStatus) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		rec, ok, err := e.store.Get(context.Background(), e.key)
		if err != nil {
			t.Fatalf("idempotency store get: %v", err)
		}
		if ok && rec.Status == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("idempotency record %q = %+v (exists %v), want status %s", e.key, rec, ok, want)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// TestIntegration_ReplayRoundTrip moves a dead-letter record back to the
// original subject on a real server: the original consumer decodes the same id
// and correlation id and handles it; the original Nats-Msg-Id survives only in
// Eventbus-Dlq-Original-Msg-Id and the replay id EVENT_ID:replay:SEQ is not
// deduplicated against it; a second Replay on the durable cursor replays
// nothing and returns promptly (FAIL-07, D-18, D-19, D-20, D-22, D-23, D-28).
func TestIntegration_ReplayRoundTrip(t *testing.T) {
	env := itReplaySetup(t)
	js := env.js

	// The DLQ record: verbatim data, no broker id, original user headers kept.
	rec := itStreamMsg(t, env.js, env.dlqStream, 1)
	if !bytes.Equal(rec.Data, env.orig) {
		t.Errorf("DLQ data is not byte-identical to the original\n orig: %s\n dlq:  %s", env.orig, rec.Data)
	}
	if got := rec.Header.Get(jetstream.MsgIDHeader); got != "" {
		t.Errorf("DLQ record carries %s %q, want none (D-28)", jetstream.MsgIDHeader, got)
	}
	if got := rec.Header.Get(event.HeaderDLQOriginalMsgID); got != "o-7" {
		t.Errorf("%s = %q, want o-7 (D-28)", event.HeaderDLQOriginalMsgID, got)
	}
	if got := rec.Header.Get("Tenant"); got != "acme" {
		t.Errorf("Tenant = %q, want acme", got)
	}
	// D-19: the dead-letter outcome is recorded, and it is not a completion.
	env.waitStatus(t, invoker.StatusDeadLettered)

	res, err := NewReplayer(js).Replay(env.ctx, ReplayOptions{FromStream: env.dlqStream})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Replayed != 1 || res.Failed != 0 {
		t.Fatalf("Replay result = %+v, want Replayed 1 Failed 0", res)
	}

	// The original consumer handles the replayed record with its lineage intact.
	select {
	case md := <-env.seen:
		if md.ID != "o-7" || md.CorrelationID != "corr-7" {
			t.Errorf("replayed handler call saw id %q correlation %q, want o-7 and corr-7", md.ID, md.CorrelationID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("original consumer did not handle the replayed record within 10s")
	}
	env.waitStatus(t, invoker.StatusCompleted)

	// The replay was published, not swallowed by the stream duplicate window.
	itWaitStreamMsgs(t, env.js, env.stream, 2)
	if n := itStreamMsgs(t, env.js, env.stream); n != 2 {
		t.Fatalf("original stream holds %d messages, want 2", n)
	}
	replayed := itStreamMsg(t, env.js, env.stream, 2)
	if got, want := replayed.Header.Get(jetstream.MsgIDHeader), "o-7:replay:1"; got != want {
		t.Errorf("replayed %s = %q, want %q (D-20)", jetstream.MsgIDHeader, got, want)
	}
	if got := replayed.Header.Get("Tenant"); got != "acme" {
		t.Errorf("replayed Tenant = %q, want acme", got)
	}
	for k := range replayed.Header {
		if strings.HasPrefix(strings.ToLower(k), strings.ToLower(event.HeaderDLQPrefix)) {
			t.Errorf("replayed message still carries %s", k)
		}
	}
	if !bytes.Equal(replayed.Data, env.orig) {
		t.Errorf("replayed data differs from the original\n orig:     %s\n replayed: %s", env.orig, replayed.Data)
	}
	if replayed.Subject != env.subject {
		t.Errorf("replayed subject = %q, want the original %q", replayed.Subject, env.subject)
	}

	// The durable cursor moved past the record: nothing left, no blocking fetch.
	start := time.Now()
	res, err = NewReplayer(js).Replay(env.ctx, ReplayOptions{FromStream: env.dlqStream})
	if err != nil {
		t.Fatalf("second Replay: %v", err)
	}
	if res.Replayed != 0 || res.Failed != 0 {
		t.Errorf("second Replay result = %+v, want nothing replayed", res)
	}
	if took := time.Since(start); took >= 2*time.Second {
		t.Errorf("second Replay took %s on a drained stream, want under 2s", took)
	}
}

// TestIntegration_ReplayRepeatDedups replays the same record again through a
// fresh durable cursor: the replay id EVENT_ID:replay:SEQ is the same, so the
// stream reports a duplicate, the Replayer counts it as replayed, and the
// original stream does not grow (D-20).
func TestIntegration_ReplayRepeatDedups(t *testing.T) {
	env := itReplaySetup(t)
	replayer := NewReplayer(env.js)

	res, err := replayer.Replay(env.ctx, ReplayOptions{FromStream: env.dlqStream})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Replayed != 1 || res.Failed != 0 {
		t.Fatalf("Replay result = %+v, want Replayed 1 Failed 0", res)
	}
	itWaitStreamMsgs(t, env.js, env.stream, 2)

	res, err = replayer.Replay(env.ctx, ReplayOptions{FromStream: env.dlqStream, Consumer: "replay_again"})
	if err != nil {
		t.Fatalf("repeat Replay: %v", err)
	}
	if res.Replayed != 1 || res.Failed != 0 {
		t.Errorf("repeat Replay result = %+v, want Replayed 1 Failed 0 (a duplicate counts as replayed)", res)
	}

	// Give a (wrongly) published duplicate time to reach the stream.
	time.Sleep(300 * time.Millisecond)
	if n := itStreamMsgs(t, env.js, env.stream); n != 2 {
		t.Errorf("original stream holds %d messages after the repeat replay, want exactly 2", n)
	}
}
