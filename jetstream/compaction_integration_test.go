//go:build integration

package jetstream

import (
	"context"
	"encoding/json"
	"slices"
	"sync"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
	"github.com/nats-io/nats.go/jetstream"
)

// Real-server proofs for the JetStream log-compaction guide (DOC-02, D-09).
// Every claim of the guide cites one of these tests, so none of it rests on
// memory. The library adds no compaction API (D-08): the tests pin what the
// server does with a MaxMsgsPerSubject=1 stream and how the existing Bus,
// DLQ and Replayer behave on top of it.
//
// Every stream, consumer and subject name comes from uniqueName and every
// time-based check polls with a deadline: subject delete markers expire lazily
// (a 2s MaxAge still held the message at 3.5s when this was probed), so a fixed
// sleep must never decide an assertion.

// cpSink records the ids of the events a handler received, in order.
type cpSink struct {
	mu  sync.Mutex
	got []string
}

// handler is the event.HandleFn that records the id of every event.
func (s *cpSink) handler() event.HandleFn {
	return func(_ context.Context, evt event.Event) error {
		o, ok := evt.(*itOrder)
		if !ok {
			return nil
		}
		s.mu.Lock()
		s.got = append(s.got, o.ID)
		s.mu.Unlock()
		return nil
	}
}

// ids returns a copy of the ids received so far.
func (s *cpSink) ids() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.got)
}

// cpPoll polls cond every 25ms until it holds, and fails the test with desc
// after d. It is the only way these tests wait for the server.
func cpPoll(t testing.TB, d time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout after %s waiting for %s", d, desc)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// cpBus builds a Bus over js with the itRegistry and a MemoryDLQ. The returned
// Bus is closed on cleanup.
func cpBus(t testing.TB, js jetstream.JetStream, obs event.Observer) *Bus {
	t.Helper()
	bus := NewBus(js, itRegistry(), BusOptions{
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
		DLQPublisher:     event.NewMemoryDLQ(),
		Observer:         obs,
	})
	t.Cleanup(func() { _ = bus.Close() })
	return bus
}

// cpEmit publishes version id on the entity subject through the Bus. The id is
// the Nats-Msg-Id, so every version needs its own.
func cpEmit(ctx context.Context, t testing.TB, bus *Bus, subject, id string) {
	t.Helper()
	if err := bus.Emit(ctx, &itOrder{ID: id}, event.WithSubject(subject)); err != nil {
		t.Fatalf("Emit %s on %s: %v", id, subject, err)
	}
}

// cpStreamInfo returns the stream state.
func cpStreamInfo(t testing.TB, js jetstream.JetStream, stream string) *jetstream.StreamInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("stream %s: %v", stream, err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("stream %s info: %v", stream, err)
	}
	return info
}

// cpEnvelopeID decodes the id of an envelope body.
func cpEnvelopeID(t testing.TB, data []byte) string {
	t.Helper()
	var env event.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("decode envelope %q: %v", data, err)
	}
	return env.ID
}

// cpExpectIDs polls until sink holds at least len(want) ids, then waits a
// grace period and requires the ids to equal want exactly: nothing arrives
// after the expected ones.
func cpExpectIDs(t testing.TB, sink *cpSink, grace time.Duration, want ...string) {
	t.Helper()
	cpPoll(t, 10*time.Second, "handler to receive "+joinIDs(want), func() bool {
		return len(sink.ids()) >= len(want)
	})
	time.Sleep(grace) // a grace window for extras, never the decision to proceed
	if got := sink.ids(); !slices.Equal(got, want) {
		t.Fatalf("handler received %v, want exactly %v", got, want)
	}
}

func joinIDs(ids []string) string {
	b, _ := json.Marshal(ids)
	return string(b)
}

// TestIntegration_CompactionKeepsLatestPerEntity is the tracer of the
// compaction guide: on a stream with MaxMsgsPerSubject=1 only the latest
// message of each entity subject survives (C1); a Bus subscription on the
// wildcard entity subject sees one message per entity, the latest, in
// stream-sequence order (C2); versions published while no consumer fetches are
// lost except the latest (C3); and a DeliverLastPerSubject consumer yields the
// same per-entity snapshot (C4) (DOC-02, D-09).
func TestIntegration_CompactionKeepsLatestPerEntity(t *testing.T) {
	_, js := connect(t)

	base := uniqueName(t, "cp")
	entity := func(n string) string { return base + ".user." + n }
	stream := newStreamCfg(t, js, jetstream.StreamConfig{
		Subjects:          []string{base + ".user.>"},
		MaxMsgsPerSubject: 1,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// C1: user.1 is written three times, the others once, interleaved.
	bus := cpBus(t, js, event.NopObserver{})
	cpEmit(ctx, t, bus, entity("1"), "u1-v1")
	cpEmit(ctx, t, bus, entity("2"), "u2-v1")
	cpEmit(ctx, t, bus, entity("1"), "u1-v2")
	cpEmit(ctx, t, bus, entity("3"), "u3-v1")
	cpEmit(ctx, t, bus, entity("1"), "u1-v3")

	st := cpStreamInfo(t, js, stream).State
	if st.Msgs != 3 || st.FirstSeq != 2 || st.LastSeq != 5 || st.NumSubjects != 3 {
		t.Fatalf("C1: stream state msgs=%d first=%d last=%d subjects=%d, want msgs=3 first=2 last=5 subjects=3",
			st.Msgs, st.FirstSeq, st.LastSeq, st.NumSubjects)
	}

	// C2: a fresh durable consumer on the wildcard. Bus.Subscribe sets the
	// consumer FilterSubject to the subscribed subject verbatim. The delivery
	// order is the stream-sequence order (user.2, user.3, user.1), not the order
	// in which the entities were first created.
	consumer := uniqueName(t, "C")
	subscribe := func(b *Bus, sink *cpSink) {
		t.Helper()
		if err := b.Subscribe(ctx, base+".user.>", sink.handler(),
			event.WithStream(stream),
			event.WithConsumer(consumer),
			event.WithMaxDeliver(3),
			event.WithBackOff([]time.Duration{time.Second, time.Second}),
		); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	}
	first := &cpSink{}
	subscribe(bus, first)
	cpExpectIDs(t, first, 500*time.Millisecond, "u2-v1", "u3-v1", "u1-v3")

	// C3: the consumer is stopped, two more versions of user.1 are published.
	// Version 4 is compacted away before any consumer fetches it.
	if err := bus.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	bus2 := cpBus(t, js, event.NopObserver{})
	cpEmit(ctx, t, bus2, entity("1"), "u1-v4")
	cpEmit(ctx, t, bus2, entity("1"), "u1-v5")

	second := &cpSink{}
	subscribe(bus2, second)
	cpExpectIDs(t, second, 500*time.Millisecond, "u1-v5")

	// C4: DeliverLastPerSubject, the right policy to rebuild current state, sees
	// the latest message of every entity and nothing else.
	cctx, ccancel := context.WithTimeout(ctx, 10*time.Second)
	defer ccancel()
	snap, err := js.CreateOrUpdateConsumer(cctx, stream, jetstream.ConsumerConfig{
		Name:          uniqueName(t, "snap"),
		AckPolicy:     jetstream.AckNonePolicy,
		DeliverPolicy: jetstream.DeliverLastPerSubjectPolicy,
		FilterSubject: base + ".user.>",
	})
	if err != nil {
		t.Fatalf("create DeliverLastPerSubject consumer: %v", err)
	}
	batch, err := snap.Fetch(3, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	snapshot := map[string]string{}
	for msg := range batch.Messages() {
		snapshot[msg.Subject()] = cpEnvelopeID(t, msg.Data())
	}
	if err := batch.Error(); err != nil {
		t.Fatalf("Fetch batch: %v", err)
	}
	want := map[string]string{entity("1"): "u1-v5", entity("2"): "u2-v1", entity("3"): "u3-v1"}
	if len(snapshot) != len(want) {
		t.Fatalf("C4: snapshot %v, want %v", snapshot, want)
	}
	for subject, id := range want {
		if snapshot[subject] != id {
			t.Errorf("C4: snapshot of %s = %q, want %q", subject, snapshot[subject], id)
		}
	}
}
