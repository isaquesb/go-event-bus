package jetstream

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	event "github.com/isaquesb/go-event-bus"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// replayBatchSize is the largest number of records fetched per round trip.
const replayBatchSize = 64

// ErrReplaySubject is returned by Replay when a dead-letter record has no
// usable target subject: the Eventbus-Dlq-Original-Subject header is missing or
// holds an empty token, a wildcard or whitespace, and ReplayOptions.ToSubject is
// not set. The record is Nakked, not published.
var ErrReplaySubject = errors.New("jetstream: replay record has no valid target subject")

// replayBackend is the part of jetstream.JetStream the Replayer needs. It is
// satisfied by jetstream.JetStream; tests use a scripted fake.
type replayBackend interface {
	CreateOrUpdateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error)
	PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// Replayer republishes dead-lettered messages from a DLQ stream.
type Replayer struct {
	js replayBackend
}

// NewReplayer returns a Replayer that reads and publishes through js.
func NewReplayer(js replayBackend) *Replayer {
	return &Replayer{js: js}
}

// ReplayOptions selects what Replay reads and where it publishes.
type ReplayOptions struct {
	// FromStream is the DLQ stream to read. It is required.
	FromStream string
	// ToSubject, when set, overrides the per-record target. Without it each
	// record goes back to its Eventbus-Dlq-Original-Subject, so a DLQ holding
	// several subjects fans back out to the right ones.
	ToSubject string
	// Limit is the largest number of records to replay. At or below 0 means
	// until the stream is drained.
	Limit int
	// Consumer is the durable consumer name. The default is "replay_" plus
	// FromStream.
	Consumer string
	// FilterSubject restricts the consumer to matching subjects, for a DLQ
	// stream shared with other subjects.
	FilterSubject string
}

// ReplayResult counts the records handled by one Replay call.
type ReplayResult struct {
	// Replayed is the number of records republished and acknowledged,
	// including records the stream reported as duplicates of an earlier replay.
	Replayed int
	// Failed is the number of records that could not be republished.
	Failed int
}

// Replay republishes DLQ records, byte for byte, to their original subject, or
// to opts.ToSubject when set. The envelope is never decoded. The original user
// headers are kept; every Eventbus-Dlq-* and Nats-* header is dropped.
//
// Each replay carries Nats-Msg-Id EVENT_ID:replay:DLQ_STREAM_SEQ, so the
// original message id cannot be swallowed by the stream duplicate window, while
// repeated replays of one record within that window dedup against each other. A
// publish the stream reports as a duplicate counts as replayed.
//
// Replay reads through a durable named consumer with explicit acks: an
// acknowledged record advances the cursor, and the next Replay continues where
// the last one stopped. Records stay in the DLQ stream for audit; retention is
// the user's choice. Replay does not block on an empty stream.
//
// It stops at the first record that fails, Naks that record and the rest of
// the fetched batch, and returns the error. It returns a nil error when the
// stream is drained or Limit is reached.
func (r *Replayer) Replay(ctx context.Context, opts ReplayOptions) (ReplayResult, error) {
	var res ReplayResult
	if opts.FromStream == "" {
		return res, ErrStreamRequired
	}
	name := opts.Consumer
	if name == "" {
		name = "replay_" + opts.FromStream
	}
	c, err := r.js.CreateOrUpdateConsumer(ctx, opts.FromStream, jetstream.ConsumerConfig{
		Name:          name,
		Durable:       name,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: opts.FilterSubject,
	})
	if err != nil {
		return res, err
	}

	for {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		n := replayBatchSize
		if opts.Limit > 0 {
			remaining := opts.Limit - res.Replayed
			if remaining <= 0 {
				return res, nil
			}
			n = min(n, remaining)
		}
		batch, err := c.FetchNoWait(n)
		if err != nil {
			if errors.Is(err, jetstream.ErrNoMessages) {
				return res, nil
			}
			return res, err
		}
		got := 0
		for msg := range batch.Messages() {
			got++
			if rerr := r.replayOne(ctx, msg, opts, &res); rerr != nil {
				return res, rerr
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, jetstream.ErrNoMessages) {
			return res, err
		}
		if got == 0 {
			return res, nil
		}
	}
}

// replayOne republishes one record and settles it: Ack on success (a duplicate
// PubAck included), Nak on failure.
func (r *Replayer) replayOne(ctx context.Context, msg jetstream.Msg, opts ReplayOptions, res *ReplayResult) error {
	var seq uint64
	md, mdErr := msg.Metadata()
	if mdErr == nil {
		seq = md.Sequence.Stream
	}
	src := msg.Headers()

	target := opts.ToSubject
	if target == "" {
		target = src.Get(event.HeaderDLQOriginalSubject)
	}
	if !validReplaySubject(target) {
		_ = msg.Nak()
		res.Failed++
		return fmt.Errorf("jetstream: replay stream %s seq %d: %w", opts.FromStream, seq, ErrReplaySubject)
	}

	h := replayHeaders(src)
	if mdErr == nil {
		sq := strconv.FormatUint(seq, 10)
		id := "replay:" + opts.FromStream + ":" + sq
		if eid := src.Get(event.HeaderDLQEventID); eid != "" {
			id = eid + ":replay:" + sq
		}
		h.Set(jetstream.MsgIDHeader, id)
	}

	if _, err := r.js.PublishMsg(ctx, &nats.Msg{Subject: target, Header: h, Data: msg.Data()}); err != nil {
		_ = msg.Nak()
		res.Failed++
		return fmt.Errorf("jetstream: replay stream %s seq %d: %w", opts.FromStream, seq, err)
	}
	// PubAck.Duplicate means this record was already replayed inside the
	// duplicate window; the record is done either way.
	_ = msg.Ack()
	res.Replayed++
	return nil
}

// replayHeaders copies the user headers of a dead-letter record, dropping every
// Eventbus-Dlq-* and Nats-* header.
func replayHeaders(src nats.Header) nats.Header {
	dlq := strings.ToLower(event.HeaderDLQPrefix)
	h := make(nats.Header, len(src))
	for k, v := range src {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, dlq) || strings.HasPrefix(lk, "nats-") {
			continue
		}
		h[k] = append([]string(nil), v...)
	}
	return h
}

// validReplaySubject reports whether s can be a replay target.
func validReplaySubject(s string) bool {
	return s != ""
}
