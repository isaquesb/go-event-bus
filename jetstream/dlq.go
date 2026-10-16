package jetstream

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	event "github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// DefaultDLQPrefix is the subject prefix NewDLQ uses when it is given an empty
// prefix. A dead-lettered message is published to the prefix plus its original
// subject. Users with an existing DLQ stream on the v0.3.0 prefix pass that
// prefix explicitly.
const DefaultDLQPrefix = "dlq."

// dlqBackend is the part of jetstream.JetStream the DLQ publisher needs. It is
// satisfied by jetstream.JetStream; tests use a capturing fake.
type dlqBackend interface {
	PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// DLQ publishes dead-lettered messages to JetStream. The record holds the
// original envelope bytes untouched, the original user headers, and the
// failure context in bounded Eventbus-Dlq-* headers. It implements
// event.DLQPublisher.
//
// Delivery to the DLQ stream is at-least-once: the publish carries no message
// id, so a record can be duplicated after a crash or a failed acknowledgement
// and consumers of the DLQ stream must tolerate that.
type DLQ struct {
	js       dlqBackend
	metrics  invoker.MetricProvider
	prefix   string
	registry event.Registry
}

var _ event.DLQPublisher = (*DLQ)(nil)

// DLQOption configures a DLQ.
type DLQOption func(*DLQ)

// NewDLQ returns a DLQ that publishes through js. A nil metrics is a no-op
// provider and an empty prefix is DefaultDLQPrefix.
func NewDLQ(js dlqBackend, metrics invoker.MetricProvider, prefix string, opts ...DLQOption) *DLQ {
	if metrics == nil {
		metrics = &invoker.NoopProvider{}
	}
	if prefix == "" {
		prefix = DefaultDLQPrefix
	}
	d := &DLQ{js: js, metrics: metrics, prefix: prefix}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// WithDLQRegistry sets the registry the DLQ uses to encode a record that has
// no envelope bytes, which is the case for records from LocalBus or NATS Core.
func WithDLQRegistry(reg event.Registry) DLQOption {
	return func(d *DLQ) { d.registry = reg }
}

// ErrNoEnvelope is returned by Publish for a record with no envelope bytes
// when the DLQ has no registry or the record has no event to encode.
var ErrNoEnvelope = errors.New("jetstream: DLQ record has no envelope bytes and no registry to encode its event")

// natsHeaderPrefix marks broker system headers (Nats-Msg-Id, Nats-Expected-*,
// ...) that must not follow a message into the DLQ stream.
const natsHeaderPrefix = "Nats-"

// dlqMinimalCauseLimit bounds the cause in the minimal max-payload retry.
const dlqMinimalCauseLimit = 128

// Publish publishes rec to the prefix plus the original subject. When rec.Raw
// is present it is the message data, unchanged. A record without Raw (LocalBus
// and NATS Core records) is encoded from rec.Evt through the registry given
// with WithDLQRegistry; without a registry or an event Publish returns
// ErrNoEnvelope and publishes nothing.
//
// When the broker rejects the message for exceeding its max payload, Publish
// retries once with only the minimal failure headers, because the failure
// headers add to the envelope size.
func (d *DLQ) Publish(ctx context.Context, rec event.DLQRecord) error {
	data := rec.Raw
	if len(data) == 0 {
		if d.registry == nil || rec.Evt == nil {
			return ErrNoEnvelope
		}
		md, ok := event.MetadataFromContext(ctx)
		if !ok {
			md = event.ResolveOutbound(ctx, rec.Evt, event.ApplyEmitOptions(rec.Evt))
		}
		enc, err := event.EncodeEvent(ctx, d.registry, rec.Evt, md)
		if err != nil {
			return fmt.Errorf("jetstream: DLQ encode %q: %w", rec.Evt.Name(), err)
		}
		data = enc
	}

	h, origMsgID := dlqCopyHeaders(rec.Headers)
	for k, v := range rec.FailureHeaders() {
		h.Set(k, v)
	}
	if origMsgID != "" {
		h.Set(event.HeaderDLQOriginalMsgID, event.TruncateHeader(origMsgID, event.DLQHeaderValueLimit))
	}
	subject := d.prefix + dlqSubject(rec)

	err := d.send(ctx, subject, h, data)
	if errors.Is(err, nats.ErrMaxPayload) {
		err = d.send(ctx, subject, dlqMinimalHeaders(rec), data)
	}
	if err != nil {
		d.metrics.IncCounter("dlq_publish_error_total", 1, invoker.Labels{})
		return err
	}
	d.metrics.IncCounter("dlq_published_total", 1, invoker.Labels{})
	return nil
}

// send publishes one message with no publish options, so no message id is set.
func (d *DLQ) send(ctx context.Context, subject string, h nats.Header, data []byte) error {
	_, err := d.js.PublishMsg(ctx, &nats.Msg{Subject: subject, Header: h, Data: data})
	return err
}

// dlqMinimalHeaders is the header set of the max-payload retry: the original
// headers plus only the reason, original subject, event id, handler and a
// shortened cause.
func dlqMinimalHeaders(rec event.DLQRecord) nats.Header {
	h, _ := dlqCopyHeaders(rec.Headers)
	full := rec.FailureHeaders()
	for _, k := range []string{
		event.HeaderDLQReason,
		event.HeaderDLQOriginalSubject,
		event.HeaderDLQEventID,
		event.HeaderDLQHandler,
	} {
		if v, ok := full[k]; ok {
			h.Set(k, v)
		}
	}
	if v, ok := full[event.HeaderDLQCause]; ok {
		h.Set(event.HeaderDLQCause, event.TruncateHeader(v, dlqMinimalCauseLimit))
	}
	return h
}

// dlqSubject is the original subject of rec, falling back to the event name.
func dlqSubject(rec event.DLQRecord) string {
	switch {
	case rec.Subject != "":
		return rec.Subject
	case rec.EventName != "":
		return rec.EventName
	default:
		return "unknown"
	}
}

// dlqCopyHeaders copies the original headers of a record into a fresh
// nats.Header. Broker system headers (Nats- prefix) and stale Eventbus-Dlq-*
// failure headers are dropped; the first Nats-Msg-Id value is returned so the
// caller can preserve it under HeaderDLQOriginalMsgID.
func dlqCopyHeaders(src map[string][]string) (nats.Header, string) {
	h := make(nats.Header, len(src)+16)
	var origMsgID string
	for k, vs := range src {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, strings.ToLower(natsHeaderPrefix)) {
			if lk == "nats-msg-id" && origMsgID == "" && len(vs) > 0 {
				origMsgID = vs[0]
			}
			continue
		}
		if strings.HasPrefix(lk, strings.ToLower(event.HeaderDLQPrefix)) {
			continue
		}
		h[k] = slices.Clone(vs)
	}
	return h, origMsgID
}
