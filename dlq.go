package event

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

// DLQPublisher publishes a failed message to a dead-letter queue.
//
// Delivery to the queue is at-least-once: a publisher may be called again for
// the same message after a crash or a failed acknowledgement, so consumers of
// the queue must tolerate duplicates. Users configure the same publisher on
// the DLQ invoker and on the bus, so a failure raised outside the invoker
// chain is dead-lettered too.
type DLQPublisher interface {
	Publish(ctx context.Context, rec DLQRecord) error
}

// DLQRecord is everything a DLQPublisher needs to dead-letter one message: the
// original bytes and headers plus the failure context.
type DLQRecord struct {
	// Raw is the original envelope bytes. It is empty for LocalBus, where the
	// publisher encodes Evt instead. When Evt is nil the record is a decode
	// failure and Raw is the complete wire form, possibly empty: a publisher
	// stores it as it is, never encoding or rejecting it.
	Raw []byte
	// Headers are the transport headers of the original message.
	Headers map[string][]string
	// Evt is the decoded event. It is nil when decoding failed.
	Evt Event
	// EventID and EventName identify the event, empty when unknown.
	EventID   string
	EventName string
	// Cause is the failure being dead-lettered.
	Cause error
	// Reason is the stable reason, see ReasonOf.
	Reason string
	// Handler is the handler that failed, empty for a decode failure.
	Handler string
	// Subject is the subject the message was received on.
	Subject string
	// Attempts is the number of in-process handler attempts, see AttemptsOf.
	Attempts int
	// Delivery and Execution are the delivery and handler context.
	Delivery  Delivery
	Execution Execution
	// FailedAt is when the record was built, in UTC.
	FailedAt time.Time
}

// NewDLQRecord builds the DLQRecord for a message that failed with cause.
// Delivery, Execution and the event identity come from ctx; a ctx without a
// Delivery is treated as a single final delivery.
func NewDLQRecord(ctx context.Context, evt Event, handler string, cause error) DLQRecord {
	d, ok := DeliveryFromContext(ctx)
	if !ok {
		d = Delivery{Count: 1, Max: 1}
	}
	x, _ := ExecutionFromContext(ctx)
	if handler == "" {
		handler = x.Handler
	}
	rec := DLQRecord{
		Raw:       d.Raw,
		Headers:   d.Headers,
		Evt:       evt,
		Cause:     cause,
		Reason:    ReasonOf(cause),
		Handler:   handler,
		Subject:   d.Subject,
		Attempts:  AttemptsOf(cause),
		Delivery:  d,
		Execution: x,
		FailedAt:  time.Now().UTC(),
	}
	if md, ok := MetadataFromContext(ctx); ok {
		rec.EventID = md.ID
		rec.EventName = md.Name
	}
	if evt != nil {
		if rec.EventName == "" {
			rec.EventName = evt.Name()
		}
		if rec.EventID == "" {
			if w, ok := evt.(WithId); ok {
				rec.EventID = w.Id()
			}
		}
	}
	return rec
}

const (
	publishAttemptsFinal = 3
	publishBackoff       = 100 * time.Millisecond
)

// PublishDeadLetter publishes rec through p. On a final delivery a failing
// publish is retried in-process, 3 attempts with 100 ms then 200 ms between
// them, because no redelivery will give it another chance; otherwise it is
// tried once. It stops early when ctx ends. It returns nil on the first
// success and the last publish error otherwise.
func PublishDeadLetter(ctx context.Context, p DLQPublisher, rec DLQRecord) error {
	attempts := 1
	if rec.Delivery.Final() {
		attempts = publishAttemptsFinal
	}
	delay := publishBackoff
	var err error
	for i := 0; i < attempts; i++ {
		if err = p.Publish(ctx, rec); err == nil {
			return nil
		}
		if i == attempts-1 {
			break
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return err
		case <-t.C:
		}
		delay *= 2
	}
	return err
}

// MemoryDLQ is a concurrency-safe in-memory DLQPublisher for tests and
// examples. The zero value is ready to use.
type MemoryDLQ struct {
	mu   sync.Mutex
	recs []DLQRecord
}

// NewMemoryDLQ returns an empty MemoryDLQ.
func NewMemoryDLQ() *MemoryDLQ { return &MemoryDLQ{} }

// Publish stores a copy of rec.
func (m *MemoryDLQ) Publish(_ context.Context, rec DLQRecord) error {
	rec = cloneRecord(rec)
	m.mu.Lock()
	m.recs = append(m.recs, rec)
	m.mu.Unlock()
	return nil
}

// Records returns a copy of the stored records, oldest first.
func (m *MemoryDLQ) Records() []DLQRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]DLQRecord, len(m.recs))
	for i, r := range m.recs {
		out[i] = cloneRecord(r)
	}
	return out
}

// Len returns the number of stored records.
func (m *MemoryDLQ) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.recs)
}

func cloneRecord(r DLQRecord) DLQRecord {
	r.Raw = slices.Clone(r.Raw)
	if r.Headers != nil {
		h := make(map[string][]string, len(r.Headers))
		for k, v := range r.Headers {
			h[k] = slices.Clone(v)
		}
		r.Headers = h
	}
	return r
}

// Wire names of the headers a DLQ publisher writes. They are wire format once
// released, so they never change.
const (
	HeaderDLQPrefix           = "Eventbus-Dlq-"
	HeaderDLQCause            = "Eventbus-Dlq-Cause"
	HeaderDLQReason           = "Eventbus-Dlq-Reason"
	HeaderDLQAttempts         = "Eventbus-Dlq-Attempts"
	HeaderDLQDeliveryCount    = "Eventbus-Dlq-Delivery-Count"
	HeaderDLQHandler          = "Eventbus-Dlq-Handler"
	HeaderDLQOriginalSubject  = "Eventbus-Dlq-Original-Subject"
	HeaderDLQFailedAt         = "Eventbus-Dlq-Failed-At"
	HeaderDLQFirstAttemptAt   = "Eventbus-Dlq-First-Attempt-At"
	HeaderDLQEventID          = "Eventbus-Dlq-Event-Id"
	HeaderDLQEventName        = "Eventbus-Dlq-Event-Name"
	HeaderDLQOriginalMsgID    = "Eventbus-Dlq-Original-Msg-Id"
	HeaderDLQOriginalStream   = "Eventbus-Dlq-Original-Stream"
	HeaderDLQOriginalSequence = "Eventbus-Dlq-Original-Sequence"
	HeaderDLQPublishedAt      = "Eventbus-Dlq-Published-At"
)

// DLQHeaderValueLimit is the maximum byte length of one DLQ failure header.
const DLQHeaderValueLimit = 1024

// TruncationMarker ends a header value that TruncateHeader shortened.
const TruncationMarker = "…[truncated]"

// TruncateHeader makes s safe to use as a header value of at most max bytes:
// invalid UTF-8 is replaced with U+FFFD, every control character (CR, LF, tab
// and the rest) becomes a space, and a longer result is cut at a rune boundary
// and ends with TruncationMarker. When max is not larger than the marker the
// cut has no marker.
func TruncateHeader(s string, max int) string {
	if max <= 0 {
		return ""
	}
	s = strings.ToValidUTF8(s, "\uFFFD")
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
	if len(s) <= max {
		return s
	}
	if max <= len(TruncationMarker) {
		return s[:runeBoundary(s, max)]
	}
	return s[:runeBoundary(s, max-len(TruncationMarker))] + TruncationMarker
}

// runeBoundary returns the largest index at or below n that does not fall in
// the middle of a rune of s.
func runeBoundary(s string, n int) int {
	for n > 0 && n < len(s) && !utf8.RuneStart(s[n]) {
		n--
	}
	return n
}

// FailureHeaders returns the Eventbus-Dlq-* headers that describe the failure.
// Every value goes through TruncateHeader at DLQHeaderValueLimit; the envelope
// itself is never truncated. Headers whose value is unknown (empty text, zero
// time, zero sequence) are omitted. Attempts is always present: 0 means the
// handler never ran. HeaderDLQOriginalMsgID is not set here, a broker adapter
// adds it from the stripped broker message id.
func (r DLQRecord) FailureHeaders() map[string]string {
	h := make(map[string]string, 13)
	set := func(key, value string) {
		if value != "" {
			h[key] = TruncateHeader(value, DLQHeaderValueLimit)
		}
	}
	setTime := func(key string, t time.Time) {
		if !t.IsZero() {
			set(key, t.UTC().Format(time.RFC3339Nano))
		}
	}
	if r.Cause != nil {
		set(HeaderDLQCause, r.Cause.Error())
	}
	set(HeaderDLQReason, r.Reason)
	h[HeaderDLQAttempts] = strconv.Itoa(r.Attempts)
	if r.Delivery.Count > 0 {
		set(HeaderDLQDeliveryCount, strconv.Itoa(r.Delivery.Count))
	}
	set(HeaderDLQHandler, r.Handler)
	set(HeaderDLQOriginalSubject, r.Subject)
	setTime(HeaderDLQFailedAt, r.FailedAt)
	setTime(HeaderDLQFirstAttemptAt, r.Execution.StartedAt)
	set(HeaderDLQEventID, r.EventID)
	set(HeaderDLQEventName, r.EventName)
	set(HeaderDLQOriginalStream, r.Delivery.Stream)
	if r.Delivery.StreamSeq > 0 {
		set(HeaderDLQOriginalSequence, strconv.FormatUint(r.Delivery.StreamSeq, 10))
	}
	setTime(HeaderDLQPublishedAt, r.Delivery.PublishedAt)
	return h
}
