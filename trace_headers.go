package event

// Trace and replay header names. All of them live outside the envelope, which
// stays verbatim (Phase 8 D-18); consumers read them from Delivery.Headers.
const (
	// HeaderDLQTraceparent is the W3C traceparent of the span where the message
	// was dead-lettered, written by broker DLQ publishers.
	HeaderDLQTraceparent = "Eventbus-Dlq-Traceparent"

	// HeaderReplayTraceparent is the traceparent of the replay span.
	HeaderReplayTraceparent = "Eventbus-Replay-Traceparent"

	// HeaderReplayFailedTraceparent is the dead-letter span copied by the
	// replayer, because the replayer strips the Eventbus-Dlq-* headers
	// (Phase 8 D-18).
	HeaderReplayFailedTraceparent = "Eventbus-Replay-Failed-Traceparent"

	// HeaderReplayDLQSequence is the DLQ stream sequence of the replayed record.
	HeaderReplayDLQSequence = "Eventbus-Replay-Dlq-Sequence"
)
