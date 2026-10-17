package event

// Trace and replay header names. They live outside the envelope, which stays
// verbatim (Phase 8 D-18); consumers read them from Delivery.Headers.
//
// RED stub: values are filled in by the GREEN commit.
const (
	HeaderDLQTraceparent          = ""
	HeaderReplayTraceparent       = ""
	HeaderReplayFailedTraceparent = ""
	HeaderReplayDLQSequence       = ""
)
