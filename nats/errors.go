package nats

import (
	"errors"
	"strings"

	"github.com/isaquesb/go-event-bus"
)

// HeaderError is the reply header a SubscribeSync responder sets on a failed
// request. Its value is the event reason code (see event.ReasonOf), optionally
// followed by ": " and a truncated error message when BusOptions.ReplyErrorMessage
// is enabled. A reply that carries it has no body.
const HeaderError = "Eventbus-Error"

var (
	// ErrRemoteFailure matches every *RemoteError: the responder handled the
	// request and failed.
	ErrRemoteFailure = errors.New("nats: remote failure")

	// ErrRemoteDecode matches a *RemoteError whose reason is a decode failure
	// (decode_* or unknown_version): the responder could not decode the request.
	ErrRemoteDecode = errors.New("nats: remote decode failure")

	// ErrRemoteHandler matches a *RemoteError whose reason is not a decode
	// failure: the responder decoded the request and its handler failed.
	ErrRemoteHandler = errors.New("nats: remote handler failure")
)

// RemoteError is what EmitSync returns when the responder replied with the
// HeaderError header instead of waiting for the request to time out.
//
// errors.Is matches ErrRemoteFailure, the class sentinel (ErrRemoteDecode or
// ErrRemoteHandler) and the root sentinel for the reason, for example
// event.ErrSendToDLQ for "terminal".
type RemoteError struct {
	// Subject is the subject the request was sent to.
	Subject string
	// Reason is the stable reason code the responder reported.
	Reason string
	// Message is the responder's error text. It is empty unless the responder
	// enabled BusOptions.ReplyErrorMessage.
	Message string
}

// Error implements error.
func (e *RemoteError) Error() string {
	if e == nil {
		return ErrRemoteFailure.Error()
	}
	s := "nats: remote failure on " + e.Subject + ": " + e.Reason
	if e.Message != "" {
		s += ": " + e.Message
	}
	return s
}

// Is reports whether target is ErrRemoteFailure, the class sentinel of the
// reason or the root sentinel mapped from the reason.
func (e *RemoteError) Is(target error) bool {
	if e == nil {
		return false
	}
	switch target {
	case ErrRemoteFailure:
		return true
	case ErrRemoteDecode:
		return e.isDecode()
	case ErrRemoteHandler:
		return !e.isDecode()
	}
	root, ok := reasonSentinels[e.Reason]
	return ok && target == root
}

func (e *RemoteError) isDecode() bool {
	return strings.HasPrefix(e.Reason, "decode_") || e.Reason == event.ReasonUnknownVersion
}

// reasonSentinels maps a reason code carried by HeaderError to the root
// sentinel that describes it.
var reasonSentinels = map[string]error{
	event.ReasonTerminal:              event.ErrSendToDLQ,
	event.ReasonRetriesExhausted:      event.ErrRetriesExhausted,
	event.ReasonCircuitOpen:           event.ErrCircuitOpen,
	event.ReasonRateLimited:           event.ErrRateLimited,
	event.ReasonInProgress:            event.ErrInProgress,
	event.ReasonUnknownVersion:        event.ErrUnknownVersion,
	event.ReasonDecodeMalformed:       event.ErrMalformedEnvelope,
	event.ReasonDecodeUnknownEvent:    event.ErrUnknownEvent,
	event.ReasonDecodeMissingUpcaster: event.ErrMissingUpcaster,
	event.ReasonDuplicate:             event.ErrDuplicate,
}

// errorHeaderValue builds the HeaderError value: the reason alone, or the reason
// followed by ": " and the error text truncated to event.DLQHeaderValueLimit
// when withMessage is set (D-30).
func errorHeaderValue(reason string, err error, withMessage bool) string {
	if !withMessage || err == nil {
		return reason
	}
	return reason + ": " + event.TruncateHeader(err.Error(), event.DLQHeaderValueLimit)
}

// parseRemoteError builds the RemoteError for a HeaderError value received in a
// reply to a request sent to subject.
func parseRemoteError(subject, value string) *RemoteError {
	reason, msg, _ := strings.Cut(value, ": ")
	return &RemoteError{Subject: subject, Reason: reason, Message: msg}
}
