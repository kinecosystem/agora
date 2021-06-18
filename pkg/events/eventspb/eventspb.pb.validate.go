// Code generated by protoc-gen-validate. DO NOT EDIT.
// source: eventspb.proto

package eventspb

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/mail"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/golang/protobuf/ptypes"
)

// ensure the imports are used
var (
	_ = bytes.MinRead
	_ = errors.New("")
	_ = fmt.Print
	_ = utf8.UTFMax
	_ = (*regexp.Regexp)(nil)
	_ = (*strings.Reader)(nil)
	_ = net.IPv4len
	_ = time.Duration(0)
	_ = (*url.URL)(nil)
	_ = (*mail.Address)(nil)
	_ = ptypes.DynamicAny{}
)

// define the regex for a UUID once up-front
var _eventspb_uuidPattern = regexp.MustCompile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

// Validate checks the field values on Event with the rules defined in the
// proto definition for this message. If any rules are violated, an error is returned.
func (m *Event) Validate() error {
	if m == nil {
		return nil
	}

	if m.GetSubmissionTime() == nil {
		return EventValidationError{
			field:  "SubmissionTime",
			reason: "value is required",
		}
	}

	switch m.Kind.(type) {

	case *Event_TransactionEvent:

		if v, ok := interface{}(m.GetTransactionEvent()).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return EventValidationError{
					field:  "TransactionEvent",
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	case *Event_SimulationEvent:

		if v, ok := interface{}(m.GetSimulationEvent()).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return EventValidationError{
					field:  "SimulationEvent",
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	}

	return nil
}

// EventValidationError is the validation error returned by Event.Validate if
// the designated constraints aren't met.
type EventValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e EventValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e EventValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e EventValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e EventValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e EventValidationError) ErrorName() string { return "EventValidationError" }

// Error satisfies the builtin error interface
func (e EventValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sEvent.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = EventValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = EventValidationError{}

// Validate checks the field values on TransactionEvent with the rules defined
// in the proto definition for this message. If any rules are violated, an
// error is returned.
func (m *TransactionEvent) Validate() error {
	if m == nil {
		return nil
	}

	if l := len(m.GetTransaction()); l < 1 || l > 1232 {
		return TransactionEventValidationError{
			field:  "Transaction",
			reason: "value length must be between 1 and 1232 bytes, inclusive",
		}
	}

	if len(m.GetTransactionError()) > 10240 {
		return TransactionEventValidationError{
			field:  "TransactionError",
			reason: "value length must be at most 10240 bytes",
		}
	}

	return nil
}

// TransactionEventValidationError is the validation error returned by
// TransactionEvent.Validate if the designated constraints aren't met.
type TransactionEventValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e TransactionEventValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e TransactionEventValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e TransactionEventValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e TransactionEventValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e TransactionEventValidationError) ErrorName() string { return "TransactionEventValidationError" }

// Error satisfies the builtin error interface
func (e TransactionEventValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sTransactionEvent.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = TransactionEventValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = TransactionEventValidationError{}

// Validate checks the field values on SimulationEvent with the rules defined
// in the proto definition for this message. If any rules are violated, an
// error is returned.
func (m *SimulationEvent) Validate() error {
	if m == nil {
		return nil
	}

	if l := len(m.GetTransaction()); l < 1 || l > 1232 {
		return SimulationEventValidationError{
			field:  "Transaction",
			reason: "value length must be between 1 and 1232 bytes, inclusive",
		}
	}

	if len(m.GetTransactionError()) > 10240 {
		return SimulationEventValidationError{
			field:  "TransactionError",
			reason: "value length must be at most 10240 bytes",
		}
	}

	return nil
}

// SimulationEventValidationError is the validation error returned by
// SimulationEvent.Validate if the designated constraints aren't met.
type SimulationEventValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e SimulationEventValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e SimulationEventValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e SimulationEventValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e SimulationEventValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e SimulationEventValidationError) ErrorName() string { return "SimulationEventValidationError" }

// Error satisfies the builtin error interface
func (e SimulationEventValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sSimulationEvent.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = SimulationEventValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = SimulationEventValidationError{}
