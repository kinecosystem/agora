// Code generated by protoc-gen-validate. DO NOT EDIT.
// source: admin_service.proto

package gcpb

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
var _admin_service_uuidPattern = regexp.MustCompile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

// Validate checks the field values on VoidResponse with the rules defined in
// the proto definition for this message. If any rules are violated, an error
// is returned.
func (m *VoidResponse) Validate() error {
	if m == nil {
		return nil
	}

	return nil
}

// VoidResponseValidationError is the validation error returned by
// VoidResponse.Validate if the designated constraints aren't met.
type VoidResponseValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e VoidResponseValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e VoidResponseValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e VoidResponseValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e VoidResponseValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e VoidResponseValidationError) ErrorName() string { return "VoidResponseValidationError" }

// Error satisfies the builtin error interface
func (e VoidResponseValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sVoidResponse.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = VoidResponseValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = VoidResponseValidationError{}

// Validate checks the field values on SetStateRequest with the rules defined
// in the proto definition for this message. If any rules are violated, an
// error is returned.
func (m *SetStateRequest) Validate() error {
	if m == nil {
		return nil
	}

	// no validation rules for State

	return nil
}

// SetStateRequestValidationError is the validation error returned by
// SetStateRequest.Validate if the designated constraints aren't met.
type SetStateRequestValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e SetStateRequestValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e SetStateRequestValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e SetStateRequestValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e SetStateRequestValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e SetStateRequestValidationError) ErrorName() string { return "SetStateRequestValidationError" }

// Error satisfies the builtin error interface
func (e SetStateRequestValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sSetStateRequest.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = SetStateRequestValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = SetStateRequestValidationError{}

// Validate checks the field values on SetRateLimitRequest with the rules
// defined in the proto definition for this message. If any rules are
// violated, an error is returned.
func (m *SetRateLimitRequest) Validate() error {
	if m == nil {
		return nil
	}

	// no validation rules for Rate

	return nil
}

// SetRateLimitRequestValidationError is the validation error returned by
// SetRateLimitRequest.Validate if the designated constraints aren't met.
type SetRateLimitRequestValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e SetRateLimitRequestValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e SetRateLimitRequestValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e SetRateLimitRequestValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e SetRateLimitRequestValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e SetRateLimitRequestValidationError) ErrorName() string {
	return "SetRateLimitRequestValidationError"
}

// Error satisfies the builtin error interface
func (e SetRateLimitRequestValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sSetRateLimitRequest.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = SetRateLimitRequestValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = SetRateLimitRequestValidationError{}

// Validate checks the field values on SetHistoryCheckRequest with the rules
// defined in the proto definition for this message. If any rules are
// violated, an error is returned.
func (m *SetHistoryCheckRequest) Validate() error {
	if m == nil {
		return nil
	}

	// no validation rules for Enabled

	return nil
}

// SetHistoryCheckRequestValidationError is the validation error returned by
// SetHistoryCheckRequest.Validate if the designated constraints aren't met.
type SetHistoryCheckRequestValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e SetHistoryCheckRequestValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e SetHistoryCheckRequestValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e SetHistoryCheckRequestValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e SetHistoryCheckRequestValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e SetHistoryCheckRequestValidationError) ErrorName() string {
	return "SetHistoryCheckRequestValidationError"
}

// Error satisfies the builtin error interface
func (e SetHistoryCheckRequestValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sSetHistoryCheckRequest.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = SetHistoryCheckRequestValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = SetHistoryCheckRequestValidationError{}

// Validate checks the field values on QueueRequest with the rules defined in
// the proto definition for this message. If any rules are violated, an error
// is returned.
func (m *QueueRequest) Validate() error {
	if m == nil {
		return nil
	}

	for idx, item := range m.GetItems() {
		_, _ = idx, item

		if v, ok := interface{}(item).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return QueueRequestValidationError{
					field:  fmt.Sprintf("Items[%v]", idx),
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	}

	return nil
}

// QueueRequestValidationError is the validation error returned by
// QueueRequest.Validate if the designated constraints aren't met.
type QueueRequestValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e QueueRequestValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e QueueRequestValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e QueueRequestValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e QueueRequestValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e QueueRequestValidationError) ErrorName() string { return "QueueRequestValidationError" }

// Error satisfies the builtin error interface
func (e QueueRequestValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sQueueRequest.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = QueueRequestValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = QueueRequestValidationError{}

// Validate checks the field values on QueueRequest_QueueItem with the rules
// defined in the proto definition for this message. If any rules are
// violated, an error is returned.
func (m *QueueRequest_QueueItem) Validate() error {
	if m == nil {
		return nil
	}

	// no validation rules for Key

	// no validation rules for IgnoreZeroBalance

	return nil
}

// QueueRequest_QueueItemValidationError is the validation error returned by
// QueueRequest_QueueItem.Validate if the designated constraints aren't met.
type QueueRequest_QueueItemValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e QueueRequest_QueueItemValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e QueueRequest_QueueItemValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e QueueRequest_QueueItemValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e QueueRequest_QueueItemValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e QueueRequest_QueueItemValidationError) ErrorName() string {
	return "QueueRequest_QueueItemValidationError"
}

// Error satisfies the builtin error interface
func (e QueueRequest_QueueItemValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sQueueRequest_QueueItem.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = QueueRequest_QueueItemValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = QueueRequest_QueueItemValidationError{}