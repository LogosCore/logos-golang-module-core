// Package errors is the typed error used across Logos module SDKs: a
// machine-readable Code with a message and an optional wrapped cause. SDKs
// built on module-core (channel-core, ...) add their own codes on the same
// Error type.
package errors

import (
	stderrs "errors"
	"fmt"
)

// Codes shared by every module SDK.
const (
	CodeInvalidInput   = "ERR_INVALID_INPUT"
	CodeNotImplemented = "ERR_NOT_IMPLEMENTED"
	CodeInternal       = "ERR_INTERNAL"
)

// Error is a machine-readable error for runtime and RPC surfaces.
type Error struct {
	Code    string
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Err)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// New returns an Error with code and message.
func New(code, message string) error {
	return &Error{Code: code, Message: message}
}

// Wrap returns an Error with code and message wrapping err (New when err is
// nil).
func Wrap(code, message string, err error) error {
	if err == nil {
		return New(code, message)
	}
	return &Error{Code: code, Message: message, Err: err}
}

// Code returns the code of the first Error in err's chain, CodeInternal for
// any other error, and "" for nil.
func Code(err error) string {
	if err == nil {
		return ""
	}
	var typed *Error
	if stderrs.As(err, &typed) {
		return typed.Code
	}
	return CodeInternal
}
