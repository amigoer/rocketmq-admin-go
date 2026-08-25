package admin

import "errors"

// Sentinel errors returned by this package; compare them with errors.Is.
var (
	ErrNotImplemented        = errors.New("not implemented")
	ErrAlreadyStarted        = errors.New("client already started")
	ErrClientClosed          = errors.New("client closed")
	ErrBrokerNotFound        = errors.New("broker not found")
	ErrTopicNotFound         = errors.New("topic not found")
	ErrConsumerGroupNotFound = errors.New("consumer group not found")
	ErrTimeout               = errors.New("request timeout")
	ErrConnectionFailed      = errors.New("connection failed")
	ErrInvalidResponse       = errors.New("invalid response")
	ErrPermissionDenied      = errors.New("permission denied")
)

// AdminError carries the response code and remark returned by the broker.
type AdminError struct {
	Code    int
	Message string
}

// Error implements the error interface.
func (e *AdminError) Error() string {
	return e.Message
}

// NewAdminError builds an AdminError from a response code and remark.
func NewAdminError(code int, message string) *AdminError {
	return &AdminError{
		Code:    code,
		Message: message,
	}
}
