package admin

import "errors"

// Sentinel errors returned by this package; compare them with errors.Is.
var (
	ErrNotImplemented        = errors.New("功能未实现")
	ErrAlreadyStarted        = errors.New("客户端已启动")
	ErrClientClosed          = errors.New("客户端已关闭")
	ErrBrokerNotFound        = errors.New("Broker 未找到")
	ErrTopicNotFound         = errors.New("Topic 未找到")
	ErrConsumerGroupNotFound = errors.New("消费者组未找到")
	ErrTimeout               = errors.New("请求超时")
	ErrConnectionFailed      = errors.New("连接失败")
	ErrInvalidResponse       = errors.New("无效响应")
	ErrPermissionDenied      = errors.New("权限不足")
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
