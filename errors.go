package admin

import "errors"

// 预定义错误
var (
	// ErrNotImplemented 功能未实现
	ErrNotImplemented = errors.New("功能未实现")

	// ErrAlreadyStarted 客户端已启动
	ErrAlreadyStarted = errors.New("客户端已启动")

	// ErrClientClosed 客户端已关闭
	ErrClientClosed = errors.New("客户端已关闭")

	// ErrBrokerNotFound Broker 未找到
	ErrBrokerNotFound = errors.New("Broker 未找到")

	// ErrTopicNotFound Topic 未找到
	ErrTopicNotFound = errors.New("Topic 未找到")

	// ErrConsumerGroupNotFound 消费者组未找到
	ErrConsumerGroupNotFound = errors.New("消费者组未找到")

	// ErrTimeout 请求超时
	ErrTimeout = errors.New("请求超时")

	// ErrConnectionFailed 连接失败
	ErrConnectionFailed = errors.New("连接失败")

	// ErrInvalidResponse 无效响应
	ErrInvalidResponse = errors.New("无效响应")

	// ErrPermissionDenied 权限不足
	ErrPermissionDenied = errors.New("权限不足")
)

// AdminError 运维操作错误
type AdminError struct {
	Code    int    // 错误码
	Message string // 错误信息
}

// Error 实现 error 接口
func (e *AdminError) Error() string {
	return e.Message
}

// NewAdminError 创建运维错误
func NewAdminError(code int, message string) *AdminError {
	return &AdminError{
		Code:    code,
		Message: message,
	}
}

