package admin

import (
	"errors"
	"time"
)

// Options 客户端配置选项
type Options struct {
	// NameServers NameServer 地址列表
	NameServers []string

	// Timeout 请求超时时间
	Timeout time.Duration

	// RetryTimes 重试次数
	RetryTimes int

	// ACL 认证配置
	AccessKey string
	SecretKey string
}

// Option 配置选项函数类型
type Option func(*Options)

// defaultOptions 返回默认配置
func defaultOptions() *Options {
	return &Options{
		Timeout:    3 * time.Second,
		RetryTimes: 2,
	}
}

// validate 验证配置有效性
func (o *Options) validate() error {
	if len(o.NameServers) == 0 {
		return errors.New("NameServers 不能为空")
	}
	return nil
}

// WithNameServers 设置 NameServer 地址列表
func WithNameServers(addrs []string) Option {
	return func(o *Options) {
		o.NameServers = addrs
	}
}

// WithTimeout 设置请求超时时间
func WithTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.Timeout = timeout
	}
}

// WithRetryTimes 设置重试次数
func WithRetryTimes(times int) Option {
	return func(o *Options) {
		o.RetryTimes = times
	}
}

// WithACL 设置 ACL 认证信息
func WithACL(accessKey, secretKey string) Option {
	return func(o *Options) {
		o.AccessKey = accessKey
		o.SecretKey = secretKey
	}
}

