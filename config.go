package admin

import (
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

// =============================================================================
// 统一配置工厂
// =============================================================================

// Config 统一配置（同时支持 Admin 运维接口和 Client 消息收发接口）
// 用户只需配置一次，即可同时使用 rocketmq-admin-go 和 rocketmq-client-go
type Config struct {
	nameServers []string
	accessKey   string
	secretKey   string
	timeout     time.Duration
}

// NewConfig 创建统一配置
// nameServers: NameServer 地址列表，如 "localhost:9876"
func NewConfig(nameServers ...string) *Config {
	return &Config{
		nameServers: nameServers,
		timeout:     10 * time.Second,
	}
}

// WithCredentials 设置 ACL 认证凭据
func (c *Config) WithCredentials(accessKey, secretKey string) *Config {
	c.accessKey = accessKey
	c.secretKey = secretKey
	return c
}

// WithTimeout 设置超时时间
func (c *Config) WithTimeout(timeout time.Duration) *Config {
	c.timeout = timeout
	return c
}

// =============================================================================
// Admin Client 工厂方法
// =============================================================================

// NewAdminClient 创建 Admin 运维客户端
// 用于集群管理、Topic 管理、消费者监控等运维操作
func (c *Config) NewAdminClient() (*Client, error) {
	opts := []Option{
		WithNameServers(c.nameServers),
		WithTimeout(c.timeout),
	}
	if c.accessKey != "" {
		opts = append(opts, WithACL(c.accessKey, c.secretKey))
	}
	return NewClient(opts...)
}

// =============================================================================
// Producer 工厂方法
// =============================================================================

// NewProducer 创建消息生产者
// 可传入额外的 producer.Option 进行定制
func (c *Config) NewProducer(opts ...producer.Option) (rocketmq.Producer, error) {
	baseOpts := []producer.Option{
		producer.WithNsResolver(primitive.NewPassthroughResolver(c.nameServers)),
	}
	if c.accessKey != "" {
		baseOpts = append(baseOpts, producer.WithCredentials(primitive.Credentials{
			AccessKey: c.accessKey,
			SecretKey: c.secretKey,
		}))
	}
	return rocketmq.NewProducer(append(baseOpts, opts...)...)
}

// =============================================================================
// Consumer 工厂方法
// =============================================================================

// NewPushConsumer 创建 Push 模式消费者
// 可传入额外的 consumer.Option 进行定制（如 WithGroupName）
func (c *Config) NewPushConsumer(opts ...consumer.Option) (rocketmq.PushConsumer, error) {
	baseOpts := []consumer.Option{
		consumer.WithNsResolver(primitive.NewPassthroughResolver(c.nameServers)),
	}
	if c.accessKey != "" {
		baseOpts = append(baseOpts, consumer.WithCredentials(primitive.Credentials{
			AccessKey: c.accessKey,
			SecretKey: c.secretKey,
		}))
	}
	return rocketmq.NewPushConsumer(append(baseOpts, opts...)...)
}

// NewPullConsumer 创建 Pull 模式消费者
// 可传入额外的 consumer.Option 进行定制
func (c *Config) NewPullConsumer(opts ...consumer.Option) (rocketmq.PullConsumer, error) {
	baseOpts := []consumer.Option{
		consumer.WithNsResolver(primitive.NewPassthroughResolver(c.nameServers)),
	}
	if c.accessKey != "" {
		baseOpts = append(baseOpts, consumer.WithCredentials(primitive.Credentials{
			AccessKey: c.accessKey,
			SecretKey: c.secretKey,
		}))
	}
	return rocketmq.NewPullConsumer(append(baseOpts, opts...)...)
}

// =============================================================================
// 配置导出
// =============================================================================

// NameServers 返回 NameServer 地址列表
func (c *Config) NameServers() []string {
	return c.nameServers
}

// HasCredentials 检查是否配置了 ACL 认证
func (c *Config) HasCredentials() bool {
	return c.accessKey != ""
}

// Timeout 返回超时时间
func (c *Config) Timeout() time.Duration {
	return c.timeout
}
