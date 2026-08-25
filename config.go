package admin

import (
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

// Config is shared configuration for both the admin API and the message
// client: set it up once and use it with rocketmq-admin-go and
// rocketmq-client-go alike.
type Config struct {
	nameServers []string
	accessKey   string
	secretKey   string
	timeout     time.Duration
}

// NewConfig creates a Config for the given NameServer addresses, e.g.
// "localhost:9876".
func NewConfig(nameServers ...string) *Config {
	return &Config{
		nameServers: nameServers,
		timeout:     10 * time.Second,
	}
}

// WithCredentials sets the ACL credentials.
func (c *Config) WithCredentials(accessKey, secretKey string) *Config {
	c.accessKey = accessKey
	c.secretKey = secretKey
	return c
}

// WithTimeout sets the request timeout.
func (c *Config) WithTimeout(timeout time.Duration) *Config {
	c.timeout = timeout
	return c
}

// NewAdminClient creates the admin client used for cluster, topic and
// consumer management.
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

// NewProducer creates a message producer. Extra producer.Option values are
// appended to the ones derived from the Config.
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

// NewPushConsumer creates a push consumer. Extra consumer.Option values, such
// as WithGroupName, are appended to the ones derived from the Config.
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

// NewPullConsumer creates a pull consumer. Extra consumer.Option values are
// appended to the ones derived from the Config.
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

// NameServers returns the configured NameServer addresses.
func (c *Config) NameServers() []string {
	return c.nameServers
}

// HasCredentials reports whether ACL credentials are set.
func (c *Config) HasCredentials() bool {
	return c.accessKey != ""
}

// Timeout returns the request timeout.
func (c *Config) Timeout() time.Duration {
	return c.timeout
}
