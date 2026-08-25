package admin

import (
	"context"
	"fmt"
	"sync"

	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

// Client is a RocketMQ admin client. It is safe for concurrent use.
type Client struct {
	opts    *Options
	pool    *remoting.ConnectionPool
	mu      sync.RWMutex // guards started and closed
	started bool
	closed  bool

	// brokerNames maps a Broker address to its name so forwarded requests can
	// fill in the bname field. See brokername.go.
	brokerNameMu sync.RWMutex
	brokerNames  map[string]string
}

// NewClient creates an admin client from the given options.
func NewClient(opts ...Option) (*Client, error) {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	if err := options.validate(); err != nil {
		return nil, err
	}

	client := &Client{
		opts: options,
		pool: remoting.NewConnectionPool(options.Timeout),
	}

	return client, nil
}

// Start marks the client as usable.
func (c *Client) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return ErrAlreadyStarted
	}
	if c.closed {
		return ErrClientClosed
	}

	c.started = true
	return nil
}

// Close releases every pooled connection. It is idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	if c.pool != nil {
		c.pool.Close()
	}

	c.closed = true
	return nil
}

// IsStarted reports whether Start has been called.
func (c *Client) IsStarted() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.started
}

// IsClosed reports whether Close has been called.
func (c *Client) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

// invokeNameServer tries each configured NameServer until one answers.
func (c *Client) invokeNameServer(ctx context.Context, cmd *remoting.RemotingCommand) (*remoting.RemotingCommand, error) {
	var lastErr error
	for _, addr := range c.opts.NameServers {
		conn, err := c.pool.GetOrCreate(addr)
		if err != nil {
			lastErr = err
			continue
		}

		resp, err := conn.InvokeSync(ctx, cmd)
		if err != nil {
			lastErr = err
			c.pool.Remove(addr)
			continue
		}

		return resp, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("所有 NameServer 请求失败: %w", lastErr)
	}
	return nil, ErrConnectionFailed
}

// invokeBroker sends cmd to one Broker, dropping the connection on failure.
func (c *Client) invokeBroker(ctx context.Context, brokerAddr string, cmd *remoting.RemotingCommand) (*remoting.RemotingCommand, error) {
	conn, err := c.pool.GetOrCreate(brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("连接 Broker 失败: %w", err)
	}

	// A RocketMQ Proxy needs bname to know which Broker to forward to.
	if cmd != nil && cmd.ExtFields != nil {
		if _, exists := cmd.ExtFields[brokerNameField]; !exists {
			if name := c.brokerNameFor(brokerAddr); name != "" {
				cmd.ExtFields[brokerNameField] = name
			}
		}
	}

	resp, err := conn.InvokeSync(ctx, cmd)
	if err != nil {
		c.pool.Remove(brokerAddr)
		return nil, fmt.Errorf("请求 Broker 失败: %w", err)
	}

	return resp, nil
}
