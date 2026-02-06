package admin

import (
	"context"
	"fmt"
	"sync"

	"github.com/codermast/rocketmq-admin-go/protocol/remoting"
)

// Client 是 RocketMQ 运维管理客户端
type Client struct {
	opts    *Options                 // 客户端配置
	pool    *remoting.ConnectionPool // 连接池
	mu      sync.RWMutex             // 保护内部状态
	started bool                     // 是否已启动
	closed  bool                     // 是否已关闭
}

// NewClient 创建新的运维管理客户端
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

// Start 启动客户端
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

// Close 关闭客户端，释放资源
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	// 关闭连接池
	if c.pool != nil {
		c.pool.Close()
	}

	c.closed = true
	return nil
}

// IsStarted 返回客户端是否已启动
func (c *Client) IsStarted() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.started
}

// IsClosed 返回客户端是否已关闭
func (c *Client) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

// =============================================================================
// 内部辅助方法
// =============================================================================

// invokeNameServer 向 NameServer 发送请求
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

// invokeBroker 向 Broker 发送请求
func (c *Client) invokeBroker(ctx context.Context, brokerAddr string, cmd *remoting.RemotingCommand) (*remoting.RemotingCommand, error) {
	conn, err := c.pool.GetOrCreate(brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("连接 Broker 失败: %w", err)
	}

	resp, err := conn.InvokeSync(ctx, cmd)
	if err != nil {
		c.pool.Remove(brokerAddr)
		return nil, fmt.Errorf("请求 Broker 失败: %w", err)
	}

	return resp, nil
}
