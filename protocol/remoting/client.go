// Package remoting 实现 RocketMQ Remoting 通信协议
package remoting

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Client 远程通信客户端
type Client struct {
	addr            string                         // 服务器地址
	conn            net.Conn                       // TCP 连接
	mu              sync.RWMutex                   // 保护内部状态
	connected       bool                           // 是否已连接
	responseTables  map[int32]chan *RemotingCommand // 响应表
	responseTableMu sync.RWMutex                   // 响应表锁
	timeout         time.Duration                  // 默认超时时间
	closeChan       chan struct{}                  // 关闭信号
}

// NewClient 创建新的远程通信客户端
func NewClient(addr string, timeout time.Duration) *Client {
	return &Client{
		addr:           addr,
		timeout:        timeout,
		responseTables: make(map[int32]chan *RemotingCommand),
		closeChan:      make(chan struct{}),
	}
}

// Connect 连接服务器
func (c *Client) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected {
		return nil
	}

	conn, err := net.DialTimeout("tcp", c.addr, c.timeout)
	if err != nil {
		return fmt.Errorf("连接服务器失败: %w", err)
	}

	c.conn = conn
	c.connected = true

	// 启动响应读取 goroutine
	go c.readLoop()

	return nil
}

// Close 关闭连接
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected {
		return nil
	}

	close(c.closeChan)
	c.connected = false

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// IsConnected 是否已连接
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

// InvokeSync 同步调用
func (c *Client) InvokeSync(ctx context.Context, cmd *RemotingCommand) (*RemotingCommand, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	// 创建响应通道
	respChan := make(chan *RemotingCommand, 1)
	c.responseTableMu.Lock()
	c.responseTables[cmd.Opaque] = respChan
	c.responseTableMu.Unlock()

	// 确保清理响应通道
	defer func() {
		c.responseTableMu.Lock()
		delete(c.responseTables, cmd.Opaque)
		c.responseTableMu.Unlock()
	}()

	// 发送请求
	if err := c.send(cmd); err != nil {
		return nil, err
	}

	// 等待响应
	select {
	case resp := <-respChan:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closeChan:
		return nil, ErrConnectionClosed
	}
}

// InvokeOneway 单向调用（不等待响应）
func (c *Client) InvokeOneway(cmd *RemotingCommand) error {
	if !c.IsConnected() {
		return ErrNotConnected
	}
	cmd.MarkOnewayRPC()
	return c.send(cmd)
}

// send 发送命令
func (c *Client) send(cmd *RemotingCommand) error {
	data, err := cmd.Encode()
	if err != nil {
		return fmt.Errorf("编码命令失败: %w", err)
	}

	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}

	_, err = conn.Write(data)
	if err != nil {
		return fmt.Errorf("发送数据失败: %w", err)
	}

	return nil
}

// readLoop 读取响应循环
func (c *Client) readLoop() {
	for {
		select {
		case <-c.closeChan:
			return
		default:
		}

		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		if conn == nil {
			return
		}

		// 读取总长度
		lengthBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lengthBuf); err != nil {
			// 连接关闭或错误
			return
		}

		totalLen := int(binary.BigEndian.Uint32(lengthBuf))
		if totalLen <= 0 || totalLen > 1024*1024*16 { // 最大 16MB
			continue
		}

		// 读取完整数据
		data := make([]byte, totalLen)
		if _, err := io.ReadFull(conn, data); err != nil {
			return
		}

		// 解码响应
		resp, err := Decode(data)
		if err != nil {
			continue
		}

		// 分发响应
		c.responseTableMu.RLock()
		respChan, ok := c.responseTables[resp.Opaque]
		c.responseTableMu.RUnlock()

		if ok {
			select {
			case respChan <- resp:
			default:
			}
		}
	}
}

// 错误定义
var (
	ErrNotConnected     = &RemotingError{Message: "未连接"}
	ErrConnectionClosed = &RemotingError{Message: "连接已关闭"}
)

