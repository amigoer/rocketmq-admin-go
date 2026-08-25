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

// Client is a connection to a single RocketMQ server.
type Client struct {
	addr            string
	conn            net.Conn
	mu              sync.RWMutex // guards conn and connected
	connected       bool
	responseTables  map[int32]chan *RemotingCommand // keyed by request Opaque
	responseTableMu sync.RWMutex
	timeout         time.Duration
	closeChan       chan struct{}
}

// NewClient creates a client for addr without dialing it.
func NewClient(addr string, timeout time.Duration) *Client {
	return &Client{
		addr:           addr,
		timeout:        timeout,
		responseTables: make(map[int32]chan *RemotingCommand),
		closeChan:      make(chan struct{}),
	}
}

// Connect dials the server and starts the response reader.
func (c *Client) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected {
		return nil
	}

	conn, err := net.DialTimeout("tcp", c.addr, c.timeout)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}

	c.conn = conn
	c.connected = true

	go c.readLoop()

	return nil
}

// Close shuts the connection down. It is idempotent.
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

// IsConnected reports whether the client holds an open connection.
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

// InvokeSync sends cmd and waits for the response with the same Opaque.
func (c *Client) InvokeSync(ctx context.Context, cmd *RemotingCommand) (*RemotingCommand, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	respChan := make(chan *RemotingCommand, 1)
	c.responseTableMu.Lock()
	c.responseTables[cmd.Opaque] = respChan
	c.responseTableMu.Unlock()

	defer func() {
		c.responseTableMu.Lock()
		delete(c.responseTables, cmd.Opaque)
		c.responseTableMu.Unlock()
	}()

	if err := c.send(cmd); err != nil {
		return nil, err
	}

	select {
	case resp := <-respChan:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closeChan:
		return nil, ErrConnectionClosed
	}
}

// InvokeOneway sends cmd without waiting for a response.
func (c *Client) InvokeOneway(cmd *RemotingCommand) error {
	if !c.IsConnected() {
		return ErrNotConnected
	}
	cmd.MarkOnewayRPC()
	return c.send(cmd)
}

func (c *Client) send(cmd *RemotingCommand) error {
	data, err := cmd.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode command: %w", err)
	}

	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}

	_, err = conn.Write(data)
	if err != nil {
		return fmt.Errorf("failed to send data: %w", err)
	}

	return nil
}

// readLoop hands each response to whoever is waiting on its Opaque.
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

		lengthBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lengthBuf); err != nil {
			// Connection closed or broken; stop reading.
			return
		}

		totalLen := int(binary.BigEndian.Uint32(lengthBuf))
		if totalLen <= 0 || totalLen > 1024*1024*16 { // cap a frame at 16MB
			continue
		}

		data := make([]byte, totalLen)
		if _, err := io.ReadFull(conn, data); err != nil {
			return
		}

		resp, err := Decode(data)
		if err != nil {
			continue
		}

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

var (
	ErrNotConnected     = &RemotingError{Message: "未连接"}
	ErrConnectionClosed = &RemotingError{Message: "连接已关闭"}
)
