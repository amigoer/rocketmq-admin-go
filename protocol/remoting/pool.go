// Package remoting 连接池管理
package remoting

import (
	"fmt"
	"sync"
	"time"
)

// ConnectionPool 连接池
type ConnectionPool struct {
	mu          sync.RWMutex
	connections map[string]*Client // key: addr
	timeout     time.Duration
}

// NewConnectionPool 创建连接池
func NewConnectionPool(timeout time.Duration) *ConnectionPool {
	return &ConnectionPool{
		connections: make(map[string]*Client),
		timeout:     timeout,
	}
}

// GetOrCreate 获取或创建连接
func (p *ConnectionPool) GetOrCreate(addr string) (*Client, error) {
	p.mu.RLock()
	client, exists := p.connections[addr]
	p.mu.RUnlock()

	if exists && client.IsConnected() {
		return client, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// 双重检查
	if client, exists = p.connections[addr]; exists && client.IsConnected() {
		return client, nil
	}

	// 创建新连接
	client = NewClient(addr, p.timeout)
	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("连接 %s 失败: %w", addr, err)
	}

	p.connections[addr] = client
	return client, nil
}

// Close 关闭所有连接
func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var lastErr error
	for addr, client := range p.connections {
		if err := client.Close(); err != nil {
			lastErr = err
		}
		delete(p.connections, addr)
	}
	return lastErr
}

// Remove 移除指定连接
func (p *ConnectionPool) Remove(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if client, exists := p.connections[addr]; exists {
		client.Close()
		delete(p.connections, addr)
	}
}
