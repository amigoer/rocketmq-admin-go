package remoting

import (
	"fmt"
	"sync"
	"time"
)

// ConnectionPool caches one Client per server address.
type ConnectionPool struct {
	mu          sync.RWMutex
	connections map[string]*Client // key: addr
	timeout     time.Duration
}

// NewConnectionPool creates an empty pool using timeout for dials and requests.
func NewConnectionPool(timeout time.Duration) *ConnectionPool {
	return &ConnectionPool{
		connections: make(map[string]*Client),
		timeout:     timeout,
	}
}

// GetOrCreate returns the pooled connection for addr, dialing if there is none.
func (p *ConnectionPool) GetOrCreate(addr string) (*Client, error) {
	p.mu.RLock()
	client, exists := p.connections[addr]
	p.mu.RUnlock()

	if exists && client.IsConnected() {
		return client, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Re-check: another goroutine may have connected while we waited for the lock.
	if client, exists = p.connections[addr]; exists && client.IsConnected() {
		return client, nil
	}

	client = NewClient(addr, p.timeout)
	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	p.connections[addr] = client
	return client, nil
}

// Close closes every pooled connection and empties the pool.
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

// Remove closes the connection for addr and drops it from the pool.
func (p *ConnectionPool) Remove(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if client, exists := p.connections[addr]; exists {
		client.Close()
		delete(p.connections, addr)
	}
}
