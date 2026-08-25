package admin

import (
	"testing"
	"time"
)

func TestNewClient_Success(t *testing.T) {
	client, err := NewClient(
		WithNameServers([]string{"localhost:9876"}),
		WithTimeout(5*time.Second),
		WithRetryTimes(3),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	if client == nil {
		t.Fatal("client must not be nil")
	}
}

func TestNewClient_NoNameServer(t *testing.T) {
	_, err := NewClient()
	if err == nil {
		t.Fatal("a missing NameServer should return an error")
	}
}

func TestNewClient_WithACL(t *testing.T) {
	client, err := NewClient(
		WithNameServers([]string{"localhost:9876"}),
		WithACL("testAccessKey", "testSecretKey"),
	)
	if err != nil {
		t.Fatalf("failed to create a client with ACL: %v", err)
	}
	defer client.Close()

	if client == nil {
		t.Fatal("client must not be nil")
	}
}

func TestClient_StartAndClose(t *testing.T) {
	client, err := NewClient(
		WithNameServers([]string{"localhost:9876"}),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	if client.IsStarted() {
		t.Error("a new client must not report itself started")
	}
	if client.IsClosed() {
		t.Error("a new client must not report itself closed")
	}

	if err := client.Start(); err != nil {
		t.Fatalf("failed to start client: %v", err)
	}
	if !client.IsStarted() {
		t.Error("the client should report itself started after Start")
	}

	if err := client.Start(); err != ErrAlreadyStarted {
		t.Errorf("a second Start should return ErrAlreadyStarted, got: %v", err)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("failed to close client: %v", err)
	}
	if !client.IsClosed() {
		t.Error("the client should report itself closed after Close")
	}

	if err := client.Close(); err != nil {
		t.Errorf("a second Close should not return an error, got: %v", err)
	}
}

func TestClient_StartAfterClose(t *testing.T) {
	client, err := NewClient(
		WithNameServers([]string{"localhost:9876"}),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("failed to close client: %v", err)
	}

	if err := client.Start(); err != ErrClientClosed {
		t.Errorf("starting a closed client should return ErrClientClosed, got: %v", err)
	}
}

func TestClient_GetNameServerAddressList(t *testing.T) {
	expectedAddrs := []string{"localhost:9876", "localhost:9877"}
	client, err := NewClient(
		WithNameServers(expectedAddrs),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	addrs := client.GetNameServerAddressList()
	if len(addrs) != len(expectedAddrs) {
		t.Errorf("address list length mismatch: got %d, want %d", len(addrs), len(expectedAddrs))
	}

	for i, addr := range addrs {
		if addr != expectedAddrs[i] {
			t.Errorf("address mismatch: got %s, want %s", addr, expectedAddrs[i])
		}
	}
}
