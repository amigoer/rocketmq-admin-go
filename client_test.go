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
		t.Fatalf("创建客户端失败: %v", err)
	}
	defer client.Close()

	if client == nil {
		t.Fatal("客户端不应为 nil")
	}
}

func TestNewClient_NoNameServer(t *testing.T) {
	_, err := NewClient()
	if err == nil {
		t.Fatal("缺少 NameServer 应该返回错误")
	}
}

func TestNewClient_WithACL(t *testing.T) {
	client, err := NewClient(
		WithNameServers([]string{"localhost:9876"}),
		WithACL("testAccessKey", "testSecretKey"),
	)
	if err != nil {
		t.Fatalf("创建带 ACL 的客户端失败: %v", err)
	}
	defer client.Close()

	if client == nil {
		t.Fatal("客户端不应为 nil")
	}
}

func TestClient_StartAndClose(t *testing.T) {
	client, err := NewClient(
		WithNameServers([]string{"localhost:9876"}),
	)
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}

	if client.IsStarted() {
		t.Error("新创建的客户端不应处于已启动状态")
	}
	if client.IsClosed() {
		t.Error("新创建的客户端不应处于已关闭状态")
	}

	if err := client.Start(); err != nil {
		t.Fatalf("启动客户端失败: %v", err)
	}
	if !client.IsStarted() {
		t.Error("启动后客户端应处于已启动状态")
	}

	if err := client.Start(); err != ErrAlreadyStarted {
		t.Errorf("重复启动应返回 ErrAlreadyStarted, got: %v", err)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("关闭客户端失败: %v", err)
	}
	if !client.IsClosed() {
		t.Error("关闭后客户端应处于已关闭状态")
	}

	if err := client.Close(); err != nil {
		t.Errorf("重复关闭不应返回错误, got: %v", err)
	}
}

func TestClient_StartAfterClose(t *testing.T) {
	client, err := NewClient(
		WithNameServers([]string{"localhost:9876"}),
	)
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("关闭客户端失败: %v", err)
	}

	if err := client.Start(); err != ErrClientClosed {
		t.Errorf("启动已关闭的客户端应返回 ErrClientClosed, got: %v", err)
	}
}

func TestClient_GetNameServerAddressList(t *testing.T) {
	expectedAddrs := []string{"localhost:9876", "localhost:9877"}
	client, err := NewClient(
		WithNameServers(expectedAddrs),
	)
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	defer client.Close()

	addrs := client.GetNameServerAddressList()
	if len(addrs) != len(expectedAddrs) {
		t.Errorf("地址列表长度不匹配: got %d, want %d", len(addrs), len(expectedAddrs))
	}

	for i, addr := range addrs {
		if addr != expectedAddrs[i] {
			t.Errorf("地址不匹配: got %s, want %s", addr, expectedAddrs[i])
		}
	}
}
