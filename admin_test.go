package admin

import (
	"testing"
)

func TestNewClient(t *testing.T) {
	tests := []struct {
		name    string
		opts    []Option
		wantErr bool
	}{
		{
			name:    "无 NameServer 配置应报错",
			opts:    []Option{},
			wantErr: true,
		},
		{
			name: "正常创建客户端",
			opts: []Option{
				WithNameServers([]string{"127.0.0.1:9876"}),
			},
			wantErr: false,
		},
		{
			name: "多个 NameServer",
			opts: []Option{
				WithNameServers([]string{"127.0.0.1:9876", "127.0.0.2:9876"}),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClient(tt.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && client == nil {
				t.Error("NewClient() returned nil client")
			}
		})
	}
}

func TestClientLifecycle(t *testing.T) {
	client, err := NewClient(
		WithNameServers([]string{"127.0.0.1:9876"}),
	)
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}

	// 测试启动前状态
	if client.IsStarted() {
		t.Error("客户端应该未启动")
	}
	if client.IsClosed() {
		t.Error("客户端不应该已关闭")
	}

	// 测试启动
	if err := client.Start(); err != nil {
		t.Errorf("启动客户端失败: %v", err)
	}

	if !client.IsStarted() {
		t.Error("客户端应该已启动")
	}

	// 测试重复启动
	if err := client.Start(); err != ErrAlreadyStarted {
		t.Errorf("重复启动应返回 ErrAlreadyStarted, got %v", err)
	}

	// 测试关闭
	if err := client.Close(); err != nil {
		t.Errorf("关闭客户端失败: %v", err)
	}

	if !client.IsClosed() {
		t.Error("客户端应该已关闭")
	}

	// 测试关闭后启动（since started is still true after close, it returns ErrAlreadyStarted first）
	// 需要确保客户端关闭后无法再次启动
	err = client.Start()
	if err == nil {
		t.Error("关闭后启动应返回错误")
	}
}

func TestGetNameServerAddressList(t *testing.T) {
	nameServers := []string{"127.0.0.1:9876", "127.0.0.2:9876"}
	client, err := NewClient(
		WithNameServers(nameServers),
	)
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}

	got := client.GetNameServerAddressList()
	if len(got) != len(nameServers) {
		t.Errorf("GetNameServerAddressList() len = %v, want %v", len(got), len(nameServers))
	}

	for i, addr := range got {
		if addr != nameServers[i] {
			t.Errorf("GetNameServerAddressList()[%d] = %v, want %v", i, addr, nameServers[i])
		}
	}
}

func TestAdminError(t *testing.T) {
	err := NewAdminError(100, "测试错误")

	if err.Code != 100 {
		t.Errorf("错误码应为 100, got %d", err.Code)
	}

	if err.Message != "测试错误" {
		t.Errorf("错误消息应为 '测试错误', got %s", err.Message)
	}

	if err.Error() != "测试错误" {
		t.Errorf("Error() 应返回 '测试错误', got %s", err.Error())
	}
}
