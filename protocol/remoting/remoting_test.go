package remoting

import (
	"testing"
)

func TestNewRequest(t *testing.T) {
	extFields := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	cmd := NewRequest(GetBrokerClusterInfo, extFields)

	if cmd.Code != GetBrokerClusterInfo {
		t.Errorf("Code 应为 %d, got %d", GetBrokerClusterInfo, cmd.Code)
	}

	if cmd.Language != LanguageGo {
		t.Errorf("Language 应为 %s, got %s", LanguageGo, cmd.Language)
	}

	if cmd.Version != CurrentVersion {
		t.Errorf("Version 应为 %d, got %d", CurrentVersion, cmd.Version)
	}

	if cmd.Opaque <= 0 {
		t.Error("Opaque 应为正数")
	}

	if cmd.ExtFields["key1"] != "value1" {
		t.Errorf("ExtFields[key1] 应为 value1, got %s", cmd.ExtFields["key1"])
	}
}

func TestRemotingCommandEncodeDecode(t *testing.T) {
	original := NewRequest(GetBrokerClusterInfo, map[string]string{
		"topic": "TestTopic",
	})
	original.Body = []byte(`{"test": "data"}`)

	// 编码
	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("编码失败: %v", err)
	}

	if len(encoded) < 8 {
		t.Fatal("编码数据太短")
	}

	// 解码（跳过前 4 字节的总长度）
	decoded, err := Decode(encoded[4:])
	if err != nil {
		t.Fatalf("解码失败: %v", err)
	}

	if decoded.Code != original.Code {
		t.Errorf("解码后 Code 不匹配: got %d, want %d", decoded.Code, original.Code)
	}

	if decoded.Opaque != original.Opaque {
		t.Errorf("解码后 Opaque 不匹配: got %d, want %d", decoded.Opaque, original.Opaque)
	}

	if decoded.ExtFields["topic"] != "TestTopic" {
		t.Errorf("解码后 ExtFields 不匹配: got %s", decoded.ExtFields["topic"])
	}

	if string(decoded.Body) != `{"test": "data"}` {
		t.Errorf("解码后 Body 不匹配: got %s", string(decoded.Body))
	}
}

func TestIsResponseType(t *testing.T) {
	cmd := NewRequest(GetBrokerClusterInfo, nil)

	if cmd.IsResponseType() {
		t.Error("新请求不应该是响应类型")
	}

	cmd.MarkResponseType()

	if !cmd.IsResponseType() {
		t.Error("标记后应该是响应类型")
	}
}

func TestNewOnewayRequest(t *testing.T) {
	cmd := NewOnewayRequest(UpdateBrokerConfig, nil)

	if cmd.Flag != OnewayRPC {
		t.Errorf("Flag 应为 %d, got %d", OnewayRPC, cmd.Flag)
	}
}

func TestConnectionPool(t *testing.T) {
	pool := NewConnectionPool(3000)

	if pool == nil {
		t.Fatal("连接池不应为 nil")
	}

	// 测试关闭空池
	if err := pool.Close(); err != nil {
		t.Errorf("关闭空池失败: %v", err)
	}
}
