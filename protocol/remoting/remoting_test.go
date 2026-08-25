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
		t.Errorf("Code should be %d, got %d", GetBrokerClusterInfo, cmd.Code)
	}

	if cmd.Language != LanguageGo {
		t.Errorf("Language should be %s, got %s", LanguageGo, cmd.Language)
	}

	if cmd.Version != CurrentVersion {
		t.Errorf("Version should be %d, got %d", CurrentVersion, cmd.Version)
	}

	if cmd.Opaque <= 0 {
		t.Error("Opaque should be positive")
	}

	if cmd.ExtFields["key1"] != "value1" {
		t.Errorf("ExtFields[key1] should be value1, got %s", cmd.ExtFields["key1"])
	}
}

func TestRemotingCommandEncodeDecode(t *testing.T) {
	original := NewRequest(GetBrokerClusterInfo, map[string]string{
		"topic": "TestTopic",
	})
	original.Body = []byte(`{"test": "data"}`)

	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	if len(encoded) < 8 {
		t.Fatal("encoded data is too short")
	}

	// Decode expects the leading 4-byte total length to be stripped.
	decoded, err := Decode(encoded[4:])
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Code != original.Code {
		t.Errorf("Code mismatch after decode: got %d, want %d", decoded.Code, original.Code)
	}

	if decoded.Opaque != original.Opaque {
		t.Errorf("Opaque mismatch after decode: got %d, want %d", decoded.Opaque, original.Opaque)
	}

	if decoded.ExtFields["topic"] != "TestTopic" {
		t.Errorf("ExtFields mismatch after decode: got %s", decoded.ExtFields["topic"])
	}

	if string(decoded.Body) != `{"test": "data"}` {
		t.Errorf("Body mismatch after decode: got %s", string(decoded.Body))
	}
}

func TestIsResponseType(t *testing.T) {
	cmd := NewRequest(GetBrokerClusterInfo, nil)

	if cmd.IsResponseType() {
		t.Error("a new request must not be a response")
	}

	cmd.MarkResponseType()

	if !cmd.IsResponseType() {
		t.Error("it should be a response after marking")
	}
}

func TestNewOnewayRequest(t *testing.T) {
	cmd := NewOnewayRequest(UpdateBrokerConfig, nil)

	if cmd.Flag != OnewayRPC {
		t.Errorf("Flag should be %d, got %d", OnewayRPC, cmd.Flag)
	}
}

func TestConnectionPool(t *testing.T) {
	pool := NewConnectionPool(3000)

	if pool == nil {
		t.Fatal("the connection pool must not be nil")
	}

	if err := pool.Close(); err != nil {
		t.Errorf("failed to close an empty pool: %v", err)
	}
}
