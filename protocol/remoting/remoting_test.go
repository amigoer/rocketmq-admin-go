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

// A Broker relaying a Java client's answer sends a binary header, not JSON.
//
// These bytes are a real GET_CONSUMER_RUNNING_INFO response off RocketMQ 5.3.2:
// success, version 477, and the remark the client never cleared. Decoding it as
// JSON drops the frame, and the caller then waits out its whole timeout.
func TestDecodeBinaryHeader(t *testing.T) {
	header := []byte{
		0x00, 0x00, // code 0
		0x00,       // language JAVA
		0x01, 0xdd, // version 477
		0x00, 0x00, 0x00, 0x07, // opaque 7
		0x00, 0x00, 0x00, 0x01, // flag: response
		0x00, 0x00, 0x00, 0x19, // remark length
	}
	header = append(header, []byte("not set any response code")...)
	header = append(header, 0x00, 0x00, 0x00, 0x00) // no extFields

	frame := []byte{RocketMQSerializeType, 0x00, 0x00, byte(len(header))}
	frame = append(frame, header...)
	frame = append(frame, []byte(`{"mqTable":{}}`)...)

	decoded, err := Decode(frame)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.Code != Success {
		t.Errorf("Code = %d, want %d", decoded.Code, Success)
	}
	if decoded.Language != "JAVA" {
		t.Errorf("Language = %q, want JAVA", decoded.Language)
	}
	if decoded.Version != 477 {
		t.Errorf("Version = %d, want 477", decoded.Version)
	}
	if decoded.Opaque != 7 {
		t.Errorf("Opaque = %d, want 7", decoded.Opaque)
	}
	if !decoded.IsResponseType() {
		t.Error("the frame should decode as a response")
	}
	if decoded.Remark != "not set any response code" {
		t.Errorf("Remark = %q", decoded.Remark)
	}
	if string(decoded.Body) != `{"mqTable":{}}` {
		t.Errorf("Body = %q", decoded.Body)
	}
}

// extFields in a binary header is keyLen int16 + key + valueLen int32 + value.
func TestDecodeBinaryHeaderExtFields(t *testing.T) {
	extFields := []byte{0x00, 0x05}
	extFields = append(extFields, []byte("topic")...)
	extFields = append(extFields, 0x00, 0x00, 0x00, 0x02)
	extFields = append(extFields, []byte("T1")...)

	header := []byte{
		0x00, 0x00,
		0x09, // language GO
		0x01, 0x3d,
		0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00, // no remark
		0x00, 0x00, 0x00, byte(len(extFields)),
	}
	header = append(header, extFields...)

	frame := []byte{RocketMQSerializeType, 0x00, 0x00, byte(len(header))}
	frame = append(frame, header...)

	decoded, err := Decode(frame)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.Language != LanguageGo {
		t.Errorf("Language = %q, want %s", decoded.Language, LanguageGo)
	}
	if decoded.Remark != "" {
		t.Errorf("Remark = %q, want empty", decoded.Remark)
	}
	if decoded.ExtFields["topic"] != "T1" {
		t.Errorf("ExtFields = %v", decoded.ExtFields)
	}
}

// A truncated binary header must be reported, not read past.
func TestDecodeBinaryHeaderTruncated(t *testing.T) {
	frame := []byte{RocketMQSerializeType, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 0}
	if _, err := Decode(frame); err != ErrInvalidData {
		t.Fatalf("err = %v, want %v", err, ErrInvalidData)
	}
}

func TestDecodeUnknownSerializeType(t *testing.T) {
	frame := []byte{0x7f, 0x00, 0x00, 0x02, 0x00, 0x00}
	if _, err := Decode(frame); err != ErrUnknownSerializeType {
		t.Fatalf("err = %v, want %v", err, ErrUnknownSerializeType)
	}
}
