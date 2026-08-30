// Package remoting implements the RocketMQ remoting protocol.
package remoting

import (
	"encoding/binary"
	"encoding/json"
	"sync/atomic"
)

const (
	// Request flags
	RPCType   = 0
	OnewayRPC = 1

	// Header serialization
	JSONSerializeType     = 0
	RocketMQSerializeType = 1

	LanguageGo     = "GO"
	CurrentVersion = 317
)

// requestID is the process-wide counter behind RemotingCommand.Opaque.
var requestID int32

// RemotingCommand is a single protocol frame, request or response.
type RemotingCommand struct {
	Code      int               `json:"code"`
	Language  string            `json:"language"`
	Version   int               `json:"version"`
	Opaque    int32             `json:"opaque"` // request id, echoed in the response
	Flag      int               `json:"flag"`   // RPCType or OnewayRPC, plus the response bit
	Remark    string            `json:"remark"`
	ExtFields map[string]string `json:"extFields"` // request header fields

	// Body is sent after the header, so it is never part of the header JSON.
	Body []byte `json:"-"`
}

// NewRequest creates an RPC request carrying the given code and header fields.
func NewRequest(code int, extFields map[string]string) *RemotingCommand {
	return &RemotingCommand{
		Code:      code,
		Language:  LanguageGo,
		Version:   CurrentVersion,
		Opaque:    atomic.AddInt32(&requestID, 1),
		Flag:      RPCType,
		ExtFields: extFields,
	}
}

// NewOnewayRequest creates a request that expects no response.
func NewOnewayRequest(code int, extFields map[string]string) *RemotingCommand {
	return &RemotingCommand{
		Code:      code,
		Language:  LanguageGo,
		Version:   CurrentVersion,
		Opaque:    atomic.AddInt32(&requestID, 1),
		Flag:      OnewayRPC,
		ExtFields: extFields,
	}
}

// IsResponseType reports whether the frame is a response.
func (cmd *RemotingCommand) IsResponseType() bool {
	return cmd.Flag&0x01 == 1
}

// MarkResponseType marks the frame as a response.
func (cmd *RemotingCommand) MarkResponseType() {
	cmd.Flag = cmd.Flag | 0x01
}

// MarkOnewayRPC marks the frame as one-way.
func (cmd *RemotingCommand) MarkOnewayRPC() {
	cmd.Flag = cmd.Flag | 0x02
}

// Encode serialises the command into a length-prefixed frame.
func (cmd *RemotingCommand) Encode() ([]byte, error) {
	headerBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	headerLen := len(headerBytes)
	bodyLen := len(cmd.Body)
	totalLen := 4 + headerLen + bodyLen

	buf := make([]byte, 4+totalLen)

	binary.BigEndian.PutUint32(buf[0:4], uint32(totalLen))

	// Header length and serialize type share one 32-bit word.
	binary.BigEndian.PutUint32(buf[4:8], uint32(headerLen)|(uint32(JSONSerializeType)<<24))

	copy(buf[8:8+headerLen], headerBytes)

	if bodyLen > 0 {
		copy(buf[8+headerLen:], cmd.Body)
	}

	return buf, nil
}

// Decode parses a frame whose leading total-length word has already been read.
func Decode(data []byte) (*RemotingCommand, error) {
	if len(data) < 4 {
		return nil, ErrInvalidData
	}

	// The top byte holds the serialize type; the rest is the header length.
	word := binary.BigEndian.Uint32(data[0:4])
	headerLen := int(word & 0x00FFFFFF)

	if len(data) < 4+headerLen {
		return nil, ErrInvalidData
	}

	var (
		cmd *RemotingCommand
		err error
	)
	switch byte(word >> 24) {
	case JSONSerializeType:
		cmd = &RemotingCommand{}
		err = json.Unmarshal(data[4:4+headerLen], cmd)
	case RocketMQSerializeType:
		cmd, err = decodeBinaryHeader(data[4 : 4+headerLen])
	default:
		return nil, ErrUnknownSerializeType
	}
	if err != nil {
		return nil, err
	}

	if len(data) > 4+headerLen {
		cmd.Body = data[4+headerLen:]
	}

	return cmd, nil
}

// decodeBinaryHeader parses the ROCKETMQ header serialization:
//
//	code int16 | language int8 | version int16 | opaque int32 | flag int32 |
//	remark int32-prefixed | extFields int32-prefixed
//
// Requests always go out as JSON, but a response the Broker relays from a
// client comes back in whatever that client encoded - a Java consumer answers
// GET_CONSUMER_RUNNING_INFO in binary - and a header this decoder cannot read
// is a frame nobody is ever handed, so the caller waits for its context.
func decodeBinaryHeader(header []byte) (*RemotingCommand, error) {
	// Up to and including both length words, with nothing after them.
	const minLen = 2 + 1 + 2 + 4 + 4 + 4 + 4
	if len(header) < minLen {
		return nil, ErrInvalidData
	}

	cmd := &RemotingCommand{
		Code:     int(int16(binary.BigEndian.Uint16(header[0:2]))),
		Language: languageOf(header[2]),
		Version:  int(int16(binary.BigEndian.Uint16(header[3:5]))),
		Opaque:   int32(binary.BigEndian.Uint32(header[5:9])),
		Flag:     int(int32(binary.BigEndian.Uint32(header[9:13]))),
	}

	remark, next, err := readInt32Prefixed(header, 13)
	if err != nil {
		return nil, err
	}
	cmd.Remark = string(remark)

	extFields, _, err := readInt32Prefixed(header, next)
	if err != nil {
		return nil, err
	}
	if len(extFields) > 0 {
		cmd.ExtFields, err = decodeBinaryExtFields(extFields)
		if err != nil {
			return nil, err
		}
	}

	return cmd, nil
}

// decodeBinaryExtFields reads the keyLen int16 + key + valueLen int32 + value
// runs that carry a binary header's extFields map.
func decodeBinaryExtFields(data []byte) (map[string]string, error) {
	fields := make(map[string]string)
	for offset := 0; offset < len(data); {
		if offset+2 > len(data) {
			return nil, ErrInvalidData
		}
		keyLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if offset+keyLen > len(data) {
			return nil, ErrInvalidData
		}
		key := string(data[offset : offset+keyLen])
		offset += keyLen

		value, next, err := readInt32Prefixed(data, offset)
		if err != nil {
			return nil, err
		}
		fields[key] = string(value)
		offset = next
	}
	return fields, nil
}

// readInt32Prefixed returns the block at offset and the index just past it.
func readInt32Prefixed(data []byte, offset int) ([]byte, int, error) {
	if offset+4 > len(data) {
		return nil, 0, ErrInvalidData
	}
	length := int(int32(binary.BigEndian.Uint32(data[offset : offset+4])))
	offset += 4
	if length < 0 || offset+length > len(data) {
		return nil, 0, ErrInvalidData
	}
	return data[offset : offset+length], offset + length, nil
}

// languageNames is RocketMQ's LanguageCode, indexed by its wire byte.
var languageNames = [...]string{
	"JAVA", "CPP", "DOTNET", "PYTHON", "DELPHI", "ERLANG",
	"RUBY", "OTHER", "HTTP", "GO", "PHP", "OMS", "RUST",
}

func languageOf(code byte) string {
	if int(code) < len(languageNames) {
		return languageNames[code]
	}
	return "OTHER"
}

var (
	ErrInvalidData          = &RemotingError{Message: "invalid data"}
	ErrUnknownSerializeType = &RemotingError{Message: "unknown serialize type"}
)

// RemotingError is a protocol-level failure.
type RemotingError struct {
	Message string
}

func (e *RemotingError) Error() string {
	return e.Message
}
