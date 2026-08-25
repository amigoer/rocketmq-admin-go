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

	// Body serialization
	JSONSerializeType = 0

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
	headerLen := int(binary.BigEndian.Uint32(data[0:4]) & 0x00FFFFFF)

	if len(data) < 4+headerLen {
		return nil, ErrInvalidData
	}

	cmd := &RemotingCommand{}
	if err := json.Unmarshal(data[4:4+headerLen], cmd); err != nil {
		return nil, err
	}

	if len(data) > 4+headerLen {
		cmd.Body = data[4+headerLen:]
	}

	return cmd, nil
}

var (
	ErrInvalidData = &RemotingError{Message: "invalid data"}
)

// RemotingError is a protocol-level failure.
type RemotingError struct {
	Message string
}

func (e *RemotingError) Error() string {
	return e.Message
}
