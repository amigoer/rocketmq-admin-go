// Package remoting 实现 RocketMQ Remoting 通信协议
package remoting

import (
	"encoding/binary"
	"encoding/json"
	"sync/atomic"
)

// 协议常量
const (
	// 请求类型
	RPCType   = 0 // RPC 请求
	OnewayRPC = 1 // 单向请求

	// 序列化类型
	JSONSerializeType = 0 // JSON 序列化

	// 语言标识
	LanguageGo = "GO"

	// 协议版本
	CurrentVersion = 317
)

// 全局请求 ID 计数器
var requestID int32

// RemotingCommand 远程命令
type RemotingCommand struct {
	// Code 请求/响应码
	Code int `json:"code"`

	// Language 语言
	Language string `json:"language"`

	// Version 版本号
	Version int `json:"version"`

	// Opaque 请求 ID
	Opaque int32 `json:"opaque"`

	// Flag 标志位
	Flag int `json:"flag"`

	// Remark 备注
	Remark string `json:"remark"`

	// ExtFields 扩展字段（请求头）
	ExtFields map[string]string `json:"extFields"`

	// Body 消息体
	Body []byte `json:"-"`
}

// NewRequest 创建请求命令
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

// NewOnewayRequest 创建单向请求命令
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

// IsResponseType 是否为响应类型
func (cmd *RemotingCommand) IsResponseType() bool {
	return cmd.Flag&0x01 == 1
}

// MarkResponseType 标记为响应类型
func (cmd *RemotingCommand) MarkResponseType() {
	cmd.Flag = cmd.Flag | 0x01
}

// MarkOnewayRPC 标记为单向 RPC
func (cmd *RemotingCommand) MarkOnewayRPC() {
	cmd.Flag = cmd.Flag | 0x02
}

// Encode 编码命令为字节数组
func (cmd *RemotingCommand) Encode() ([]byte, error) {
	// 编码 header
	headerBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	headerLen := len(headerBytes)
	bodyLen := len(cmd.Body)
	totalLen := 4 + headerLen + bodyLen

	// 分配缓冲区
	buf := make([]byte, 4+totalLen)

	// 写入总长度
	binary.BigEndian.PutUint32(buf[0:4], uint32(totalLen))

	// 写入 header 长度和序列化类型
	binary.BigEndian.PutUint32(buf[4:8], uint32(headerLen)|(uint32(JSONSerializeType)<<24))

	// 写入 header
	copy(buf[8:8+headerLen], headerBytes)

	// 写入 body
	if bodyLen > 0 {
		copy(buf[8+headerLen:], cmd.Body)
	}

	return buf, nil
}

// Decode 从字节数组解码命令
func Decode(data []byte) (*RemotingCommand, error) {
	if len(data) < 4 {
		return nil, ErrInvalidData
	}

	// 读取 header 长度
	headerLen := int(binary.BigEndian.Uint32(data[0:4]) & 0x00FFFFFF)

	if len(data) < 4+headerLen {
		return nil, ErrInvalidData
	}

	// 解析 header
	cmd := &RemotingCommand{}
	if err := json.Unmarshal(data[4:4+headerLen], cmd); err != nil {
		return nil, err
	}

	// 读取 body
	if len(data) > 4+headerLen {
		cmd.Body = data[4+headerLen:]
	}

	return cmd, nil
}

// 错误定义
var (
	ErrInvalidData = &RemotingError{Message: "无效数据"}
)

// RemotingError 远程通信错误
type RemotingError struct {
	Message string
}

func (e *RemotingError) Error() string {
	return e.Message
}

