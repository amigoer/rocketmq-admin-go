package admin

import (
	"regexp"
)

// =============================================================================
// JSON 响应预处理
// =============================================================================

// RocketMQ 返回的响应中可能包含非标准 JSON：
// 1. 数字 key 没有引号: {"brokerAddrs":{0:"192.168.1.1:10911"}}
// 2. 字符串属性名没有引号: {topic:xxx,brokerName:xxx,queueId:0}
// 需要转换为标准 JSON 格式

// 匹配非标准 JSON 数字 key 的正则表达式
// 匹配模式: {数字: 或 ,数字: （key 没有引号）
var unquotedNumKeyRegex = regexp.MustCompile(`([{,])(\d+):`)

// 匹配非标准 JSON 字符串 key 的正则表达式
// 匹配模式: {key: 或 ,key: （key 没有引号，key 是字母开头的标识符）
var unquotedStrKeyRegex = regexp.MustCompile(`([{,])([a-zA-Z_][a-zA-Z0-9_]*):`)

// fixJSONBody 修复 RocketMQ 返回的非标准 JSON
// 将没有引号的 key 转换为带引号的字符串 key
func fixJSONBody(body []byte) []byte {
	// 1. 替换数字 key：{0: -> {"0": 或 ,1: -> ,"1":
	result := unquotedNumKeyRegex.ReplaceAll(body, []byte(`$1"$2":`))

	// 2. 替换字符串 key：{topic: -> {"topic": 或 ,brokerName: -> ,"brokerName":
	result = unquotedStrKeyRegex.ReplaceAll(result, []byte(`$1"$2":`))

	return result
}
