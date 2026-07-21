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
// 3. Map 的 key 是 JSON 对象（Fastjson）:
//    "offsetTable":{{"brokerName":"a","queueId":0,"topic":"t"}:{"brokerOffset":1}}
// 需要转换为标准 JSON 格式

// 匹配非标准 JSON 数字 key 的正则表达式
// 匹配模式: {数字: 或 ,数字: （key 没有引号）
var unquotedNumKeyRegex = regexp.MustCompile(`([{,])(\d+):`)

// 匹配非标准 JSON 字符串 key 的正则表达式
// 匹配模式: {key: 或 ,key: （key 没有引号，key 是字母开头的标识符）
var unquotedStrKeyRegex = regexp.MustCompile(`([{,])([a-zA-Z_][a-zA-Z0-9_]*):`)

// fixJSONBody 修复 RocketMQ 返回的非标准 JSON
// 将没有引号的 key 转换为带引号的字符串 key，并处理 object-key map。
func fixJSONBody(body []byte) []byte {
	// 1. 替换数字 key：{0: -> {"0": 或 ,1: -> ,"1":
	result := unquotedNumKeyRegex.ReplaceAll(body, []byte(`$1"$2":`))

	// 2. 替换字符串 key：{topic: -> {"topic": 或 ,brokerName: -> ,"brokerName":
	result = unquotedStrKeyRegex.ReplaceAll(result, []byte(`$1"$2":`))

	// 3. Fastjson 将复杂对象作为 map key 时会产出 {{...}:{...}}，需转成标准 JSON。
	result = fixObjectKeyedMaps(result)

	return result
}

// fixObjectKeyedMaps converts Fastjson maps whose keys are JSON objects:
//
//	{{"a":1}:{"b":2},{"a":3}:{"b":4}}
//
// into:
//
//	{"{\"a\":1}":{"b":2},"{\"a\":3}":{"b":4}}
func fixObjectKeyedMaps(body []byte) []byte {
	out := make([]byte, 0, len(body)+64)
	i := 0
	for i < len(body) {
		if i+2 < len(body) && body[i] == ':' && body[i+1] == '{' && body[i+2] == '{' {
			out = append(out, ':')
			converted, next := convertObjectKeyedMap(body, i+1)
			out = append(out, converted...)
			i = next
			continue
		}
		out = append(out, body[i])
		i++
	}
	return out
}

// convertObjectKeyedMap reads body[start:] which must begin with '{', and returns
// a standard JSON object plus the index just past the original map.
func convertObjectKeyedMap(body []byte, start int) ([]byte, int) {
	if start >= len(body) || body[start] != '{' {
		return []byte{'{', '}'}, start
	}

	out := []byte{'{'}
	i := start + 1
	first := true

	for i < len(body) {
		for i < len(body) && isJSONSpace(body[i]) {
			i++
		}
		if i >= len(body) {
			break
		}
		if body[i] == '}' {
			out = append(out, '}')
			return out, i + 1
		}
		if body[i] == ',' {
			i++
			continue
		}
		if body[i] != '{' {
			// Not an object-keyed map; return the original slice unchanged.
			end := findMatchingBrace(body, start)
			if end < 0 {
				return body[start:], len(body)
			}
			return body[start : end+1], end + 1
		}

		keyEnd := findMatchingBrace(body, i)
		if keyEnd < 0 {
			return body[start:], len(body)
		}
		key := body[i : keyEnd+1]
		i = keyEnd + 1
		for i < len(body) && isJSONSpace(body[i]) {
			i++
		}
		if i >= len(body) || body[i] != ':' {
			end := findMatchingBrace(body, start)
			if end < 0 {
				return body[start:], len(body)
			}
			return body[start : end+1], end + 1
		}
		i++ // skip ':'
		for i < len(body) && isJSONSpace(body[i]) {
			i++
		}
		if i >= len(body) || body[i] != '{' {
			end := findMatchingBrace(body, start)
			if end < 0 {
				return body[start:], len(body)
			}
			return body[start : end+1], end + 1
		}
		valEnd := findMatchingBrace(body, i)
		if valEnd < 0 {
			return body[start:], len(body)
		}
		val := body[i : valEnd+1]
		i = valEnd + 1

		if !first {
			out = append(out, ',')
		}
		first = false
		out = append(out, '"')
		out = append(out, escapeJSONString(key)...)
		out = append(out, '"', ':')
		out = append(out, val...)
	}

	out = append(out, '}')
	return out, len(body)
}

func findMatchingBrace(body []byte, start int) int {
	if start >= len(body) || body[start] != '{' {
		return -1
	}
	depth := 0
	inString := false
	escape := false
	for i := start; i < len(body); i++ {
		ch := body[i]
		if inString {
			if escape {
				escape = false
				continue
			}
			if ch == '\\' {
				escape = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func escapeJSONString(raw []byte) []byte {
	out := make([]byte, 0, len(raw)+8)
	for _, ch := range raw {
		switch ch {
		case '\\', '"':
			out = append(out, '\\', ch)
		case '\n':
			out = append(out, '\\', 'n')
		case '\r':
			out = append(out, '\\', 'r')
		case '\t':
			out = append(out, '\\', 't')
		default:
			out = append(out, ch)
		}
	}
	return out
}

func isJSONSpace(ch byte) bool {
	return ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t'
}
