package admin

import (
	"regexp"
)

// RocketMQ can answer with JSON that no standard parser accepts:
// 1. unquoted numeric keys: {"brokerAddrs":{0:"192.168.1.1:10911"}}
// 2. unquoted property names: {topic:xxx,brokerName:xxx,queueId:0}
// 3. Fastjson maps whose key is itself a JSON object:
//    "offsetTable":{{"brokerName":"a","queueId":0,"topic":"t"}:{"brokerOffset":1}}
// All three are rewritten into standard JSON before unmarshalling.

// Matches an unquoted numeric key: {0: or ,1:
var unquotedNumKeyRegex = regexp.MustCompile(`([{,])(\d+):`)

// Matches an unquoted identifier key: {topic: or ,brokerName:
var unquotedStrKeyRegex = regexp.MustCompile(`([{,])([a-zA-Z_][a-zA-Z0-9_]*):`)

// fixJSONBody rewrites RocketMQ's non-standard JSON: unquoted keys get
// quoted, and object-keyed maps become string-keyed ones.
func fixJSONBody(body []byte) []byte {
	result := unquotedNumKeyRegex.ReplaceAll(body, []byte(`$1"$2":`))

	result = unquotedStrKeyRegex.ReplaceAll(result, []byte(`$1"$2":`))

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
