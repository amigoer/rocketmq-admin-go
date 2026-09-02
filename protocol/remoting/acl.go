package remoting

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"sort"
)

// ACL 1.0 header fields. The Broker reads the caller's identity from
// AccessKey and recomputes Signature over everything else.
const (
	AccessKeyField = "AccessKey"
	SignatureField = "Signature"
)

// Sign stamps cmd with an ACL 1.0 signature, and does nothing without an
// access key so an unauthenticated cluster is unaffected.
//
// The signed content is every header field except Signature, sorted by field
// name, values concatenated with no separator, and the body appended. That is
// what PlainAccessValidator rebuilds on the other side, from the extFields it
// received - so a field the Broker sees and the client did not sign breaks the
// comparison. Sign last, once bname and every other field is final, and sign
// again before each retry: the same command sent to a second Broker carries a
// different bname.
func Sign(cmd *RemotingCommand, accessKey, secretKey string) {
	if cmd == nil || accessKey == "" {
		return
	}

	if cmd.ExtFields == nil {
		cmd.ExtFields = make(map[string]string)
	}
	cmd.ExtFields[AccessKeyField] = accessKey

	// Dropped rather than skipped: a re-signed command would otherwise cover
	// the previous signature, which the Broker excludes and never sees.
	delete(cmd.ExtFields, SignatureField)

	names := make([]string, 0, len(cmd.ExtFields))
	for name := range cmd.ExtFields {
		names = append(names, name)
	}
	sort.Strings(names)

	content := make([]byte, 0, len(cmd.Body)+64)
	for _, name := range names {
		content = append(content, cmd.ExtFields[name]...)
	}
	content = append(content, cmd.Body...)

	// HMAC-SHA1 is what RocketMQ's AclSigner uses; the algorithm is not
	// negotiable and not a security choice made here.
	mac := hmac.New(sha1.New, []byte(secretKey))
	mac.Write(content)
	cmd.ExtFields[SignatureField] = base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
