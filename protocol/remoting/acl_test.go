package remoting

import (
	"testing"
)

// The vector is derived from rocketmq-client-go's own ACLInterceptor rather
// than from this implementation, so a rewrite that changes the field order,
// drops the body or picks another digest fails here.
const (
	vectorAccessKey = "mqstudio"
	vectorSecretKey = "mqstudio-secret"
	vectorSignature = "6TDbyCwWGlzqe46La9s9mEsyl1U=" // over "mqstudiobroker-aTestTopic" + body
)

func vectorCommand() *RemotingCommand {
	cmd := NewRequest(GetBrokerClusterInfo, map[string]string{
		"topic": "TestTopic",
		"bname": "broker-a",
	})
	cmd.Body = []byte(`{"test":"data"}`)
	return cmd
}

func TestSign(t *testing.T) {
	cmd := vectorCommand()
	Sign(cmd, vectorAccessKey, vectorSecretKey)

	if got := cmd.ExtFields[AccessKeyField]; got != vectorAccessKey {
		t.Errorf("AccessKey should be %s, got %s", vectorAccessKey, got)
	}
	if got := cmd.ExtFields[SignatureField]; got != vectorSignature {
		t.Errorf("Signature should be %s, got %s", vectorSignature, got)
	}
}

func TestSignWithoutCredentials(t *testing.T) {
	cmd := vectorCommand()
	Sign(cmd, "", vectorSecretKey)

	if _, exists := cmd.ExtFields[SignatureField]; exists {
		t.Error("an unauthenticated cluster should get no Signature field")
	}
	if _, exists := cmd.ExtFields[AccessKeyField]; exists {
		t.Error("an unauthenticated cluster should get no AccessKey field")
	}
}

// Several requests carry no header fields at all, and the credentials still
// have to go somewhere.
func TestSignCreatesTheFieldMap(t *testing.T) {
	cmd := NewRequest(GetBrokerClusterInfo, nil)
	Sign(cmd, vectorAccessKey, vectorSecretKey)

	if cmd.ExtFields[SignatureField] == "" {
		t.Fatal("a command with no header fields was not signed")
	}
}

// invokeBroker re-signs for every address it walks, so signing twice has to
// land on the same answer - the second pass must not cover the first
// signature, which the Broker strips before it recomputes anything.
func TestSignIsRepeatable(t *testing.T) {
	cmd := vectorCommand()
	Sign(cmd, vectorAccessKey, vectorSecretKey)
	first := cmd.ExtFields[SignatureField]

	Sign(cmd, vectorAccessKey, vectorSecretKey)

	if second := cmd.ExtFields[SignatureField]; second != first {
		t.Errorf("re-signing changed the signature: %s then %s", first, second)
	}
}

// What the signature has to cover, one piece at a time. Each change is one
// the Broker sees, so a signature that survives it is one the Broker rejects.
func TestSignCoversTheWholeRequest(t *testing.T) {
	base := vectorCommand()
	Sign(base, vectorAccessKey, vectorSecretKey)
	unchanged := base.ExtFields[SignatureField]

	cases := []struct {
		name   string
		secret string
		mutate func(*RemotingCommand)
	}{
		{"a header field's value", vectorSecretKey, func(c *RemotingCommand) { c.ExtFields["topic"] = "OtherTopic" }},
		{"a header field nobody read", vectorSecretKey, func(c *RemotingCommand) { c.ExtFields["bname"] = "broker-b" }},
		{"an added header field", vectorSecretKey, func(c *RemotingCommand) { c.ExtFields["group"] = "G" }},
		{"the body", vectorSecretKey, func(c *RemotingCommand) { c.Body = []byte(`{"test":"other"}`) }},
		{"the secret key", "wrong-secret", func(c *RemotingCommand) {}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := vectorCommand()
			tc.mutate(cmd)
			Sign(cmd, vectorAccessKey, tc.secret)

			if got := cmd.ExtFields[SignatureField]; got == unchanged {
				t.Errorf("changing %s left the signature alone", tc.name)
			}
		})
	}
}
