package admin

import "testing"

func newBrokerNameTestClient(t *testing.T) *Client {
	t.Helper()
	client, err := NewClient(WithNameServers([]string{"ns:9876"}))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	return client
}

func TestRememberRouteBrokerNames(t *testing.T) {
	client := newBrokerNameTestClient(t)

	// A Proxy rewrites the Broker addresses in a route to its own address, so
	// what gets recorded is "Proxy address -> real Broker name".
	client.rememberRouteBrokerNames(&TopicRouteData{
		BrokerDatas: []*BrokerData{
			{BrokerName: "broker-a", BrokerAddrs: map[string]string{"0": "127.0.0.1:8080"}},
			{BrokerName: "broker-b", BrokerAddrs: map[string]string{"0": "10.0.0.2:10911", "1": "10.0.0.3:10911"}},
			nil,
		},
	})

	if got := client.brokerNameFor("127.0.0.1:8080"); got != "broker-a" {
		t.Fatalf("brokerNameFor(proxy) = %q, want broker-a", got)
	}
	// A slave's address must resolve to the same Broker name.
	if got := client.brokerNameFor("10.0.0.3:10911"); got != "broker-b" {
		t.Fatalf("brokerNameFor(slave) = %q, want broker-b", got)
	}
	if got := client.brokerNameFor("unknown:10911"); got != "" {
		t.Fatalf("an unknown address should return an empty string, got %q", got)
	}

	client.rememberRouteBrokerNames(nil)
}

func TestRememberClusterBrokerNames(t *testing.T) {
	client := newBrokerNameTestClient(t)
	client.rememberClusterBrokerNames(&ClusterInfo{
		BrokerAddrTable: map[string]*BrokerData{
			"broker-a": {BrokerName: "broker-a", BrokerAddrs: map[string]string{"0": "10.0.0.1:10911"}},
			// Falls back to the table key when BrokerData.BrokerName is empty.
			"broker-c": {BrokerAddrs: map[string]string{"0": "10.0.0.9:10911"}},
			"broker-d": nil,
		},
	})

	if got := client.brokerNameFor("10.0.0.1:10911"); got != "broker-a" {
		t.Fatalf("brokerNameFor() = %q, want broker-a", got)
	}
	if got := client.brokerNameFor("10.0.0.9:10911"); got != "broker-c" {
		t.Fatalf("an empty BrokerName should fall back to the table key, got %q", got)
	}

	client.rememberClusterBrokerNames(nil)
}

func TestRememberBrokerNameIgnoresBlanks(t *testing.T) {
	client := newBrokerNameTestClient(t)
	client.rememberBrokerName("", "broker-a")
	client.rememberBrokerName("10.0.0.1:10911", "")
	if got := client.brokerNameFor("10.0.0.1:10911"); got != "" {
		t.Fatalf("an empty name must not be recorded, got %q", got)
	}
}
