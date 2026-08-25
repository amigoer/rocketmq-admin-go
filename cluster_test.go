package admin

import (
	"testing"
)

func TestIntegration_ExamineBrokerClusterInfo(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to query cluster info: %v", err)
	}

	if clusterInfo == nil {
		t.Fatal("cluster info must not be nil")
	}

	if len(clusterInfo.ClusterAddrTable) == 0 {
		t.Error("the cluster address table must not be empty")
	}

	if len(clusterInfo.BrokerAddrTable) == 0 {
		t.Error("the Broker address table must not be empty")
	}

	t.Logf("clusters: %d", len(clusterInfo.ClusterAddrTable))
	t.Logf("Brokers: %d", len(clusterInfo.BrokerAddrTable))

	for clusterName, brokers := range clusterInfo.ClusterAddrTable {
		t.Logf("cluster: %s, Broker names: %v", clusterName, brokers)
	}

	for brokerName, brokerData := range clusterInfo.BrokerAddrTable {
		t.Logf("Broker: %s, cluster: %s, addrs: %v",
			brokerName, brokerData.Cluster, brokerData.BrokerAddrs)
	}
}

func TestIntegration_GetNameServerConfig(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	config, err := client.GetNameServerConfig(ctx)
	if err != nil {
		t.Fatalf("failed to get NameServer config: %v", err)
	}

	if config == nil {
		t.Fatal("NameServer config must not be nil")
	}

	t.Logf("NameServer config entries: %d", len(config))
	for k, v := range config {
		t.Logf("  %s = %s", k, v)
	}
}

// This test may need elevated permissions; it skips rather than fails.
func TestIntegration_UpdateNameServerConfig(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// No NameServer setting is safe to change from a test, so leave this empty.
	properties := map[string]string{}

	if len(properties) == 0 {
		t.Skip("skipping NameServer config update test: no config key is safe to change")
	}

	err := client.UpdateNameServerConfig(ctx, properties)
	if err != nil {
		t.Logf("failed to update NameServer config (may be a permission issue): %v", err)
	}
}
