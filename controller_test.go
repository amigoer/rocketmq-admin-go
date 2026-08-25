package admin

import (
	"testing"
)

func TestIntegration_GetControllerMetaData(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// The controller listens on 9878 by default.
	controllerAddr := "localhost:9878"

	meta, err := client.GetControllerMetaData(ctx, controllerAddr)
	if err != nil {
		t.Logf("failed to get controller metadata (no controller may be deployed): %v", err)
		return
	}

	t.Logf("controller metadata:")
	t.Logf("  LeaderAddr: %s", meta.LeaderAddr)
	t.Logf("  LeaderId: %s", meta.LeaderId)
	t.Logf("  IsLeader: %v", meta.IsLeader)
	t.Logf("  ControllerAddrs: %d", len(meta.ControllerAddrs))
}

func TestIntegration_GetControllerConfig(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	controllerAddr := "localhost:9878"

	config, err := client.GetControllerConfig(ctx, controllerAddr)
	if err != nil {
		t.Logf("failed to get controller config (no controller may be deployed): %v", err)
		return
	}

	t.Logf("controller config entries: %d", len(config))
	for k, v := range config {
		t.Logf("  %s = %s", k, v)
	}
}

func TestIntegration_UpdateControllerConfig(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("skipping UpdateControllerConfig test: it would disturb a running controller")
}

func TestIntegration_ElectMaster(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("skipping ElectMaster test: the operation would affect the cluster")
}

func TestIntegration_CleanControllerBrokerData(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("skipping CleanControllerBrokerData test: the operation would erase data")
}

func TestIntegration_GetInSyncStateData(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to get cluster info: %v", err)
	}

	var brokerNames []string
	for name := range clusterInfo.BrokerAddrTable {
		brokerNames = append(brokerNames, name)
	}

	if len(brokerNames) == 0 {
		t.Skip("no Broker available")
	}

	controllerAddr := "localhost:9878"

	syncStateData, err := client.GetInSyncStateData(ctx, controllerAddr, brokerNames)
	if err != nil {
		t.Logf("failed to get in-sync state data (no controller may be deployed): %v", err)
		return
	}

	t.Logf("in-sync state entries: %d", len(syncStateData))
	for brokerName, data := range syncStateData {
		t.Logf("Broker %s:", brokerName)
		t.Logf("  MasterAddr: %s", data.MasterAddr)
		t.Logf("  MasterEpoch: %d", data.MasterEpoch)
		t.Logf("  MasterFlushOffset: %d", data.MasterFlushOffset)
		t.Logf("  InSyncMembers: %v", data.InSyncMembers)
	}
}
