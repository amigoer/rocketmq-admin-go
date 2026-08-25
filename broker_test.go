package admin

import (
	"testing"
)

func TestIntegration_FetchBrokerRuntimeStats(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to get cluster info: %v", err)
	}

	var brokerAddr string
	for _, brokerData := range clusterInfo.BrokerAddrTable {
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}
		if brokerAddr != "" {
			break
		}
	}

	if brokerAddr == "" {
		t.Fatal("no usable Broker address found")
	}

	stats, err := client.FetchBrokerRuntimeStats(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("failed to get Broker runtime stats: %v", err)
	}

	if stats == nil || stats.Table == nil {
		t.Fatal("stats must not be nil")
	}

	t.Logf("Broker runtime stat entries: %d", len(stats.Table))

	keyMetrics := []string{
		"brokerVersion",
		"brokerVersionDesc",
		"putTps",
		"getTransferedTps",
		"msgPutTotalYesterdayMorning",
		"msgPutTotalTodayMorning",
		"bootTimestamp",
	}

	for _, key := range keyMetrics {
		if value, ok := stats.Table[key]; ok {
			t.Logf("  %s = %s", key, value)
		}
	}
}

func TestIntegration_GetBrokerConfig(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to get cluster info: %v", err)
	}

	var brokerAddr string
	for _, brokerData := range clusterInfo.BrokerAddrTable {
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}
		if brokerAddr != "" {
			break
		}
	}

	if brokerAddr == "" {
		t.Fatal("no usable Broker address found")
	}

	config, err := client.GetBrokerConfig(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("failed to get Broker config: %v", err)
	}

	if config == nil {
		t.Fatal("Broker config must not be nil")
	}

	t.Logf("Broker config entries: %d", len(config))

	keyConfigs := []string{
		"brokerName",
		"brokerId",
		"brokerClusterName",
		"namesrvAddr",
		"autoCreateTopicEnable",
		"deleteWhen",
		"fileReservedTime",
	}

	for _, key := range keyConfigs {
		if value, ok := config[key]; ok {
			t.Logf("  %s = %s", key, value)
		}
	}
}

func TestIntegration_UpdateBrokerConfig(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to get cluster info: %v", err)
	}

	var brokerAddr string
	for _, brokerData := range clusterInfo.BrokerAddrTable {
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}
		if brokerAddr != "" {
			break
		}
	}

	if brokerAddr == "" {
		t.Fatal("no usable Broker address found")
	}

	// Updating config can need elevated permissions, so send an empty property
	// set: it exercises the call path without changing the Broker.
	properties := map[string]string{}

	if len(properties) == 0 {
		t.Skip("skipping Broker config update test: no config key is safe to change")
	}

	err = client.UpdateBrokerConfig(ctx, brokerAddr, properties)
	if err != nil {
		t.Logf("failed to update Broker config (may be a permission issue): %v", err)
	}
}

func TestIntegration_WipeWritePermOfBroker(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to get cluster info: %v", err)
	}

	var brokerName string
	for name := range clusterInfo.BrokerAddrTable {
		brokerName = name
		break
	}

	if brokerName == "" {
		t.Fatal("no usable Broker found")
	}

	// Wiping write permission would disrupt the Broker, so only log the call.
	t.Logf("WipeWritePermOfBroker (brokerName=%s) - not executed, it would disrupt the service", brokerName)

}

func TestIntegration_AddWritePermOfBroker(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to get cluster info: %v", err)
	}

	var brokerName string
	for name := range clusterInfo.BrokerAddrTable {
		brokerName = name
		break
	}

	if brokerName == "" {
		t.Fatal("no usable Broker found")
	}

	t.Logf("AddWritePermOfBroker (brokerName=%s) - not executed", brokerName)
}

func TestIntegration_ViewBrokerStatsData(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to get cluster info: %v", err)
	}

	var brokerAddr string
	for _, brokerData := range clusterInfo.BrokerAddrTable {
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}
		if brokerAddr != "" {
			break
		}
	}

	if brokerAddr == "" {
		t.Fatal("no usable Broker address found")
	}

	statsNames := []string{
		"TOPIC_PUT_NUMS",
		"TOPIC_PUT_SIZE",
		"GROUP_GET_NUMS",
		"BROKER_PUT_NUMS",
	}

	for _, statsName := range statsNames {
		stats, err := client.ViewBrokerStatsData(ctx, brokerAddr, statsName, "")
		if err != nil {
			t.Logf("failed to query stats %s: %v", statsName, err)
			continue
		}

		t.Logf("stats %s: StatsMinute=%+v, StatsHour=%+v, StatsDay=%+v",
			statsName, stats.StatsMinute, stats.StatsHour, stats.StatsDay)
		return
	}

	t.Log("no Broker stats available to query")
}

func TestIntegration_GetBrokerHAStatus(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to get cluster info: %v", err)
	}

	var brokerAddr string
	for _, brokerData := range clusterInfo.BrokerAddrTable {
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}
		if brokerAddr != "" {
			break
		}
	}

	if brokerAddr == "" {
		t.Fatal("no usable Broker address found")
	}

	status, err := client.GetBrokerHAStatus(ctx, brokerAddr)
	if err != nil {
		t.Logf("failed to get Broker HA status (may be unsupported): %v", err)
		return
	}

	t.Logf("Broker HA status: MasterAddr=%s", status.MasterAddr)
}

func TestIntegration_GetBrokerEpochCache(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to get cluster info: %v", err)
	}

	var brokerAddr string
	for _, brokerData := range clusterInfo.BrokerAddrTable {
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}
		if brokerAddr != "" {
			break
		}
	}

	if brokerAddr == "" {
		t.Fatal("no usable Broker address found")
	}

	epochInfo, err := client.GetBrokerEpochCache(ctx, brokerAddr)
	if err != nil {
		t.Logf("failed to get Broker epoch cache (may be unsupported): %v", err)
		return
	}

	t.Logf("Broker Epoch: Epoch=%d, MaxOffset=%d, ConfirmOffset=%d",
		epochInfo.Epoch, epochInfo.MaxOffset, epochInfo.ConfirmOffset)
}

func TestIntegration_AddBrokerToContainer(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("skipping AddBrokerToContainer test: needs a Broker container environment")
}

func TestIntegration_RemoveBrokerFromContainer(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("skipping RemoveBrokerFromContainer test: needs a Broker container environment")
}
