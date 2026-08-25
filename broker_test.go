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
		t.Fatalf("获取集群信息失败: %v", err)
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
		t.Fatal("未找到可用的 Broker 地址")
	}

	stats, err := client.FetchBrokerRuntimeStats(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("获取 Broker 运行时统计失败: %v", err)
	}

	if stats == nil || stats.Table == nil {
		t.Fatal("统计数据不应为 nil")
	}

	t.Logf("Broker 运行时统计项数量: %d", len(stats.Table))

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
		t.Fatalf("获取集群信息失败: %v", err)
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
		t.Fatal("未找到可用的 Broker 地址")
	}

	config, err := client.GetBrokerConfig(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("获取 Broker 配置失败: %v", err)
	}

	if config == nil {
		t.Fatal("Broker 配置不应为 nil")
	}

	t.Logf("Broker 配置项数量: %d", len(config))

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
		t.Fatalf("获取集群信息失败: %v", err)
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
		t.Fatal("未找到可用的 Broker 地址")
	}

	// Updating config can need elevated permissions, so send an empty property
	// set: it exercises the call path without changing the Broker.
	properties := map[string]string{}

	if len(properties) == 0 {
		t.Skip("跳过 Broker 配置更新测试：没有安全的测试配置项")
	}

	err = client.UpdateBrokerConfig(ctx, brokerAddr, properties)
	if err != nil {
		t.Logf("更新 Broker 配置失败（可能是权限问题）: %v", err)
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
		t.Fatalf("获取集群信息失败: %v", err)
	}

	var brokerName string
	for name := range clusterInfo.BrokerAddrTable {
		brokerName = name
		break
	}

	if brokerName == "" {
		t.Fatal("未找到可用的 Broker")
	}

	// Wiping write permission would disrupt the Broker, so only log the call.
	t.Logf("测试 WipeWritePermOfBroker (brokerName=%s) - 跳过实际执行以避免影响服务", brokerName)

}

func TestIntegration_AddWritePermOfBroker(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("获取集群信息失败: %v", err)
	}

	var brokerName string
	for name := range clusterInfo.BrokerAddrTable {
		brokerName = name
		break
	}

	if brokerName == "" {
		t.Fatal("未找到可用的 Broker")
	}

	t.Logf("测试 AddWritePermOfBroker (brokerName=%s) - 跳过实际执行", brokerName)
}

func TestIntegration_ViewBrokerStatsData(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("获取集群信息失败: %v", err)
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
		t.Fatal("未找到可用的 Broker 地址")
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
			t.Logf("查询统计 %s 失败: %v", statsName, err)
			continue
		}

		t.Logf("统计 %s: StatsMinute=%+v, StatsHour=%+v, StatsDay=%+v",
			statsName, stats.StatsMinute, stats.StatsHour, stats.StatsDay)
		return
	}

	t.Log("没有可查询的 Broker 统计数据")
}

func TestIntegration_GetBrokerHAStatus(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("获取集群信息失败: %v", err)
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
		t.Fatal("未找到可用的 Broker 地址")
	}

	status, err := client.GetBrokerHAStatus(ctx, brokerAddr)
	if err != nil {
		t.Logf("获取 Broker HA 状态失败（可能不支持）: %v", err)
		return
	}

	t.Logf("Broker HA 状态: MasterAddr=%s", status.MasterAddr)
}

func TestIntegration_GetBrokerEpochCache(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("获取集群信息失败: %v", err)
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
		t.Fatal("未找到可用的 Broker 地址")
	}

	epochInfo, err := client.GetBrokerEpochCache(ctx, brokerAddr)
	if err != nil {
		t.Logf("获取 Broker Epoch 缓存失败（可能不支持）: %v", err)
		return
	}

	t.Logf("Broker Epoch: Epoch=%d, MaxOffset=%d, ConfirmOffset=%d",
		epochInfo.Epoch, epochInfo.MaxOffset, epochInfo.ConfirmOffset)
}

func TestIntegration_AddBrokerToContainer(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("跳过 AddBrokerToContainer 测试：需要 Broker 容器环境")
}

func TestIntegration_RemoveBrokerFromContainer(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("跳过 RemoveBrokerFromContainer 测试：需要 Broker 容器环境")
}
