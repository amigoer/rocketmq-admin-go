package admin

import (
	"testing"
)

// =============================================================================
// Controller 管理接口集成测试 (RocketMQ 5.x)
// =============================================================================

// TestIntegration_GetControllerMetaData 测试获取 Controller 元数据
func TestIntegration_GetControllerMetaData(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// Controller 端口通常是 9878
	controllerAddr := "localhost:9878"

	meta, err := client.GetControllerMetaData(ctx, controllerAddr)
	if err != nil {
		t.Logf("获取 Controller 元数据失败（Controller 可能未部署）: %v", err)
		return
	}

	t.Logf("Controller 元数据:")
	t.Logf("  LeaderAddr: %s", meta.LeaderAddr)
	t.Logf("  LeaderId: %s", meta.LeaderId)
	t.Logf("  IsLeader: %v", meta.IsLeader)
	t.Logf("  ControllerAddrs 数量: %d", len(meta.ControllerAddrs))
}

// TestIntegration_GetControllerConfig 测试获取 Controller 配置
func TestIntegration_GetControllerConfig(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	controllerAddr := "localhost:9878"

	config, err := client.GetControllerConfig(ctx, controllerAddr)
	if err != nil {
		t.Logf("获取 Controller 配置失败（Controller 可能未部署）: %v", err)
		return
	}

	t.Logf("Controller 配置项数量: %d", len(config))
	for k, v := range config {
		t.Logf("  %s = %s", k, v)
	}
}

// TestIntegration_UpdateControllerConfig 测试更新 Controller 配置
func TestIntegration_UpdateControllerConfig(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("跳过 UpdateControllerConfig 测试：避免影响 Controller 运行")
}

// TestIntegration_ElectMaster 测试选举 Master
func TestIntegration_ElectMaster(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("跳过 ElectMaster 测试：此操作会影响集群")
}

// TestIntegration_CleanControllerBrokerData 测试清理 Controller Broker 数据
func TestIntegration_CleanControllerBrokerData(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("跳过 CleanControllerBrokerData 测试：此操作会清理数据")
}

// TestIntegration_GetInSyncStateData 测试获取同步状态数据
func TestIntegration_GetInSyncStateData(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 获取集群信息
	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("获取集群信息失败: %v", err)
	}

	var brokerNames []string
	for name := range clusterInfo.BrokerAddrTable {
		brokerNames = append(brokerNames, name)
	}

	if len(brokerNames) == 0 {
		t.Skip("没有可用的 Broker")
	}

	controllerAddr := "localhost:9878"

	syncStateData, err := client.GetInSyncStateData(ctx, controllerAddr, brokerNames)
	if err != nil {
		t.Logf("获取同步状态数据失败（Controller 可能未部署）: %v", err)
		return
	}

	t.Logf("同步状态数据数量: %d", len(syncStateData))
	for brokerName, data := range syncStateData {
		t.Logf("Broker %s:", brokerName)
		t.Logf("  MasterAddr: %s", data.MasterAddr)
		t.Logf("  MasterEpoch: %d", data.MasterEpoch)
		t.Logf("  MasterFlushOffset: %d", data.MasterFlushOffset)
		t.Logf("  InSyncMembers: %v", data.InSyncMembers)
	}
}
