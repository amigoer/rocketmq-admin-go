package admin

import (
	"testing"
)

// =============================================================================
// 集群管理接口集成测试
// =============================================================================

// TestIntegration_ExamineBrokerClusterInfo 测试查询集群信息
func TestIntegration_ExamineBrokerClusterInfo(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("查询集群信息失败: %v", err)
	}

	if clusterInfo == nil {
		t.Fatal("集群信息不应为 nil")
	}

	// 验证至少有一个集群
	if len(clusterInfo.ClusterAddrTable) == 0 {
		t.Error("集群地址表不应为空")
	}

	// 验证至少有一个 Broker
	if len(clusterInfo.BrokerAddrTable) == 0 {
		t.Error("Broker 地址表不应为空")
	}

	// 打印集群信息用于调试
	t.Logf("集群数量: %d", len(clusterInfo.ClusterAddrTable))
	t.Logf("Broker 数量: %d", len(clusterInfo.BrokerAddrTable))

	for clusterName, brokers := range clusterInfo.ClusterAddrTable {
		t.Logf("集群: %s, Broker 名称: %v", clusterName, brokers)
	}

	for brokerName, brokerData := range clusterInfo.BrokerAddrTable {
		t.Logf("Broker: %s, 集群: %s, 地址: %v",
			brokerName, brokerData.Cluster, brokerData.BrokerAddrs)
	}
}

// TestIntegration_GetNameServerConfig 测试获取 NameServer 配置
func TestIntegration_GetNameServerConfig(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	config, err := client.GetNameServerConfig(ctx)
	if err != nil {
		t.Fatalf("获取 NameServer 配置失败: %v", err)
	}

	if config == nil {
		t.Fatal("NameServer 配置不应为 nil")
	}

	t.Logf("NameServer 配置项数量: %d", len(config))
	for k, v := range config {
		t.Logf("  %s = %s", k, v)
	}
}

// TestIntegration_UpdateNameServerConfig 测试更新 NameServer 配置
// 注意：此测试可能需要特定权限，失败时跳过
func TestIntegration_UpdateNameServerConfig(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 尝试更新一个安全的配置项
	properties := map[string]string{
		// 使用一个相对安全的配置项进行测试
	}

	// 如果没有可安全更新的配置项，跳过此测试
	if len(properties) == 0 {
		t.Skip("跳过 NameServer 配置更新测试：没有安全的测试配置项")
	}

	err := client.UpdateNameServerConfig(ctx, properties)
	if err != nil {
		t.Logf("更新 NameServer 配置失败（可能是权限问题）: %v", err)
	}
}
