package admin

import (
	"testing"
)

// =============================================================================
// 维护管理接口集成测试
// =============================================================================

// TestIntegration_CleanExpiredConsumerQueueByAddr 测试清理过期消费队列
func TestIntegration_CleanExpiredConsumerQueueByAddr(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 获取 Broker 地址
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

	err = client.CleanExpiredConsumerQueueByAddr(ctx, brokerAddr)
	if err != nil {
		t.Logf("清理过期消费队列失败: %v", err)
	} else {
		t.Log("清理过期消费队列成功")
	}
}

// TestIntegration_CleanExpiredConsumerQueue 测试按集群清理过期消费队列
func TestIntegration_CleanExpiredConsumerQueue(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 获取集群名称
	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("获取集群信息失败: %v", err)
	}

	var clusterName string
	for name := range clusterInfo.ClusterAddrTable {
		clusterName = name
		break
	}

	if clusterName == "" {
		t.Skip("没有可用的集群")
	}

	err = client.CleanExpiredConsumerQueue(ctx, clusterName)
	if err != nil {
		t.Logf("按集群清理过期消费队列失败: %v", err)
	} else {
		t.Logf("按集群清理过期消费队列成功: %s", clusterName)
	}
}

// TestIntegration_DeleteExpiredCommitLogByAddr 测试删除过期 CommitLog
func TestIntegration_DeleteExpiredCommitLogByAddr(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 获取 Broker 地址
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

	err = client.DeleteExpiredCommitLogByAddr(ctx, brokerAddr)
	if err != nil {
		t.Logf("删除过期 CommitLog 失败: %v", err)
	} else {
		t.Log("删除过期 CommitLog 成功")
	}
}

// TestIntegration_DeleteExpiredCommitLog 测试按集群删除过期 CommitLog
func TestIntegration_DeleteExpiredCommitLog(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 获取集群名称
	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("获取集群信息失败: %v", err)
	}

	var clusterName string
	for name := range clusterInfo.ClusterAddrTable {
		clusterName = name
		break
	}

	if clusterName == "" {
		t.Skip("没有可用的集群")
	}

	err = client.DeleteExpiredCommitLog(ctx, clusterName)
	if err != nil {
		t.Logf("按集群删除过期 CommitLog 失败: %v", err)
	} else {
		t.Logf("按集群删除过期 CommitLog 成功: %s", clusterName)
	}
}

// TestIntegration_CleanUnusedTopicByAddr 测试清理未使用 Topic
func TestIntegration_CleanUnusedTopicByAddr(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 获取 Broker 地址
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

	err = client.CleanUnusedTopicByAddr(ctx, brokerAddr)
	if err != nil {
		t.Logf("清理未使用 Topic 失败: %v", err)
	} else {
		t.Log("清理未使用 Topic 成功")
	}
}

// TestIntegration_CleanUnusedTopic 测试按集群清理未使用 Topic
func TestIntegration_CleanUnusedTopic(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 获取集群名称
	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("获取集群信息失败: %v", err)
	}

	var clusterName string
	for name := range clusterInfo.ClusterAddrTable {
		clusterName = name
		break
	}

	if clusterName == "" {
		t.Skip("没有可用的集群")
	}

	err = client.CleanUnusedTopic(ctx, clusterName)
	if err != nil {
		t.Logf("按集群清理未使用 Topic 失败: %v", err)
	} else {
		t.Logf("按集群清理未使用 Topic 成功: %s", clusterName)
	}
}

// TestIntegration_SetCommitLogReadAheadMode 测试设置 CommitLog 预读模式
func TestIntegration_SetCommitLogReadAheadMode(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 获取 Broker 地址
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

	// 设置为顺序预读模式
	err = client.SetCommitLogReadAheadMode(ctx, brokerAddr, 1)
	if err != nil {
		t.Logf("设置 CommitLog 预读模式失败（可能不支持）: %v", err)
	} else {
		t.Log("设置 CommitLog 预读模式成功")
	}
}

// TestIntegration_SetCommitLogReadAheadModeInCluster 测试按集群设置 CommitLog 预读模式
func TestIntegration_SetCommitLogReadAheadModeInCluster(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 获取集群名称
	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("获取集群信息失败: %v", err)
	}

	var clusterName string
	for name := range clusterInfo.ClusterAddrTable {
		clusterName = name
		break
	}

	if clusterName == "" {
		t.Skip("没有可用的集群")
	}

	err = client.SetCommitLogReadAheadModeInCluster(ctx, clusterName, 1)
	if err != nil {
		t.Logf("按集群设置 CommitLog 预读模式失败: %v", err)
	} else {
		t.Logf("按集群设置 CommitLog 预读模式成功: %s", clusterName)
	}
}

// TestIntegration_ExportRocksDBConfigToJson 测试导出 RocksDB 配置
func TestIntegration_ExportRocksDBConfigToJson(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 获取 Broker 地址
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

	configJson, err := client.ExportRocksDBConfigToJson(ctx, brokerAddr)
	if err != nil {
		t.Logf("导出 RocksDB 配置失败（Broker 可能未使用 RocksDB）: %v", err)
		return
	}

	t.Logf("RocksDB 配置 JSON 长度: %d", len(configJson))
	if len(configJson) > 200 {
		t.Logf("RocksDB 配置内容（截断）: %s...", configJson[:200])
	} else {
		t.Logf("RocksDB 配置内容: %s", configJson)
	}
}

// TestIntegration_CheckRocksdbCqWriteProgress 测试检查 RocksDB CQ 写入进度
func TestIntegration_CheckRocksdbCqWriteProgress(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 获取 Broker 地址
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

	// 获取一个 Topic
	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		t.Fatalf("获取 Topic 列表失败: %v", err)
	}

	var testTopic string
	for _, topic := range topicList.TopicList {
		if len(topic) >= 4 && topic[:4] == "RMQ_" {
			continue
		}
		testTopic = topic
		break
	}

	if testTopic == "" {
		t.Skip("没有可用的测试 Topic")
	}

	progress, err := client.CheckRocksdbCqWriteProgress(ctx, brokerAddr, testTopic)
	if err != nil {
		t.Logf("检查 RocksDB CQ 写入进度失败（可能不支持）: %v", err)
		return
	}

	t.Logf("RocksDB CQ 写入进度数量: %d", len(progress))
	for _, p := range progress {
		t.Logf("  Topic=%s, QueueId=%d, Progress=%.2f%%, IsCompleted=%v",
			p.Topic, p.QueueId, p.Progress, p.IsCompleted)
	}
}

// TestIntegration_SwitchTimerEngine 测试切换定时器引擎
func TestIntegration_SwitchTimerEngine(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("跳过 SwitchTimerEngine 测试：此操作会影响 Broker 运行")
}
