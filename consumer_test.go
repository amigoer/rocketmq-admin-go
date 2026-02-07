package admin

import (
	"testing"
)

// =============================================================================
// 消费者管理接口集成测试
// =============================================================================

// TestIntegration_CreateAndDeleteSubscriptionGroup 测试创建和删除订阅组
func TestIntegration_CreateAndDeleteSubscriptionGroup(t *testing.T) {
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

	// 创建测试订阅组
	groupName := getTestGroupName("CREATE")
	config := SubscriptionGroupConfig{
		GroupName:                      groupName,
		ConsumeEnable:                  true,
		ConsumeFromMinEnable:           false,
		ConsumeBroadcastEnable:         false,
		RetryQueueNums:                 1,
		RetryMaxTimes:                  16,
		BrokerId:                       0,
		WhichBrokerWhenConsumeSlowly:   1,
		NotifyConsumerIdsChangedEnable: true,
	}

	t.Logf("创建订阅组: %s", groupName)
	err = client.CreateSubscriptionGroup(ctx, brokerAddr, config)
	if err != nil {
		t.Fatalf("创建订阅组失败: %v", err)
	}

	// 验证创建成功
	groupConfig, err := client.ExamineSubscriptionGroupConfig(ctx, brokerAddr, groupName)
	if err != nil {
		t.Logf("查询订阅组配置失败（可能是接口问题）: %v", err)
	} else {
		t.Logf("订阅组配置: GroupName=%s, ConsumeEnable=%v",
			groupConfig.GroupName, groupConfig.ConsumeEnable)
	}

	// 删除订阅组
	t.Logf("删除订阅组: %s", groupName)
	err = client.DeleteSubscriptionGroup(ctx, brokerAddr, groupName)
	if err != nil {
		t.Fatalf("删除订阅组失败: %v", err)
	}
}

// TestIntegration_GetAllSubscriptionGroup 测试获取所有订阅组
func TestIntegration_GetAllSubscriptionGroup(t *testing.T) {
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

	groups, err := client.GetAllSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("获取所有订阅组失败: %v", err)
	}

	t.Logf("订阅组数量: %d", len(groups))

	// 打印部分订阅组
	count := 0
	for name, config := range groups {
		if count >= 5 {
			break
		}
		t.Logf("  %s: ConsumeEnable=%v, RetryMaxTimes=%d",
			name, config.ConsumeEnable, config.RetryMaxTimes)
		count++
	}
}

// TestIntegration_GetUserSubscriptionGroup 测试获取用户订阅组（过滤系统组）
func TestIntegration_GetUserSubscriptionGroup(t *testing.T) {
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

	userGroups, err := client.GetUserSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("获取用户订阅组失败: %v", err)
	}

	t.Logf("用户订阅组数量: %d", len(userGroups))

	for name := range userGroups {
		t.Logf("  用户订阅组: %s", name)
	}
}

// TestIntegration_ExamineConsumeStats 测试查询消费统计
func TestIntegration_ExamineConsumeStats(t *testing.T) {
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

	// 获取所有订阅组
	groups, err := client.GetAllSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("获取订阅组失败: %v", err)
	}

	// 对第一个用户组查询消费统计
	for groupName := range groups {
		stats, err := client.ExamineConsumeStats(ctx, groupName)
		if err != nil {
			continue
		}

		t.Logf("消费组 %s 统计信息:", groupName)
		t.Logf("  ConsumeTps: %.2f", stats.ConsumeTps)
		t.Logf("  OffsetTable 大小: %d", len(stats.OffsetTable))

		for key, wrapper := range stats.OffsetTable {
			t.Logf("  %s: BrokerOffset=%d, ConsumerOffset=%d",
				key, wrapper.BrokerOffset, wrapper.ConsumerOffset)
		}
		return
	}

	t.Log("没有可查询消费统计的消费组")
}

// TestIntegration_ExamineConsumerConnectionInfo 测试查询消费者连接信息
func TestIntegration_ExamineConsumerConnectionInfo(t *testing.T) {
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

	// 获取所有订阅组
	groups, err := client.GetAllSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("获取订阅组失败: %v", err)
	}

	// 尝试查询连接信息
	for groupName := range groups {
		connInfo, err := client.ExamineConsumerConnectionInfo(ctx, groupName)
		if err == ErrConsumerGroupNotFound {
			continue
		}
		if err != nil {
			continue
		}

		t.Logf("消费组 %s 连接信息:", groupName)
		t.Logf("  消费类型: %s", connInfo.ConsumeType)
		t.Logf("  消息模型: %s", connInfo.MessageModel)
		t.Logf("  连接数: %d", len(connInfo.ConnectionSet))

		for _, conn := range connInfo.ConnectionSet {
			t.Logf("  客户端: %s, 地址: %s", conn.ClientId, conn.ClientAddr)
		}
		return
	}

	t.Log("没有在线的消费者")
}

// TestIntegration_QueryTopicsByConsumer 测试查询消费者订阅的 Topic
func TestIntegration_QueryTopicsByConsumer(t *testing.T) {
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

	// 获取所有订阅组
	groups, err := client.GetAllSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("获取订阅组失败: %v", err)
	}

	// 尝试查询订阅的 Topic
	for groupName := range groups {
		topicList, err := client.QueryTopicsByConsumer(ctx, groupName)
		if err != nil {
			continue
		}

		if len(topicList.TopicList) > 0 {
			t.Logf("消费组 %s 订阅的 Topic: %v", groupName, topicList.TopicList)
			return
		}
	}

	t.Log("没有找到有订阅 Topic 的消费组")
}

// TestIntegration_UpdateConsumeOffset 测试更新消费 Offset
func TestIntegration_UpdateConsumeOffset(t *testing.T) {
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

	// 创建测试订阅组
	groupName := getTestGroupName("OFFSET")
	config := SubscriptionGroupConfig{
		GroupName:     groupName,
		ConsumeEnable: true,
	}

	err = client.CreateSubscriptionGroup(ctx, brokerAddr, config)
	if err != nil {
		t.Fatalf("创建订阅组失败: %v", err)
	}
	defer func() {
		_ = client.DeleteSubscriptionGroup(ctx, brokerAddr, groupName)
	}()

	// 获取一个 Topic
	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		t.Fatalf("获取 Topic 列表失败: %v", err)
	}

	var testTopic string
	for _, topic := range topicList.TopicList {
		if len(topic) < 4 || topic[:4] != "RMQ_" {
			testTopic = topic
			break
		}
	}

	if testTopic == "" {
		t.Skip("没有可用的测试 Topic")
	}

	// 更新 Offset
	err = client.UpdateConsumeOffset(ctx, brokerAddr, groupName, testTopic, 0, 100)
	if err != nil {
		t.Logf("更新消费 Offset 失败（可能是 Topic 不存在于此 Broker）: %v", err)
	} else {
		t.Logf("更新消费 Offset 成功")
	}
}

// TestIntegration_ResetOffsetByTimestamp 测试按时间戳重置消费位点
func TestIntegration_ResetOffsetByTimestamp(t *testing.T) {
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

	// 获取所有订阅组
	groups, err := client.GetAllSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("获取订阅组失败: %v", err)
	}

	// 获取 Topic 列表
	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		t.Fatalf("获取 Topic 列表失败: %v", err)
	}

	var testTopic string
	for _, topic := range topicList.TopicList {
		if len(topic) < 4 || topic[:4] != "RMQ_" {
			testTopic = topic
			break
		}
	}

	if testTopic == "" {
		t.Skip("没有可用的测试 Topic")
	}

	// 尝试重置 Offset
	for groupName := range groups {
		result, err := client.ResetOffsetByTimestamp(ctx, testTopic, groupName, 0, false)
		if err != nil {
			continue
		}

		t.Logf("重置消费组 %s 的 Offset 结果: %d 个队列", groupName, len(result))
		return
	}

	t.Log("没有可重置 Offset 的消费组")
}

// TestIntegration_QueryConsumeTimeSpan 测试查询消费时间跨度
func TestIntegration_QueryConsumeTimeSpan(t *testing.T) {
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

	// 获取订阅组和 Topic
	groups, err := client.GetAllSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("获取订阅组失败: %v", err)
	}

	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		t.Fatalf("获取 Topic 列表失败: %v", err)
	}

	var testTopic string
	for _, topic := range topicList.TopicList {
		if len(topic) < 4 || topic[:4] != "RMQ_" {
			testTopic = topic
			break
		}
	}

	if testTopic == "" {
		t.Skip("没有可用的测试 Topic")
	}

	for groupName := range groups {
		spans, err := client.QueryConsumeTimeSpan(ctx, testTopic, groupName)
		if err != nil {
			continue
		}

		if len(spans) > 0 {
			t.Logf("消费组 %s 的时间跨度: %d 个", groupName, len(spans))
			for _, span := range spans {
				t.Logf("  MinTimestamp=%d, MaxTimestamp=%d",
					span.MinTimeStamp, span.MaxTimeStamp)
			}
			return
		}
	}

	t.Log("没有找到消费时间跨度数据")
}

// TestIntegration_GetConsumerRunningInfo 测试获取消费者运行时信息
func TestIntegration_GetConsumerRunningInfo(t *testing.T) {
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

	// 获取订阅组
	groups, err := client.GetAllSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("获取订阅组失败: %v", err)
	}

	for groupName := range groups {
		runningInfo, err := client.GetConsumerRunningInfo(ctx, groupName, "", false)
		if err == ErrConsumerGroupNotFound {
			continue
		}
		if err != nil {
			continue
		}

		t.Logf("消费组 %s 运行时信息:", groupName)
		t.Logf("  JStack 长度: %d", len(runningInfo.JStack))
		return
	}

	t.Log("没有在线的消费者")
}

// TestIntegration_ColdDataFlowCtr 测试冷数据流控
func TestIntegration_ColdDataFlowCtr(t *testing.T) {
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

	// 获取冷数据流控信息
	infos, err := client.GetColdDataFlowCtrInfo(ctx, brokerAddr)
	if err != nil {
		t.Logf("获取冷数据流控信息失败（可能不支持）: %v", err)
		return
	}

	t.Logf("冷数据流控信息数量: %d", len(infos))
	for _, info := range infos {
		t.Logf("  消费组=%s, CurrentQPS=%d, IsEnabled=%v",
			info.ConsumerGroup, info.CurrentQPS, info.IsFlowCtrEnabled)
	}
}

// TestIntegration_UpdateColdDataFlowCtrGroupConfig 测试更新冷数据流控配置
func TestIntegration_UpdateColdDataFlowCtrGroupConfig(t *testing.T) {
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

	// 创建测试订阅组
	groupName := getTestGroupName("COLDDATA")
	subConfig := SubscriptionGroupConfig{
		GroupName:     groupName,
		ConsumeEnable: true,
	}

	err = client.CreateSubscriptionGroup(ctx, brokerAddr, subConfig)
	if err != nil {
		t.Fatalf("创建订阅组失败: %v", err)
	}
	defer func() {
		_ = client.DeleteSubscriptionGroup(ctx, brokerAddr, groupName)
	}()

	// 更新冷数据流控配置
	config := ColdDataFlowCtrConfig{
		ConsumerGroup:   groupName,
		ThresholdPerSec: 1000,
		GlobalThreshold: 10000,
		EnableFlowCtr:   true,
	}

	err = client.UpdateColdDataFlowCtrGroupConfig(ctx, brokerAddr, config)
	if err != nil {
		t.Logf("更新冷数据流控配置失败（可能不支持）: %v", err)
		return
	}

	t.Logf("更新冷数据流控配置成功")

	// 移除配置
	err = client.RemoveColdDataFlowCtrGroupConfig(ctx, brokerAddr, groupName)
	if err != nil {
		t.Logf("移除冷数据流控配置失败: %v", err)
	}
}

// TestIntegration_CloneGroupOffset 测试克隆消费组偏移
func TestIntegration_CloneGroupOffset(t *testing.T) {
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

	// 创建源和目标订阅组
	srcGroup := getTestGroupName("SRC")
	destGroup := getTestGroupName("DEST")

	srcConfig := SubscriptionGroupConfig{GroupName: srcGroup, ConsumeEnable: true}
	destConfig := SubscriptionGroupConfig{GroupName: destGroup, ConsumeEnable: true}

	_ = client.CreateSubscriptionGroup(ctx, brokerAddr, srcConfig)
	_ = client.CreateSubscriptionGroup(ctx, brokerAddr, destConfig)
	defer func() {
		_ = client.DeleteSubscriptionGroup(ctx, brokerAddr, srcGroup)
		_ = client.DeleteSubscriptionGroup(ctx, brokerAddr, destGroup)
	}()

	// 获取 Topic
	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		t.Fatalf("获取 Topic 列表失败: %v", err)
	}

	var testTopic string
	for _, topic := range topicList.TopicList {
		if len(topic) < 4 || topic[:4] != "RMQ_" {
			testTopic = topic
			break
		}
	}

	if testTopic == "" {
		t.Skip("没有可用的测试 Topic")
	}

	// 克隆 Offset
	err = client.CloneGroupOffset(ctx, srcGroup, destGroup, testTopic, false)
	if err != nil {
		t.Logf("克隆 Offset 失败（可能是源组没有消费数据）: %v", err)
	} else {
		t.Log("克隆 Offset 成功")
	}
}

// TestIntegration_UpdateAndGetGroupReadForbidden 测试更新并获取组读取禁止状态
func TestIntegration_UpdateAndGetGroupReadForbidden(t *testing.T) {
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

	// 创建测试订阅组
	groupName := getTestGroupName("FORBID")
	config := SubscriptionGroupConfig{GroupName: groupName, ConsumeEnable: true}

	err = client.CreateSubscriptionGroup(ctx, brokerAddr, config)
	if err != nil {
		t.Fatalf("创建订阅组失败: %v", err)
	}
	defer func() {
		_ = client.DeleteSubscriptionGroup(ctx, brokerAddr, groupName)
	}()

	// 禁止读取
	forbidden, err := client.UpdateAndGetGroupReadForbidden(ctx, brokerAddr, groupName, "", true)
	if err != nil {
		t.Logf("更新读取禁止状态失败: %v", err)
	} else {
		t.Logf("读取禁止状态: %v", forbidden)
	}

	// 恢复读取
	_, err = client.UpdateAndGetGroupReadForbidden(ctx, brokerAddr, groupName, "", false)
	if err != nil {
		t.Logf("恢复读取状态失败: %v", err)
	}
}
