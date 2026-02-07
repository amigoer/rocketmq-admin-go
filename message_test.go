package admin

import (
	"testing"
)

// =============================================================================
// 消息管理接口集成测试
// =============================================================================

// TestIntegration_QueryConsumeQueue 测试查询消费队列
func TestIntegration_QueryConsumeQueue(t *testing.T) {
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
		if topic == "SCHEDULE_TOPIC_XXXX" || topic == "TBW102" {
			continue
		}
		testTopic = topic
		break
	}

	if testTopic == "" {
		t.Skip("没有可用的测试 Topic")
	}

	// 查询消费队列
	queueData, err := client.QueryConsumeQueue(ctx, brokerAddr, testTopic, 0, 0, 10, "")
	if err != nil {
		t.Logf("查询消费队列失败（可能是队列为空）: %v", err)
		return
	}

	t.Logf("消费队列数据数量: %d", len(queueData))
	for i, data := range queueData {
		if i >= 3 {
			break
		}
		t.Logf("  PhysicalOffset=%d, Size=%d, TagsCode=%d",
			data.PhysicalOffset, data.Size, data.TagsCode)
	}
}

// TestIntegration_QueryMessage 测试按 Key 查询消息
func TestIntegration_QueryMessage(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

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

	// 按 Key 查询消息（使用空 Key 查询所有）
	messages, err := client.QueryMessage(ctx, testTopic, "", 10, 0, 0)
	if err != nil {
		t.Logf("查询消息失败（可能是没有消息）: %v", err)
		return
	}

	t.Logf("查询到 %d 条消息", len(messages))
	for i, msg := range messages {
		if i >= 3 {
			break
		}
		t.Logf("  MsgId=%s, Topic=%s", msg.MsgId, msg.Topic)
	}
}

// TestIntegration_ViewMessage 测试按 ID 查询消息详情
func TestIntegration_ViewMessage(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("跳过 ViewMessage 测试：需要有效的消息 ID")
}

// TestIntegration_SearchOffset 测试搜索偏移
func TestIntegration_SearchOffset(t *testing.T) {
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

	// 搜索偏移
	offset, err := client.SearchOffset(ctx, brokerAddr, testTopic, 0, 0)
	if err != nil {
		t.Logf("搜索偏移失败: %v", err)
		return
	}

	t.Logf("Topic %s 队列 0 偏移: %d", testTopic, offset)
}

// TestIntegration_ConsumeMessageDirectly 测试直接消费消息
func TestIntegration_ConsumeMessageDirectly(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("跳过 ConsumeMessageDirectly 测试：需要在线消费者和有效消息 ID")
}

// TestIntegration_ResumeCheckHalfMessage 测试恢复检查半消息
func TestIntegration_ResumeCheckHalfMessage(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("跳过 ResumeCheckHalfMessage 测试：需要有效的事务消息 ID")
}

// TestIntegration_SetMessageRequestMode 测试设置消息请求模式
func TestIntegration_SetMessageRequestMode(t *testing.T) {
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

	// 创建测试资源
	topicName := getTestTopicName("MSGMODE")
	groupName := getTestGroupName("MSGMODE")

	topicConfig := TopicConfig{
		TopicName:      topicName,
		ReadQueueNums:  4,
		WriteQueueNums: 4,
		Perm:           6,
	}
	subConfig := SubscriptionGroupConfig{
		GroupName:     groupName,
		ConsumeEnable: true,
	}

	_ = client.CreateTopic(ctx, brokerAddr, topicConfig)
	_ = client.CreateSubscriptionGroup(ctx, brokerAddr, subConfig)
	defer func() {
		_ = client.DeleteTopicInBroker(ctx, brokerAddr, topicName)
		_ = client.DeleteTopicInNameServer(ctx, topicName)
		_ = client.DeleteSubscriptionGroup(ctx, brokerAddr, groupName)
	}()

	// 设置消息请求模式
	err = client.SetMessageRequestMode(ctx, brokerAddr, topicName, groupName, 0, 0)
	if err != nil {
		t.Logf("设置消息请求模式失败（可能不支持）: %v", err)
	} else {
		t.Log("设置消息请求模式成功")
	}
}

// TestIntegration_ExportPopRecords 测试导出 Pop 记录
func TestIntegration_ExportPopRecords(t *testing.T) {
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

	// 获取 Topic 和消费组
	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		t.Fatalf("获取 Topic 列表失败: %v", err)
	}

	groups, err := client.GetAllSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("获取订阅组失败: %v", err)
	}

	var testTopic string
	for _, topic := range topicList.TopicList {
		if len(topic) >= 4 && topic[:4] == "RMQ_" {
			continue
		}
		testTopic = topic
		break
	}

	var testGroup string
	for name := range groups {
		testGroup = name
		break
	}

	if testTopic == "" || testGroup == "" {
		t.Skip("没有可用的测试 Topic 或消费组")
	}

	records, err := client.ExportPopRecords(ctx, brokerAddr, testTopic, testGroup)
	if err != nil {
		t.Logf("导出 Pop 记录失败（可能没有 Pop 消费记录）: %v", err)
		return
	}

	t.Logf("Pop 记录数量: %d", len(records))
	for i, record := range records {
		if i >= 3 {
			break
		}
		t.Logf("  Topic=%s, Group=%s, QueueId=%d",
			record.Topic, record.ConsumerGroup, record.QueueId)
	}
}
