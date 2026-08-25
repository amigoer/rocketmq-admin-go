package admin

import (
	"testing"
)

func TestIntegration_QueryConsumeQueue(t *testing.T) {
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

func TestIntegration_QueryMessage(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

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

	// An empty key matches every message.
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

func TestIntegration_ViewMessage(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("跳过 ViewMessage 测试：需要有效的消息 ID")
}

func TestIntegration_SearchOffset(t *testing.T) {
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

	offset, err := client.SearchOffset(ctx, brokerAddr, testTopic, 0, 0)
	if err != nil {
		t.Logf("搜索偏移失败: %v", err)
		return
	}

	t.Logf("Topic %s 队列 0 偏移: %d", testTopic, offset)
}

func TestIntegration_ConsumeMessageDirectly(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("跳过 ConsumeMessageDirectly 测试：需要在线消费者和有效消息 ID")
}

func TestIntegration_ResumeCheckHalfMessage(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("跳过 ResumeCheckHalfMessage 测试：需要有效的事务消息 ID")
}

func TestIntegration_SetMessageRequestMode(t *testing.T) {
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

	err = client.SetMessageRequestMode(ctx, brokerAddr, topicName, groupName, 0, 0)
	if err != nil {
		t.Logf("设置消息请求模式失败（可能不支持）: %v", err)
	} else {
		t.Log("设置消息请求模式成功")
	}
}
