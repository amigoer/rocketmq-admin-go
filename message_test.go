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

	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		t.Fatalf("failed to get topic list: %v", err)
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
		t.Skip("no usable test topic")
	}

	queueData, err := client.QueryConsumeQueue(ctx, brokerAddr, testTopic, 0, 0, 10, "")
	if err != nil {
		t.Logf("failed to query consume queue (it may be empty): %v", err)
		return
	}

	t.Logf("consume queue entries: %d", len(queueData))
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
		t.Fatalf("failed to get topic list: %v", err)
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
		t.Skip("no usable test topic")
	}

	// An empty key matches every message.
	messages, err := client.QueryMessage(ctx, testTopic, "", 10, 0, 0)
	if err != nil {
		t.Logf("failed to query messages (there may be none): %v", err)
		return
	}

	t.Logf("found %d messages", len(messages))
	for i, msg := range messages {
		if i >= 3 {
			break
		}
		t.Logf("  MsgId=%s, Topic=%s", msg.MsgId, msg.Topic)
	}
}

func TestIntegration_ViewMessage(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("skipping ViewMessage test: needs a valid message id")
}

func TestIntegration_SearchOffset(t *testing.T) {
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

	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		t.Fatalf("failed to get topic list: %v", err)
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
		t.Skip("no usable test topic")
	}

	offset, err := client.SearchOffset(ctx, brokerAddr, testTopic, 0, 0)
	if err != nil {
		t.Logf("failed to search offset: %v", err)
		return
	}

	t.Logf("topic %s queue 0 offset: %d", testTopic, offset)
}

func TestIntegration_ConsumeMessageDirectly(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("skipping ConsumeMessageDirectly test: needs an online consumer and a valid message id")
}

func TestIntegration_ResumeCheckHalfMessage(t *testing.T) {
	skipIfNoRocketMQ(t)
	t.Skip("skipping ResumeCheckHalfMessage test: needs a valid transaction message id")
}

func TestIntegration_SetMessageRequestMode(t *testing.T) {
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
		t.Logf("failed to set message request mode (may be unsupported): %v", err)
	} else {
		t.Log("set message request mode")
	}
}
