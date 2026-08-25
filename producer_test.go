package admin

import (
	"testing"
)

func TestIntegration_ExamineProducerConnectionInfo(t *testing.T) {
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

	producerGroup := "DEFAULT_PRODUCER"
	connInfo, err := client.ExamineProducerConnectionInfo(ctx, producerGroup, testTopic)
	if err != nil {
		t.Logf("查询生产者连接信息失败（可能没有在线生产者）: %v", err)
		return
	}

	t.Logf("生产者组 %s 连接信息:", producerGroup)
	t.Logf("  连接数: %d", len(connInfo.ConnectionSet))
	for _, conn := range connInfo.ConnectionSet {
		t.Logf("  客户端: %s, 地址: %s", conn.ClientId, conn.ClientAddr)
	}
}

func TestIntegration_GetAllProducerInfo(t *testing.T) {
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

	producerInfo, err := client.GetAllProducerInfo(ctx, brokerAddr)
	if err != nil {
		t.Logf("获取所有生产者信息失败: %v", err)
		return
	}

	t.Logf("生产者组数量: %d", len(producerInfo))
	for group, connections := range producerInfo {
		t.Logf("生产者组 %s: %d 个连接", group, len(connections))
		for _, conn := range connections {
			t.Logf("  客户端: %s, 地址: %s", conn.ClientId, conn.ClientAddr)
		}
	}
}
