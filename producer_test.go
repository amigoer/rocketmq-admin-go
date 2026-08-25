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

	producerGroup := "DEFAULT_PRODUCER"
	connInfo, err := client.ExamineProducerConnectionInfo(ctx, producerGroup, testTopic)
	if err != nil {
		t.Logf("failed to query producer connections (there may be no online producer): %v", err)
		return
	}

	t.Logf("producer group %s connections:", producerGroup)
	t.Logf("  connections: %d", len(connInfo.ConnectionSet))
	for _, conn := range connInfo.ConnectionSet {
		t.Logf("  client: %s, addr: %s", conn.ClientId, conn.ClientAddr)
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

	producerInfo, err := client.GetAllProducerInfo(ctx, brokerAddr)
	if err != nil {
		t.Logf("failed to get all producer info: %v", err)
		return
	}

	t.Logf("producer groups: %d", len(producerInfo))
	for group, connections := range producerInfo {
		t.Logf("producer group %s: %d connections", group, len(connections))
		for _, conn := range connections {
			t.Logf("  client: %s, addr: %s", conn.ClientId, conn.ClientAddr)
		}
	}
}
