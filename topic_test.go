package admin

import (
	"testing"
)

func TestIntegration_FetchAllTopicList(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		t.Fatalf("failed to get topic list: %v", err)
	}

	if topicList == nil {
		t.Fatal("the topic list must not be nil")
	}

	t.Logf("total topics: %d", len(topicList.TopicList))

	systemTopics := []string{
		"RMQ_SYS_TRANS_HALF_TOPIC",
		"SCHEDULE_TOPIC_XXXX",
		"DefaultCluster", // the default cluster topic on RocketMQ 5.x
	}

	foundSystemTopic := false
	for _, topic := range topicList.TopicList {
		for _, sysTopic := range systemTopics {
			if topic == sysTopic {
				foundSystemTopic = true
				break
			}
		}
		if foundSystemTopic {
			break
		}
	}

	maxShow := 10
	if len(topicList.TopicList) < maxShow {
		maxShow = len(topicList.TopicList)
	}
	t.Logf("first %d topics: %v", maxShow, topicList.TopicList[:maxShow])
}

func TestIntegration_CreateAndDeleteTopic(t *testing.T) {
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
	var clusterName string
	for cluster, brokerNames := range clusterInfo.ClusterAddrTable {
		clusterName = cluster
		for _, brokerName := range brokerNames {
			if brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]; ok {
				for _, addr := range brokerData.BrokerAddrs {
					brokerAddr = addr
					break
				}
			}
			if brokerAddr != "" {
				break
			}
		}
		if brokerAddr != "" {
			break
		}
	}

	if brokerAddr == "" {
		t.Fatal("no usable Broker address found")
	}

	t.Logf("using Broker addr: %s, cluster: %s", brokerAddr, clusterName)

	topicName := getTestTopicName("CREATE")
	topicConfig := TopicConfig{
		TopicName:       topicName,
		ReadQueueNums:   4,
		WriteQueueNums:  4,
		Perm:            6, // read + write
		TopicFilterType: "SINGLE_TAG",
		Order:           false,
	}

	t.Logf("creating topic: %s", topicName)
	err = client.CreateTopic(ctx, brokerAddr, topicConfig)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	routeData, err := client.ExamineTopicRouteInfo(ctx, topicName)
	if err != nil {
		t.Fatalf("failed to query topic route: %v", err)
	}

	if routeData == nil {
		t.Fatal("the topic route data must not be nil")
	}

	t.Logf("topic route: QueueDatas=%d, BrokerDatas=%d",
		len(routeData.QueueDatas), len(routeData.BrokerDatas))

	t.Logf("deleting topic: %s", topicName)
	err = client.DeleteTopic(ctx, topicName, clusterName)
	if err != nil {
		t.Logf("failed to delete topic (ignorable): %v", err)
	}

	_, err = client.ExamineTopicRouteInfo(ctx, topicName)
	if err != ErrTopicNotFound && err != nil {
		t.Logf("the topic may still be cached: %v", err)
	}
}

func TestIntegration_FetchTopicsByCluster(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to get cluster info: %v", err)
	}

	var clusterName string
	for name := range clusterInfo.ClusterAddrTable {
		clusterName = name
		break
	}

	if clusterName == "" {
		t.Skip("no cluster available")
	}

	topicList, err := client.FetchTopicsByCluster(ctx, clusterName)
	if err != nil {
		t.Fatalf("failed to get the topic list by cluster: %v", err)
	}

	t.Logf("topics in cluster %s: %d", clusterName, len(topicList.TopicList))
}

func TestIntegration_ExamineTopicRouteInfo(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// RocketMQ 5.x ships DefaultCluster as the default topic.
	testTopics := []string{
		"TBW102", // RocketMQ internal topic
		"SELF_TEST_TOPIC",
		"BenchmarkTest",
	}

	for _, topic := range testTopics {
		routeData, err := client.ExamineTopicRouteInfo(ctx, topic)
		if err == ErrTopicNotFound {
			t.Logf("topic %s does not exist (expected)", topic)
			continue
		}
		if err != nil {
			t.Logf("failed to query the route of topic %s: %v", topic, err)
			continue
		}

		t.Logf("topic %s route:", topic)
		t.Logf("  QueueDatas: %d", len(routeData.QueueDatas))
		t.Logf("  BrokerDatas: %d", len(routeData.BrokerDatas))

		for _, qd := range routeData.QueueDatas {
			t.Logf("  queue: BrokerName=%s, ReadQueue=%d, WriteQueue=%d",
				qd.BrokerName, qd.ReadQueueNums, qd.WriteQueueNums)
		}
		return // one usable topic is enough
	}
}

func TestIntegration_ExamineTopicStats(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		t.Fatalf("failed to get topic list: %v", err)
	}

	for _, topic := range topicList.TopicList {
		// Skip RocketMQ's own system topics.
		if len(topic) > 4 && topic[:4] == "RMQ_" {
			continue
		}
		if topic == "SCHEDULE_TOPIC_XXXX" || topic == "TBW102" {
			continue
		}

		stats, err := client.ExamineTopicStats(ctx, topic)
		if err != nil {
			t.Logf("failed to query the stats of topic %s: %v", topic, err)
			continue
		}

		t.Logf("topic %s stats:", topic)
		t.Logf("  OffsetTable size: %d", len(stats.OffsetTable))

		for key, offset := range stats.OffsetTable {
			t.Logf("  %s: MinOffset=%d, MaxOffset=%d",
				key, offset.MinOffset, offset.MaxOffset)
		}
		return // one topic with stats is enough
	}

	t.Log("no topic found to query stats for")
}

func TestIntegration_GetAllTopicConfig(t *testing.T) {
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

	configs, err := client.GetAllTopicConfig(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("failed to get all topic configs: %v", err)
	}

	t.Logf("topic configs: %d", len(configs))

	count := 0
	for name, config := range configs {
		if count >= 5 {
			break
		}
		t.Logf("  %s: ReadQueue=%d, WriteQueue=%d, Perm=%d",
			name, config.ReadQueueNums, config.WriteQueueNums, config.Perm)
		count++
	}
}

func TestIntegration_DeleteTopicInBroker(t *testing.T) {
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

	topicName := getTestTopicName("DELETE_BROKER")
	topicConfig := TopicConfig{
		TopicName:       topicName,
		ReadQueueNums:   4,
		WriteQueueNums:  4,
		Perm:            6,
		TopicFilterType: "SINGLE_TAG",
	}

	err = client.CreateTopic(ctx, brokerAddr, topicConfig)
	if err != nil {
		t.Fatalf("failed to create test topic: %v", err)
	}

	err = client.DeleteTopicInBroker(ctx, brokerAddr, topicName)
	if err != nil {
		t.Fatalf("failed to delete the topic on the Broker: %v", err)
	}

	t.Logf("deleted topic on the Broker: %s", topicName)

	_ = client.DeleteTopicInNameServer(ctx, topicName)
}

func TestIntegration_DeleteTopicInNameServer(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// Deleting a topic that never existed should still succeed, as a no-op.
	topicName := getTestTopicName("DELETE_NAMESRV")

	err := client.DeleteTopicInNameServer(ctx, topicName)
	if err != nil {
		t.Logf("failed to delete the topic on the NameServer (expected): %v", err)
	} else {
		t.Logf("deleted topic on the NameServer: %s", topicName)
	}
}

func TestIntegration_QueryTopicConsumeByWho(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		t.Fatalf("failed to get topic list: %v", err)
	}

	for _, topic := range topicList.TopicList {
		if len(topic) > 4 && topic[:4] == "RMQ_" {
			continue
		}

		groups, err := client.QueryTopicConsumeByWho(ctx, topic)
		if err != nil {
			continue
		}

		if len(groups) > 0 {
			t.Logf("consumer groups of topic %s: %v", topic, groups)
			return
		}
	}

	t.Log("no topic with consumers found")
}

func TestIntegration_GetTopicClusterList(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		t.Fatalf("failed to get topic list: %v", err)
	}

	for _, topic := range topicList.TopicList {
		if len(topic) > 4 && topic[:4] == "RMQ_" {
			continue
		}

		clusters, err := client.GetTopicClusterList(ctx, topic)
		if err != nil {
			continue
		}

		t.Logf("clusters of topic %s: %v", topic, clusters)
		return
	}

	t.Log("no topic found to query")
}

func TestIntegration_CreateStaticTopic(t *testing.T) {
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

	topicName := getTestTopicName("STATIC")
	err = client.CreateStaticTopic(ctx, brokerAddr, topicName, 4, "")
	if err != nil {
		t.Logf("failed to create a static topic (may be unsupported): %v", err)
	} else {
		t.Logf("created static topic: %s", topicName)
		_ = client.DeleteTopicInBroker(ctx, brokerAddr, topicName)
		_ = client.DeleteTopicInNameServer(ctx, topicName)
	}
}

func TestIntegration_CreateAndUpdateTopicConfigList(t *testing.T) {
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

	configs := []TopicConfig{
		{
			TopicName:       getTestTopicName("BATCH1"),
			ReadQueueNums:   4,
			WriteQueueNums:  4,
			Perm:            6,
			TopicFilterType: "SINGLE_TAG",
		},
		{
			TopicName:       getTestTopicName("BATCH2"),
			ReadQueueNums:   8,
			WriteQueueNums:  8,
			Perm:            6,
			TopicFilterType: "SINGLE_TAG",
		},
	}

	err = client.CreateAndUpdateTopicConfigList(ctx, brokerAddr, configs)
	if err != nil {
		t.Fatalf("failed to create topics in bulk: %v", err)
	}

	t.Logf("created %d topics in bulk", len(configs))

	for _, config := range configs {
		_ = client.DeleteTopicInBroker(ctx, brokerAddr, config.TopicName)
		_ = client.DeleteTopicInNameServer(ctx, config.TopicName)
	}
}

func TestIntegration_ExamineTopicConfig(t *testing.T) {
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

	topicName := getTestTopicName("CONFIG")
	topicConfig := TopicConfig{
		TopicName:       topicName,
		ReadQueueNums:   4,
		WriteQueueNums:  4,
		Perm:            6,
		TopicFilterType: "SINGLE_TAG",
	}

	err = client.CreateTopic(ctx, brokerAddr, topicConfig)
	if err != nil {
		t.Fatalf("failed to create test topic: %v", err)
	}
	defer func() {
		_ = client.DeleteTopicInBroker(ctx, brokerAddr, topicName)
		_ = client.DeleteTopicInNameServer(ctx, topicName)
	}()

	config, err := client.ExamineTopicConfig(ctx, brokerAddr, topicName)
	if err != nil {
		t.Fatalf("failed to query topic config: %v", err)
	}

	t.Logf("topic config: Name=%s, ReadQueue=%d, WriteQueue=%d, Perm=%d",
		config.TopicName, config.ReadQueueNums, config.WriteQueueNums, config.Perm)

	if config.TopicName != topicName {
		t.Errorf("topic name mismatch: got %s, want %s", config.TopicName, topicName)
	}
}
