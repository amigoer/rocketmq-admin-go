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
		t.Fatalf("获取 Topic 列表失败: %v", err)
	}

	if topicList == nil {
		t.Fatal("Topic 列表不应为 nil")
	}

	t.Logf("Topic 总数: %d", len(topicList.TopicList))

	systemTopics := []string{
		"RMQ_SYS_TRANS_HALF_TOPIC",
		"SCHEDULE_TOPIC_XXXX",
		"DefaultCluster", // RocketMQ 5.x 中的默认集群 Topic
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
	t.Logf("前 %d 个 Topic: %v", maxShow, topicList.TopicList[:maxShow])
}

func TestIntegration_CreateAndDeleteTopic(t *testing.T) {
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
		t.Fatal("未找到可用的 Broker 地址")
	}

	t.Logf("使用 Broker 地址: %s, 集群: %s", brokerAddr, clusterName)

	topicName := getTestTopicName("CREATE")
	topicConfig := TopicConfig{
		TopicName:       topicName,
		ReadQueueNums:   4,
		WriteQueueNums:  4,
		Perm:            6, // read + write
		TopicFilterType: "SINGLE_TAG",
		Order:           false,
	}

	t.Logf("创建 Topic: %s", topicName)
	err = client.CreateTopic(ctx, brokerAddr, topicConfig)
	if err != nil {
		t.Fatalf("创建 Topic 失败: %v", err)
	}

	routeData, err := client.ExamineTopicRouteInfo(ctx, topicName)
	if err != nil {
		t.Fatalf("查询 Topic 路由失败: %v", err)
	}

	if routeData == nil {
		t.Fatal("Topic 路由数据不应为 nil")
	}

	t.Logf("Topic 路由: QueueDatas=%d, BrokerDatas=%d",
		len(routeData.QueueDatas), len(routeData.BrokerDatas))

	t.Logf("删除 Topic: %s", topicName)
	err = client.DeleteTopic(ctx, topicName, clusterName)
	if err != nil {
		t.Logf("删除 Topic 失败（可忽略）: %v", err)
	}

	_, err = client.ExamineTopicRouteInfo(ctx, topicName)
	if err != ErrTopicNotFound && err != nil {
		t.Logf("Topic 可能仍在缓存中: %v", err)
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

	topicList, err := client.FetchTopicsByCluster(ctx, clusterName)
	if err != nil {
		t.Fatalf("按集群获取 Topic 列表失败: %v", err)
	}

	t.Logf("集群 %s 的 Topic 数量: %d", clusterName, len(topicList.TopicList))
}

func TestIntegration_ExamineTopicRouteInfo(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// RocketMQ 5.x ships DefaultCluster as the default topic.
	testTopics := []string{
		"TBW102", // 系统内部 Topic
		"SELF_TEST_TOPIC",
		"BenchmarkTest",
	}

	for _, topic := range testTopics {
		routeData, err := client.ExamineTopicRouteInfo(ctx, topic)
		if err == ErrTopicNotFound {
			t.Logf("Topic %s 不存在（正常）", topic)
			continue
		}
		if err != nil {
			t.Logf("查询 Topic %s 路由失败: %v", topic, err)
			continue
		}

		t.Logf("Topic %s 路由信息:", topic)
		t.Logf("  QueueDatas 数量: %d", len(routeData.QueueDatas))
		t.Logf("  BrokerDatas 数量: %d", len(routeData.BrokerDatas))

		for _, qd := range routeData.QueueDatas {
			t.Logf("  队列: BrokerName=%s, ReadQueue=%d, WriteQueue=%d",
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
		t.Fatalf("获取 Topic 列表失败: %v", err)
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
			t.Logf("查询 Topic %s 统计失败: %v", topic, err)
			continue
		}

		t.Logf("Topic %s 统计信息:", topic)
		t.Logf("  OffsetTable 大小: %d", len(stats.OffsetTable))

		for key, offset := range stats.OffsetTable {
			t.Logf("  %s: MinOffset=%d, MaxOffset=%d",
				key, offset.MinOffset, offset.MaxOffset)
		}
		return // one topic with stats is enough
	}

	t.Log("没有找到可查询统计的 Topic")
}

func TestIntegration_GetAllTopicConfig(t *testing.T) {
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

	configs, err := client.GetAllTopicConfig(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("获取所有 Topic 配置失败: %v", err)
	}

	t.Logf("Topic 配置数量: %d", len(configs))

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
		t.Fatalf("创建测试 Topic 失败: %v", err)
	}

	err = client.DeleteTopicInBroker(ctx, brokerAddr, topicName)
	if err != nil {
		t.Fatalf("在 Broker 中删除 Topic 失败: %v", err)
	}

	t.Logf("成功在 Broker 删除 Topic: %s", topicName)

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
		t.Logf("在 NameServer 中删除 Topic 失败（正常现象）: %v", err)
	} else {
		t.Logf("在 NameServer 中删除 Topic 成功: %s", topicName)
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
		t.Fatalf("获取 Topic 列表失败: %v", err)
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
			t.Logf("Topic %s 的消费者组: %v", topic, groups)
			return
		}
	}

	t.Log("没有找到有消费者的 Topic")
}

func TestIntegration_GetTopicClusterList(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		t.Fatalf("获取 Topic 列表失败: %v", err)
	}

	for _, topic := range topicList.TopicList {
		if len(topic) > 4 && topic[:4] == "RMQ_" {
			continue
		}

		clusters, err := client.GetTopicClusterList(ctx, topic)
		if err != nil {
			continue
		}

		t.Logf("Topic %s 所属集群: %v", topic, clusters)
		return
	}

	t.Log("没有找到可查询的 Topic")
}

func TestIntegration_CreateStaticTopic(t *testing.T) {
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

	topicName := getTestTopicName("STATIC")
	err = client.CreateStaticTopic(ctx, brokerAddr, topicName, 4, "")
	if err != nil {
		t.Logf("创建静态 Topic 失败（可能不支持）: %v", err)
	} else {
		t.Logf("创建静态 Topic 成功: %s", topicName)
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
		t.Fatalf("批量创建 Topic 失败: %v", err)
	}

	t.Logf("批量创建 Topic 成功: %d 个", len(configs))

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
		t.Fatalf("创建测试 Topic 失败: %v", err)
	}
	defer func() {
		_ = client.DeleteTopicInBroker(ctx, brokerAddr, topicName)
		_ = client.DeleteTopicInNameServer(ctx, topicName)
	}()

	config, err := client.ExamineTopicConfig(ctx, brokerAddr, topicName)
	if err != nil {
		t.Fatalf("查询 Topic 配置失败: %v", err)
	}

	t.Logf("Topic 配置: Name=%s, ReadQueue=%d, WriteQueue=%d, Perm=%d",
		config.TopicName, config.ReadQueueNums, config.WriteQueueNums, config.Perm)

	if config.TopicName != topicName {
		t.Errorf("Topic 名称不匹配: got %s, want %s", config.TopicName, topicName)
	}
}
