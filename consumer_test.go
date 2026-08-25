package admin

import (
	"testing"
)

func TestIntegration_CreateAndDeleteSubscriptionGroup(t *testing.T) {
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

	t.Logf("created subscription group: %s", groupName)
	err = client.CreateSubscriptionGroup(ctx, brokerAddr, config)
	if err != nil {
		t.Fatalf("failed to create subscription group: %v", err)
	}

	groupConfig, err := client.ExamineSubscriptionGroupConfig(ctx, brokerAddr, groupName)
	if err != nil {
		t.Logf("failed to query subscription group config (the call may be unsupported): %v", err)
	} else {
		t.Logf("subscription group config: GroupName=%s, ConsumeEnable=%v",
			groupConfig.GroupName, groupConfig.ConsumeEnable)
	}

	t.Logf("deleting subscription group: %s", groupName)
	err = client.DeleteSubscriptionGroup(ctx, brokerAddr, groupName)
	if err != nil {
		t.Fatalf("failed to delete subscription group: %v", err)
	}
}

func TestIntegration_GetAllSubscriptionGroup(t *testing.T) {
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

	groups, err := client.GetAllSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("failed to get all subscription groups: %v", err)
	}

	t.Logf("subscription groups: %d", len(groups))

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

func TestIntegration_GetUserSubscriptionGroup(t *testing.T) {
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

	userGroups, err := client.GetUserSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("failed to get user subscription groups: %v", err)
	}

	t.Logf("user subscription groups: %d", len(userGroups))

	for name := range userGroups {
		t.Logf("  user subscription group: %s", name)
	}
}

func TestIntegration_ExamineConsumeStats(t *testing.T) {
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

	groups, err := client.GetAllSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("failed to get subscription groups: %v", err)
	}

	for groupName := range groups {
		stats, err := client.ExamineConsumeStats(ctx, groupName)
		if err != nil {
			continue
		}

		t.Logf("consumer group %s stats:", groupName)
		t.Logf("  ConsumeTps: %.2f", stats.ConsumeTps)
		t.Logf("  OffsetTable size: %d", len(stats.OffsetTable))

		for key, wrapper := range stats.OffsetTable {
			t.Logf("  %s: BrokerOffset=%d, ConsumerOffset=%d",
				key, wrapper.BrokerOffset, wrapper.ConsumerOffset)
		}
		return
	}

	t.Log("no consumer group available to query stats for")
}

func TestIntegration_ExamineConsumerConnectionInfo(t *testing.T) {
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

	groups, err := client.GetAllSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("failed to get subscription groups: %v", err)
	}

	for groupName := range groups {
		connInfo, err := client.ExamineConsumerConnectionInfo(ctx, groupName)
		if err == ErrConsumerGroupNotFound {
			continue
		}
		if err != nil {
			continue
		}

		t.Logf("consumer group %s connections:", groupName)
		t.Logf("  consume type: %s", connInfo.ConsumeType)
		t.Logf("  message model: %s", connInfo.MessageModel)
		t.Logf("  connections: %d", len(connInfo.ConnectionSet))

		for _, conn := range connInfo.ConnectionSet {
			t.Logf("  client: %s, addr: %s", conn.ClientId, conn.ClientAddr)
		}
		return
	}

	t.Log("no consumer online")
}

func TestIntegration_QueryTopicsByConsumer(t *testing.T) {
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

	groups, err := client.GetAllSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("failed to get subscription groups: %v", err)
	}

	for groupName := range groups {
		topicList, err := client.QueryTopicsByConsumer(ctx, groupName)
		if err != nil {
			continue
		}

		if len(topicList.TopicList) > 0 {
			t.Logf("topics subscribed by consumer group %s: %v", groupName, topicList.TopicList)
			return
		}
	}

	t.Log("no consumer group with subscribed topics found")
}

func TestIntegration_UpdateConsumeOffset(t *testing.T) {
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

	groupName := getTestGroupName("OFFSET")
	config := SubscriptionGroupConfig{
		GroupName:     groupName,
		ConsumeEnable: true,
	}

	err = client.CreateSubscriptionGroup(ctx, brokerAddr, config)
	if err != nil {
		t.Fatalf("failed to create subscription group: %v", err)
	}
	defer func() {
		_ = client.DeleteSubscriptionGroup(ctx, brokerAddr, groupName)
	}()

	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		t.Fatalf("failed to get topic list: %v", err)
	}

	var testTopic string
	for _, topic := range topicList.TopicList {
		if len(topic) < 4 || topic[:4] != "RMQ_" {
			testTopic = topic
			break
		}
	}

	if testTopic == "" {
		t.Skip("no usable test topic")
	}

	err = client.UpdateConsumeOffset(ctx, brokerAddr, groupName, testTopic, 0, 100)
	if err != nil {
		t.Logf("failed to update consume offset (the topic may not exist on this Broker): %v", err)
	} else {
		t.Logf("updated consume offset")
	}
}

func TestIntegration_ResetOffsetByTimestamp(t *testing.T) {
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

	groups, err := client.GetAllSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("failed to get subscription groups: %v", err)
	}

	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		t.Fatalf("failed to get topic list: %v", err)
	}

	var testTopic string
	for _, topic := range topicList.TopicList {
		if len(topic) < 4 || topic[:4] != "RMQ_" {
			testTopic = topic
			break
		}
	}

	if testTopic == "" {
		t.Skip("no usable test topic")
	}

	for groupName := range groups {
		result, err := client.ResetOffsetByTimestamp(ctx, testTopic, groupName, 0, false)
		if err != nil {
			continue
		}

		t.Logf("reset offsets for consumer group %s: %d queues", groupName, len(result))
		return
	}

	t.Log("no consumer group available to reset offsets for")
}

func TestIntegration_QueryConsumeTimeSpan(t *testing.T) {
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

	groups, err := client.GetAllSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("failed to get subscription groups: %v", err)
	}

	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		t.Fatalf("failed to get topic list: %v", err)
	}

	var testTopic string
	for _, topic := range topicList.TopicList {
		if len(topic) < 4 || topic[:4] != "RMQ_" {
			testTopic = topic
			break
		}
	}

	if testTopic == "" {
		t.Skip("no usable test topic")
	}

	for groupName := range groups {
		spans, err := client.QueryConsumeTimeSpan(ctx, testTopic, groupName)
		if err != nil {
			continue
		}

		if len(spans) > 0 {
			t.Logf("consume time spans for group %s: %d", groupName, len(spans))
			for _, span := range spans {
				t.Logf("  MinTimestamp=%d, MaxTimestamp=%d",
					span.MinTimeStamp, span.MaxTimeStamp)
			}
			return
		}
	}

	t.Log("no consume time span data found")
}

func TestIntegration_GetConsumerRunningInfo(t *testing.T) {
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

	groups, err := client.GetAllSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		t.Fatalf("failed to get subscription groups: %v", err)
	}

	for groupName := range groups {
		runningInfo, err := client.GetConsumerRunningInfo(ctx, groupName, "", false)
		if err == ErrConsumerGroupNotFound {
			continue
		}
		if err != nil {
			continue
		}

		t.Logf("consumer group %s runtime info:", groupName)
		t.Logf("  JStack length: %d", len(runningInfo.JStack))
		return
	}

	t.Log("no consumer online")
}

func TestIntegration_ColdDataFlowCtr(t *testing.T) {
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

	infos, err := client.GetColdDataFlowCtrInfo(ctx, brokerAddr)
	if err != nil {
		t.Logf("failed to get cold data flow control info (may be unsupported): %v", err)
		return
	}

	t.Logf("cold data flow control entries: %d", len(infos))
	for _, info := range infos {
		t.Logf("  group=%s, CurrentQPS=%d, IsEnabled=%v",
			info.ConsumerGroup, info.CurrentQPS, info.IsFlowCtrEnabled)
	}
}

func TestIntegration_UpdateColdDataFlowCtrGroupConfig(t *testing.T) {
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

	groupName := getTestGroupName("COLDDATA")
	subConfig := SubscriptionGroupConfig{
		GroupName:     groupName,
		ConsumeEnable: true,
	}

	err = client.CreateSubscriptionGroup(ctx, brokerAddr, subConfig)
	if err != nil {
		t.Fatalf("failed to create subscription group: %v", err)
	}
	defer func() {
		_ = client.DeleteSubscriptionGroup(ctx, brokerAddr, groupName)
	}()

	config := ColdDataFlowCtrConfig{
		ConsumerGroup:   groupName,
		ThresholdPerSec: 1000,
		GlobalThreshold: 10000,
		EnableFlowCtr:   true,
	}

	err = client.UpdateColdDataFlowCtrGroupConfig(ctx, brokerAddr, config)
	if err != nil {
		t.Logf("failed to update cold data flow control config (may be unsupported): %v", err)
		return
	}

	t.Logf("updated cold data flow control config")

	err = client.RemoveColdDataFlowCtrGroupConfig(ctx, brokerAddr, groupName)
	if err != nil {
		t.Logf("failed to remove cold data flow control config: %v", err)
	}
}

func TestIntegration_CloneGroupOffset(t *testing.T) {
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

	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		t.Fatalf("failed to get topic list: %v", err)
	}

	var testTopic string
	for _, topic := range topicList.TopicList {
		if len(topic) < 4 || topic[:4] != "RMQ_" {
			testTopic = topic
			break
		}
	}

	if testTopic == "" {
		t.Skip("no usable test topic")
	}

	err = client.CloneGroupOffset(ctx, srcGroup, destGroup, testTopic, false)
	if err != nil {
		t.Logf("failed to clone offsets (the source group may have consumed nothing): %v", err)
	} else {
		t.Log("cloned offsets")
	}
}

func TestIntegration_UpdateAndGetGroupReadForbidden(t *testing.T) {
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

	groupName := getTestGroupName("FORBID")
	config := SubscriptionGroupConfig{GroupName: groupName, ConsumeEnable: true}

	err = client.CreateSubscriptionGroup(ctx, brokerAddr, config)
	if err != nil {
		t.Fatalf("failed to create subscription group: %v", err)
	}
	defer func() {
		_ = client.DeleteSubscriptionGroup(ctx, brokerAddr, groupName)
	}()

	forbidden, err := client.UpdateAndGetGroupReadForbidden(ctx, brokerAddr, groupName, "", true)
	if err != nil {
		t.Logf("failed to update the read-forbidden flag: %v", err)
	} else {
		t.Logf("read-forbidden flag: %v", forbidden)
	}

	_, err = client.UpdateAndGetGroupReadForbidden(ctx, brokerAddr, groupName, "", false)
	if err != nil {
		t.Logf("failed to restore the read flag: %v", err)
	}
}
