package admin

import (
	"testing"
)

func TestIntegration_CleanExpiredConsumerQueueByAddr(t *testing.T) {
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

	err = client.CleanExpiredConsumerQueueByAddr(ctx, brokerAddr)
	if err != nil {
		t.Logf("failed to clean expired consume queues: %v", err)
	} else {
		t.Log("cleaned expired consume queues")
	}
}

func TestIntegration_CleanExpiredConsumerQueue(t *testing.T) {
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

	err = client.CleanExpiredConsumerQueue(ctx, clusterName)
	if err != nil {
		t.Logf("failed to clean expired consume queues by cluster: %v", err)
	} else {
		t.Logf("cleaned expired consume queues on cluster: %s", clusterName)
	}
}

func TestIntegration_DeleteExpiredCommitLogByAddr(t *testing.T) {
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

	err = client.DeleteExpiredCommitLogByAddr(ctx, brokerAddr)
	if err != nil {
		t.Logf("failed to delete expired CommitLog files: %v", err)
	} else {
		t.Log("deleted expired CommitLog files")
	}
}

func TestIntegration_DeleteExpiredCommitLog(t *testing.T) {
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

	err = client.DeleteExpiredCommitLog(ctx, clusterName)
	if err != nil {
		t.Logf("failed to delete expired CommitLog files by cluster: %v", err)
	} else {
		t.Logf("deleted expired CommitLog files on cluster: %s", clusterName)
	}
}

func TestIntegration_CleanUnusedTopicByAddr(t *testing.T) {
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

	err = client.CleanUnusedTopicByAddr(ctx, brokerAddr)
	if err != nil {
		t.Logf("failed to clean unused topics: %v", err)
	} else {
		t.Log("cleaned unused topics")
	}
}

func TestIntegration_CleanUnusedTopic(t *testing.T) {
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

	err = client.CleanUnusedTopic(ctx, clusterName)
	if err != nil {
		t.Logf("failed to clean unused topics by cluster: %v", err)
	} else {
		t.Logf("cleaned unused topics on cluster: %s", clusterName)
	}
}

func TestIntegration_SetCommitLogReadAheadMode(t *testing.T) {
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

	err = client.SetCommitLogReadAheadMode(ctx, brokerAddr, 1)
	if err != nil {
		t.Logf("failed to set CommitLog read-ahead mode (may be unsupported): %v", err)
	} else {
		t.Log("set CommitLog read-ahead mode")
	}
}

func TestIntegration_SetCommitLogReadAheadModeInCluster(t *testing.T) {
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

	err = client.SetCommitLogReadAheadModeInCluster(ctx, clusterName, 1)
	if err != nil {
		t.Logf("failed to set CommitLog read-ahead mode by cluster: %v", err)
	} else {
		t.Logf("set CommitLog read-ahead mode on cluster: %s", clusterName)
	}
}

func TestIntegration_ExportRocksDBConfigToJson(t *testing.T) {
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

	configJson, err := client.ExportRocksDBConfigToJson(ctx, brokerAddr)
	if err != nil {
		t.Logf("failed to export RocksDB config (the Broker may not use RocksDB): %v", err)
		return
	}

	t.Logf("RocksDB config JSON length: %d", len(configJson))
	if len(configJson) > 200 {
		t.Logf("RocksDB config (truncated): %s...", configJson[:200])
	} else {
		t.Logf("RocksDB config: %s", configJson)
	}
}

func TestIntegration_CheckRocksdbCqWriteProgress(t *testing.T) {
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

	progress, err := client.CheckRocksdbCqWriteProgress(ctx, brokerAddr, testTopic)
	if err != nil {
		t.Logf("failed to check RocksDB CQ write progress (may be unsupported): %v", err)
		return
	}

	t.Logf("RocksDB CQ write progress entries: %d", len(progress))
	for _, p := range progress {
		t.Logf("  Topic=%s, QueueId=%d, Progress=%.2f%%, IsCompleted=%v",
			p.Topic, p.QueueId, p.Progress, p.IsCompleted)
	}
}
