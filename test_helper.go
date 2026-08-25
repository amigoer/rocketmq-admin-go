package admin

import (
	"context"
	"os"
	"testing"
	"time"
)

const (
	testNameServerAddr = "localhost:9876"
	testTimeout        = 10 * time.Second
	testTopicPrefix    = "TEST_TOPIC_"
	testGroupPrefix    = "TEST_GROUP_"
)

// getTestNameServer returns ROCKETMQ_NAMESRV_ADDR, or the default address.
func getTestNameServer() string {
	if addr := os.Getenv("ROCKETMQ_NAMESRV_ADDR"); addr != "" {
		return addr
	}
	return testNameServerAddr
}

// getTestClient creates and starts a client, failing the test if it cannot.
func getTestClient(t *testing.T) *Client {
	client, err := NewClient(
		WithNameServers([]string{getTestNameServer()}),
		WithTimeout(testTimeout),
	)
	if err != nil {
		t.Fatalf("创建测试客户端失败: %v", err)
	}

	if err := client.Start(); err != nil {
		t.Fatalf("启动测试客户端失败: %v", err)
	}

	return client
}

// skipIfNoRocketMQ skips the test unless a reachable RocketMQ cluster is configured.
func skipIfNoRocketMQ(t *testing.T) {
	if os.Getenv("ROCKETMQ_TEST_SKIP") == "true" {
		t.Skip("跳过 RocketMQ 集成测试 (ROCKETMQ_TEST_SKIP=true)")
	}

	client, err := NewClient(
		WithNameServers([]string{getTestNameServer()}),
		WithTimeout(3*time.Second),
	)
	if err != nil {
		t.Skipf("跳过测试: 无法创建客户端: %v", err)
	}
	defer client.Close()

	if err := client.Start(); err != nil {
		t.Skipf("跳过测试: 无法启动客户端: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// FetchAllTopicList is a more reliable liveness probe than ExamineBrokerClusterInfo.
	_, err = client.FetchAllTopicList(ctx)
	if err != nil {
		t.Skipf("跳过测试: RocketMQ 不可用: %v", err)
	}
}

// getTestTopicName builds a unique topic name from suffix and the current time.
func getTestTopicName(suffix string) string {
	return testTopicPrefix + suffix + "_" + time.Now().Format("20060102150405")
}

// getTestGroupName builds a unique group name from suffix and the current time.
func getTestGroupName(suffix string) string {
	return testGroupPrefix + suffix + "_" + time.Now().Format("20060102150405")
}

// testContext returns a context bounded by testTimeout.
func testContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), testTimeout)
}
