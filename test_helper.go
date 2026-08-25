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
		t.Fatalf("failed to create test client: %v", err)
	}

	if err := client.Start(); err != nil {
		t.Fatalf("failed to start test client: %v", err)
	}

	return client
}

// skipIfNoRocketMQ skips the test unless a reachable RocketMQ cluster is configured.
func skipIfNoRocketMQ(t *testing.T) {
	if os.Getenv("ROCKETMQ_TEST_SKIP") == "true" {
		t.Skip("skipping RocketMQ integration tests (ROCKETMQ_TEST_SKIP=true)")
	}

	client, err := NewClient(
		WithNameServers([]string{getTestNameServer()}),
		WithTimeout(3*time.Second),
	)
	if err != nil {
		t.Skipf("skipping: cannot create a client: %v", err)
	}
	defer client.Close()

	if err := client.Start(); err != nil {
		t.Skipf("skipping: cannot start the client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// FetchAllTopicList is a more reliable liveness probe than ExamineBrokerClusterInfo.
	_, err = client.FetchAllTopicList(ctx)
	if err != nil {
		t.Skipf("skipping: RocketMQ is not reachable: %v", err)
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
