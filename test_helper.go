package admin

import (
	"context"
	"os"
	"testing"
	"time"
)

// =============================================================================
// 测试配置常量
// =============================================================================

const (
	// 默认 NameServer 地址
	testNameServerAddr = "localhost:9876"
	// 默认 Broker 地址
	testBrokerAddr = "localhost:10911"
	// 测试超时时间
	testTimeout = 10 * time.Second
	// 测试 Topic 前缀
	testTopicPrefix = "TEST_TOPIC_"
	// 测试消费组前缀
	testGroupPrefix = "TEST_GROUP_"
)

// =============================================================================
// 测试辅助函数
// =============================================================================

// getTestNameServer 获取测试用 NameServer 地址
// 优先使用环境变量 ROCKETMQ_NAMESRV_ADDR
func getTestNameServer() string {
	if addr := os.Getenv("ROCKETMQ_NAMESRV_ADDR"); addr != "" {
		return addr
	}
	return testNameServerAddr
}

// getTestBrokerAddr 获取测试用 Broker 地址
// 优先使用环境变量 ROCKETMQ_BROKER_ADDR
func getTestBrokerAddr() string {
	if addr := os.Getenv("ROCKETMQ_BROKER_ADDR"); addr != "" {
		return addr
	}
	return testBrokerAddr
}

// getTestClient 创建测试客户端
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

// skipIfNoRocketMQ 如果 RocketMQ 不可用则跳过测试
func skipIfNoRocketMQ(t *testing.T) {
	if os.Getenv("ROCKETMQ_TEST_SKIP") == "true" {
		t.Skip("跳过 RocketMQ 集成测试 (ROCKETMQ_TEST_SKIP=true)")
	}

	// 尝试连接 NameServer 验证可用性
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

	// 使用 FetchAllTopicList 检测可用性，比 ExamineBrokerClusterInfo 更可靠
	_, err = client.FetchAllTopicList(ctx)
	if err != nil {
		t.Skipf("跳过测试: RocketMQ 不可用: %v", err)
	}
}

// getTestTopicName 生成测试 Topic 名称
func getTestTopicName(suffix string) string {
	return testTopicPrefix + suffix + "_" + time.Now().Format("20060102150405")
}

// getTestGroupName 生成测试消费组名称
func getTestGroupName(suffix string) string {
	return testGroupPrefix + suffix + "_" + time.Now().Format("20060102150405")
}

// testContext 创建带超时的测试上下文
func testContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), testTimeout)
}
