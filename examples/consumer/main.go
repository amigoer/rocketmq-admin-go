//go:build ignore
// +build ignore

// 消费者管理示例
package main

import (
	"context"
	"fmt"
	"log"

	admin "github.com/codermast/rocketmq-admin-go"
)

func main() {
	// 创建管理客户端
	client, err := admin.NewClient(
		admin.WithNameServers([]string{"127.0.0.1:9876"}),
	)
	if err != nil {
		log.Fatalf("创建客户端失败: %v", err)
	}

	if err := client.Start(); err != nil {
		log.Fatalf("启动客户端失败: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// 示例 1: 创建订阅组
	fmt.Println("=== 创建订阅组 ===")
	groupConfig := admin.SubscriptionGroupConfig{
		GroupName:          "TestConsumerGroup",
		ConsumeEnable:      true,
		RetryQueueNums:     1,
		RetryMaxTimes:      16,
	}
	if err := client.CreateSubscriptionGroup(ctx, "127.0.0.1:10911", groupConfig); err != nil {
		log.Printf("创建订阅组失败: %v", err)
	} else {
		fmt.Println("订阅组创建成功")
	}

	// 示例 2: 查询消费统计
	fmt.Println("\n=== 查询消费统计 ===")
	consumeStats, err := client.ExamineConsumeStats(ctx, "TestConsumerGroup")
	if err != nil {
		log.Printf("查询消费统计失败: %v", err)
	} else {
		fmt.Printf("消费 TPS: %.2f\n", consumeStats.ConsumeTps)
		fmt.Printf("队列数量: %d\n", len(consumeStats.OffsetTable))
	}

	// 示例 3: 查询消费者连接
	fmt.Println("\n=== 查询消费者连接 ===")
	connInfo, err := client.ExamineConsumerConnectionInfo(ctx, "TestConsumerGroup")
	if err != nil {
		log.Printf("查询消费者连接失败: %v", err)
	} else {
		fmt.Printf("连接数: %d\n", len(connInfo.ConnectionSet))
		fmt.Printf("消费类型: %s\n", connInfo.ConsumeType)
		fmt.Printf("消息模型: %s\n", connInfo.MessageModel)
	}

	// 示例 4: 重置消费位点
	fmt.Println("\n=== 重置消费位点 ===")
	// 重置到 1 小时前
	timestamp := (time.Now().Unix() - 3600) * 1000
	offsets, err := client.ResetOffsetByTimestamp(ctx, "TestTopic", "TestConsumerGroup", timestamp, false)
	if err != nil {
		log.Printf("重置消费位点失败: %v", err)
	} else {
		fmt.Printf("重置队列数: %d\n", len(offsets))
	}
}

