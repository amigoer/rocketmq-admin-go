//go:build ignore
// +build ignore

// 消息操作示例
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	admin "github.com/codermast/rocketmq-admin-go"
)

func main() {
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
	topic := "TestTopic"

	// 1. 查询 Topic 路由（辅助验证）
	fmt.Printf("=== 查询 Topic 路由: %s ===\n", topic)
	_, err = client.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		log.Printf("Topic 可能不存在: %v\n", err)
		// 如果 Topic 不存在，后续操作可能会失败，这里仅做提示
	} else {
		fmt.Println("Topic 存在")
	}

	// 2. 按 Key 查询消息
	key := "Order-1001"
	fmt.Printf("\n=== 按 Key 查询消息: %s ===\n", key)
	beginTime := time.Now().Add(-24 * time.Hour).UnixMilli()
	endTime := time.Now().UnixMilli()
	msgs, err := client.QueryMessage(ctx, topic, key, 32, beginTime, endTime)
	if err != nil {
		log.Printf("查询失败: %v", err)
	} else {
		fmt.Printf("找到消息数: %d\n", len(msgs))
		for i, msg := range msgs {
			fmt.Printf("[%d] MsgId: %s, StoreTime: %d\n", i, msg.MsgId, msg.StoreTimestamp)
			// 记录一个 MsgId 用于后续查询详情
			if i == 0 {
				queryDetail(ctx, client, topic, msg.MsgId)
			}
		}
	}

	// 3. 查询消费队列
	fmt.Printf("\n=== 查询消费队列: %s ===\n", topic)
	// 假设 Broker 地址已知，实际应从 ClusterInfo 获取
	brokerAddr := "127.0.0.1:10911"
	qData, err := client.QueryConsumeQueue(ctx, brokerAddr, topic, 0, 0, 10, "DefaultGroup")
	if err != nil {
		log.Printf("查询消费队列失败: %v", err)
	} else {
		fmt.Printf("获取条目数: %d\n", len(qData))
	}
}

func queryDetail(ctx context.Context, client *admin.Client, topic, msgId string) {
	fmt.Printf("\n=== 查看消息详情: %s ===\n", msgId)
	msg, err := client.ViewMessage(ctx, topic, msgId)
	if err != nil {
		log.Printf("查看详情失败: %v", err)
	} else {
		fmt.Printf("Topic: %s\n", msg.Topic)
		fmt.Printf("QueueId: %d\n", msg.QueueId)
		fmt.Printf("QueueOffset: %d\n", msg.QueueOffset)
		fmt.Printf("BornHost: %s\n", msg.BornHost)
		fmt.Printf("Properties: %v\n", msg.Properties)
	}
}
