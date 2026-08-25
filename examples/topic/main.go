//go:build ignore
// +build ignore

// Example: creating, inspecting and deleting a topic.
package main

import (
	"context"
	"fmt"
	"log"

	admin "github.com/amigoer/rocketmq-admin-go"
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

	fmt.Println("=== 创建 Topic ===")
	topicConfig := admin.TopicConfig{
		TopicName:      "TestTopic",
		ReadQueueNums:  8,
		WriteQueueNums: 8,
		Perm:           6, // read + write
	}
	if err := client.CreateTopic(ctx, "127.0.0.1:10911", topicConfig); err != nil {
		log.Printf("创建 Topic 失败: %v", err)
	} else {
		fmt.Println("Topic 创建成功")
	}

	fmt.Println("\n=== 查询 Topic 路由 ===")
	routeData, err := client.ExamineTopicRouteInfo(ctx, "TestTopic")
	if err != nil {
		log.Printf("查询 Topic 路由失败: %v", err)
	} else {
		fmt.Printf("Broker 数量: %d\n", len(routeData.BrokerDatas))
		fmt.Printf("队列数量: %d\n", len(routeData.QueueDatas))
	}

	fmt.Println("\n=== 查询 Topic 统计 ===")
	stats, err := client.ExamineTopicStats(ctx, "TestTopic")
	if err != nil {
		log.Printf("查询 Topic 统计失败: %v", err)
	} else {
		fmt.Printf("消息队列数量: %d\n", len(stats.OffsetTable))
	}

	fmt.Println("\n=== 删除 Topic ===")
	if err := client.DeleteTopic(ctx, "TestTopic", "DefaultCluster"); err != nil {
		log.Printf("删除 Topic 失败: %v", err)
	} else {
		fmt.Println("Topic 删除成功")
	}
}
