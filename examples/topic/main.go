//go:build ignore
// +build ignore

// Topic 管理示例
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

	// 示例 1: 创建 Topic
	fmt.Println("=== 创建 Topic ===")
	topicConfig := admin.TopicConfig{
		TopicName:      "TestTopic",
		ReadQueueNums:  8,
		WriteQueueNums: 8,
		Perm:           6, // 读写权限
	}
	if err := client.CreateTopic(ctx, "127.0.0.1:10911", topicConfig); err != nil {
		log.Printf("创建 Topic 失败: %v", err)
	} else {
		fmt.Println("Topic 创建成功")
	}

	// 示例 2: 查询 Topic 路由信息
	fmt.Println("\n=== 查询 Topic 路由 ===")
	routeData, err := client.ExamineTopicRouteInfo(ctx, "TestTopic")
	if err != nil {
		log.Printf("查询 Topic 路由失败: %v", err)
	} else {
		fmt.Printf("Broker 数量: %d\n", len(routeData.BrokerDatas))
		fmt.Printf("队列数量: %d\n", len(routeData.QueueDatas))
	}

	// 示例 3: 查询 Topic 统计
	fmt.Println("\n=== 查询 Topic 统计 ===")
	stats, err := client.ExamineTopicStats(ctx, "TestTopic")
	if err != nil {
		log.Printf("查询 Topic 统计失败: %v", err)
	} else {
		fmt.Printf("消息队列数量: %d\n", len(stats.OffsetTable))
	}

	// 示例 4: 删除 Topic
	fmt.Println("\n=== 删除 Topic ===")
	if err := client.DeleteTopic(ctx, "TestTopic", "DefaultCluster"); err != nil {
		log.Printf("删除 Topic 失败: %v", err)
	} else {
		fmt.Println("Topic 删除成功")
	}
}

