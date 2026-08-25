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
		log.Fatalf("failed to create client: %v", err)
	}

	if err := client.Start(); err != nil {
		log.Fatalf("failed to start client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	fmt.Println("=== create topic ===")
	topicConfig := admin.TopicConfig{
		TopicName:      "TestTopic",
		ReadQueueNums:  8,
		WriteQueueNums: 8,
		Perm:           6, // read + write
	}
	if err := client.CreateTopic(ctx, "127.0.0.1:10911", topicConfig); err != nil {
		log.Printf("failed to create topic: %v", err)
	} else {
		fmt.Println("topic created")
	}

	fmt.Println("\n=== query topic route ===")
	routeData, err := client.ExamineTopicRouteInfo(ctx, "TestTopic")
	if err != nil {
		log.Printf("failed to query topic route: %v", err)
	} else {
		fmt.Printf("Brokers: %d\n", len(routeData.BrokerDatas))
		fmt.Printf("queues: %d\n", len(routeData.QueueDatas))
	}

	fmt.Println("\n=== query topic stats ===")
	stats, err := client.ExamineTopicStats(ctx, "TestTopic")
	if err != nil {
		log.Printf("failed to query topic stats: %v", err)
	} else {
		fmt.Printf("message queues: %d\n", len(stats.OffsetTable))
	}

	fmt.Println("\n=== delete topic ===")
	if err := client.DeleteTopic(ctx, "TestTopic", "DefaultCluster"); err != nil {
		log.Printf("failed to delete topic: %v", err)
	} else {
		fmt.Println("topic deleted")
	}
}
