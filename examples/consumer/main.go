//go:build ignore
// +build ignore

// Example: managing consumer groups and offsets.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

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

	fmt.Println("=== create subscription group ===")
	groupConfig := admin.SubscriptionGroupConfig{
		GroupName:      "TestConsumerGroup",
		ConsumeEnable:  true,
		RetryQueueNums: 1,
		RetryMaxTimes:  16,
	}
	if err := client.CreateSubscriptionGroup(ctx, "127.0.0.1:10911", groupConfig); err != nil {
		log.Printf("failed to create subscription group: %v", err)
	} else {
		fmt.Println("subscription group created")
	}

	fmt.Println("\n=== query consume stats ===")
	consumeStats, err := client.ExamineConsumeStats(ctx, "TestConsumerGroup")
	if err != nil {
		log.Printf("failed to query consume stats: %v", err)
	} else {
		fmt.Printf("consume TPS: %.2f\n", consumeStats.ConsumeTps)
		fmt.Printf("queues: %d\n", len(consumeStats.OffsetTable))
	}

	fmt.Println("\n=== query consumer connections ===")
	connInfo, err := client.ExamineConsumerConnectionInfo(ctx, "TestConsumerGroup")
	if err != nil {
		log.Printf("failed to query consumer connections: %v", err)
	} else {
		fmt.Printf("connections: %d\n", len(connInfo.ConnectionSet))
		fmt.Printf("consume type: %s\n", connInfo.ConsumeType)
		fmt.Printf("message model: %s\n", connInfo.MessageModel)
	}

	fmt.Println("\n=== reset consume offsets ===")
	// One hour ago, in Unix milliseconds.
	timestamp := (time.Now().Unix() - 3600) * 1000
	offsets, err := client.ResetOffsetByTimestamp(ctx, "TestTopic", "TestConsumerGroup", timestamp, false)
	if err != nil {
		log.Printf("failed to reset consume offsets: %v", err)
	} else {
		fmt.Printf("queues reset: %d\n", len(offsets))
	}
}
