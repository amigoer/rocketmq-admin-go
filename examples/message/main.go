//go:build ignore
// +build ignore

// Example: querying and inspecting messages.
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
	topic := "TestTopic"

	fmt.Printf("=== query topic route: %s ===\n", topic)
	_, err = client.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		log.Printf("the topic may not exist: %v\n", err)
		// Later calls may fail without the topic; this is only a warning.
	} else {
		fmt.Println("topic exists")
	}

	key := "Order-1001"
	fmt.Printf("\n=== query messages by key: %s ===\n", key)
	beginTime := time.Now().Add(-24 * time.Hour).UnixMilli()
	endTime := time.Now().UnixMilli()
	msgs, err := client.QueryMessage(ctx, topic, key, 32, beginTime, endTime)
	if err != nil {
		log.Printf("query failed: %v", err)
	} else {
		fmt.Printf("messages found: %d\n", len(msgs))
		for i, msg := range msgs {
			fmt.Printf("[%d] MsgId: %s, StoreTime: %d\n", i, msg.MsgId, msg.StoreTimestamp)
			// Expand only the first hit.
			if i == 0 {
				queryDetail(ctx, client, topic, msg.MsgId)
			}
		}
	}

	fmt.Printf("\n=== query consume queue: %s ===\n", topic)
	// Hardcoded for brevity; real code should read this from ClusterInfo.
	brokerAddr := "127.0.0.1:10911"
	qData, err := client.QueryConsumeQueue(ctx, brokerAddr, topic, 0, 0, 10, "DefaultGroup")
	if err != nil {
		log.Printf("failed to query consume queue: %v", err)
	} else {
		fmt.Printf("entries: %d\n", len(qData))
	}
}

func queryDetail(ctx context.Context, client *admin.Client, topic, msgId string) {
	fmt.Printf("\n=== view message detail: %s ===\n", msgId)
	msg, err := client.ViewMessage(ctx, topic, msgId)
	if err != nil {
		log.Printf("failed to view detail: %v", err)
	} else {
		fmt.Printf("Topic: %s\n", msg.Topic)
		fmt.Printf("QueueId: %d\n", msg.QueueId)
		fmt.Printf("QueueOffset: %d\n", msg.QueueOffset)
		fmt.Printf("BornHost: %s\n", msg.BornHost)
		fmt.Printf("Properties: %v\n", msg.Properties)
	}
}
