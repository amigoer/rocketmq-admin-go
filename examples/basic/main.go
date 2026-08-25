//go:build ignore
// +build ignore

// Example: querying cluster information.
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

	fmt.Println("=== query cluster info ===")
	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		log.Printf("failed to query cluster info: %v", err)
	} else {
		fmt.Printf("cluster info: %+v\n", clusterInfo)
	}

	fmt.Println("\n=== get topic list ===")
	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		log.Printf("failed to get topic list: %v", err)
	} else {
		fmt.Printf("topics: %d\n", len(topicList.TopicList))
		for i, topic := range topicList.TopicList {
			if i >= 10 {
				fmt.Printf("  ... and %d more topics\n", len(topicList.TopicList)-10)
				break
			}
			fmt.Printf("  - %s\n", topic)
		}
	}

	fmt.Println("\n=== NameServer addresses ===")
	nameServers := client.GetNameServerAddressList()
	for _, ns := range nameServers {
		fmt.Printf("  - %s\n", ns)
	}
}
