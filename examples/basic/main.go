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
		log.Fatalf("创建客户端失败: %v", err)
	}

	if err := client.Start(); err != nil {
		log.Fatalf("启动客户端失败: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	fmt.Println("=== 查询集群信息 ===")
	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		log.Printf("查询集群信息失败: %v", err)
	} else {
		fmt.Printf("集群信息: %+v\n", clusterInfo)
	}

	fmt.Println("\n=== 获取 Topic 列表 ===")
	topicList, err := client.FetchAllTopicList(ctx)
	if err != nil {
		log.Printf("获取 Topic 列表失败: %v", err)
	} else {
		fmt.Printf("Topic 数量: %d\n", len(topicList.TopicList))
		for i, topic := range topicList.TopicList {
			if i >= 10 {
				fmt.Printf("  ... 还有 %d 个 Topic\n", len(topicList.TopicList)-10)
				break
			}
			fmt.Printf("  - %s\n", topic)
		}
	}

	fmt.Println("\n=== NameServer 地址 ===")
	nameServers := client.GetNameServerAddressList()
	for _, ns := range nameServers {
		fmt.Printf("  - %s\n", ns)
	}
}
