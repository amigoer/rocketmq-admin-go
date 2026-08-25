//go:build ignore
// +build ignore

// Example: inspecting Broker runtime stats and configuration.
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
		admin.WithTimeout(3*time.Second),
	)
	if err != nil {
		log.Fatalf("创建客户端失败: %v", err)
	}

	if err := client.Start(); err != nil {
		log.Fatalf("启动客户端失败: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Cluster info is what yields a Broker address to talk to.
	fmt.Println("=== 获取集群信息 ===")
	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		log.Fatalf("获取集群信息失败: %v", err)
	}

	var targetBrokerAddr string
	for name, brokerData := range clusterInfo.BrokerAddrTable {
		fmt.Printf("发现 Broker: %s\n", name)
		if addr, ok := brokerData.BrokerAddrs["0"]; ok { // broker id "0" is the master
			targetBrokerAddr = addr
			break
		}
	}

	if targetBrokerAddr == "" {
		log.Fatalf("未找到可用的 Broker Master")
	}

	fmt.Printf("\n=== 获取 Broker Runtime 统计 (%s) ===\n", targetBrokerAddr)
	kvTable, err := client.FetchBrokerRuntimeStats(ctx, targetBrokerAddr)
	if err != nil {
		log.Printf("获取统计失败: %v", err)
	} else {
		keys := []string{"brokerVersionDesc", "msgPutTotalTodayNow", "msgGetTotalTodayNow"}
		for _, k := range keys {
			if v, ok := kvTable.Table[k]; ok {
				fmt.Printf("%s: %s\n", k, v)
			}
		}
	}

	fmt.Printf("\n=== 获取 Broker 配置 (%s) ===\n", targetBrokerAddr)
	config, err := client.GetBrokerConfig(ctx, targetBrokerAddr)
	if err != nil {
		log.Printf("获取配置失败: %v", err)
	} else {
		fmt.Printf("brokerName: %s\n", config["brokerName"])
		fmt.Printf("brokerId: %s\n", config["brokerId"])
		fmt.Printf("fileReservedTime: %s\n", config["fileReservedTime"])
	}

	// Updating config is left commented out so that running this example
	// cannot change the reader's Broker.
	// fmt.Println("\n=== 更新 Broker 配置 ===")
	// newConfig := map[string]string{
	// 	"fileReservedTime": "48",
	// }
	// if err := client.UpdateBrokerConfig(ctx, targetBrokerAddr, newConfig); err != nil {
	// 	log.Printf("更新配置失败: %v", err)
	// } else {
	// 	fmt.Println("更新配置成功")
	// }
}
