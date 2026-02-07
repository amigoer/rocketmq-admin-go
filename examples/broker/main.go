//go:build ignore
// +build ignore

// Broker 管理示例
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	admin "github.com/codermast/rocketmq-admin-go"
)

func main() {
	// 1. 初始化客户端
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

	// 2. 获取集群信息以找到 Broker 地址
	fmt.Println("=== 获取集群信息 ===")
	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		log.Fatalf("获取集群信息失败: %v", err)
	}

	var targetBrokerAddr string
	for name, brokerData := range clusterInfo.BrokerAddrTable {
		fmt.Printf("发现 Broker: %s\n", name)
		if addr, ok := brokerData.BrokerAddrs["0"]; ok { // 获取 Master
			targetBrokerAddr = addr
			break
		}
	}

	if targetBrokerAddr == "" {
		log.Fatalf("未找到可用的 Broker Master")
	}

	// 3. 获取 Broker 运行时统计
	fmt.Printf("\n=== 获取 Broker Runtime 统计 (%s) ===\n", targetBrokerAddr)
	kvTable, err := client.FetchBrokerRuntimeStats(ctx, targetBrokerAddr)
	if err != nil {
		log.Printf("获取统计失败: %v", err)
	} else {
		// 打印部分关键指标
		keys := []string{"brokerVersionDesc", "msgPutTotalTodayNow", "msgGetTotalTodayNow"}
		for _, k := range keys {
			if v, ok := kvTable.Table[k]; ok {
				fmt.Printf("%s: %s\n", k, v)
			}
		}
	}

	// 4. 获取 Broker 配置
	fmt.Printf("\n=== 获取 Broker 配置 (%s) ===\n", targetBrokerAddr)
	config, err := client.GetBrokerConfig(ctx, targetBrokerAddr)
	if err != nil {
		log.Printf("获取配置失败: %v", err)
	} else {
		fmt.Printf("brokerName: %s\n", config["brokerName"])
		fmt.Printf("brokerId: %s\n", config["brokerId"])
		fmt.Printf("fileReservedTime: %s\n", config["fileReservedTime"])
	}

	// 5. 更新 Broker 配置 (示例：仅打印，不实际执行以免影响环境)
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
