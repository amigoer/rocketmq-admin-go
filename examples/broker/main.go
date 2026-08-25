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
		log.Fatalf("failed to create client: %v", err)
	}

	if err := client.Start(); err != nil {
		log.Fatalf("failed to start client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Cluster info is what yields a Broker address to talk to.
	fmt.Println("=== get cluster info ===")
	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		log.Fatalf("failed to get cluster info: %v", err)
	}

	var targetBrokerAddr string
	for name, brokerData := range clusterInfo.BrokerAddrTable {
		fmt.Printf("found Broker: %s\n", name)
		if addr, ok := brokerData.BrokerAddrs["0"]; ok { // broker id "0" is the master
			targetBrokerAddr = addr
			break
		}
	}

	if targetBrokerAddr == "" {
		log.Fatalf("no usable Broker master found")
	}

	fmt.Printf("\n=== Broker runtime stats (%s) ===\n", targetBrokerAddr)
	kvTable, err := client.FetchBrokerRuntimeStats(ctx, targetBrokerAddr)
	if err != nil {
		log.Printf("failed to get stats: %v", err)
	} else {
		keys := []string{"brokerVersionDesc", "msgPutTotalTodayNow", "msgGetTotalTodayNow"}
		for _, k := range keys {
			if v, ok := kvTable.Table[k]; ok {
				fmt.Printf("%s: %s\n", k, v)
			}
		}
	}

	fmt.Printf("\n=== Broker config (%s) ===\n", targetBrokerAddr)
	config, err := client.GetBrokerConfig(ctx, targetBrokerAddr)
	if err != nil {
		log.Printf("failed to get config: %v", err)
	} else {
		fmt.Printf("brokerName: %s\n", config["brokerName"])
		fmt.Printf("brokerId: %s\n", config["brokerId"])
		fmt.Printf("fileReservedTime: %s\n", config["fileReservedTime"])
	}

	// Updating config is left commented out so that running this example
	// cannot change the reader's Broker.
	// fmt.Println("\n=== update Broker config ===")
	// newConfig := map[string]string{
	// 	"fileReservedTime": "48",
	// }
	// if err := client.UpdateBrokerConfig(ctx, targetBrokerAddr, newConfig); err != nil {
	// 	log.Printf("failed to update config: %v", err)
	// } else {
	// 	fmt.Println("config updated")
	// }
}
