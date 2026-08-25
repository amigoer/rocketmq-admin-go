//go:build ignore
// +build ignore

// Example: managing ACL users and rules.
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
	// admin.WithACL("accessKey", "secretKey"), // uncomment if the cluster requires auth
	)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	if err := client.Start(); err != nil {
		log.Fatalf("failed to start client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	// ACL calls address one Broker directly, so its address is needed here.
	brokerAddr := "127.0.0.1:10911"

	fmt.Println("=== create user: test_user ===")
	user := admin.UserInfo{
		Username:   "test_user",
		Password:   "12345678",
		UserType:   "NORMAL",
		UserStatus: "OPEN", // enabled
	}
	if err := client.UpdateUser(ctx, brokerAddr, user); err != nil {
		log.Printf("failed to create user: %v", err)
	} else {
		fmt.Println("user created")
	}

	fmt.Println("\n=== get user info ===")
	userInfo, err := client.GetUser(ctx, brokerAddr, "test_user")
	if err != nil {
		log.Printf("failed to get user: %v", err)
	} else {
		fmt.Printf("user: %s, status: %s\n", userInfo.Username, userInfo.UserStatus)
	}

	fmt.Println("\n=== configure ACL permissions ===")
	acl := admin.AclInfo{
		Subject: "test_user",
		Policies: []admin.AclPolicy{
			{
				Resource: "TestTopic",
				Actions:  []string{"PUB", "SUB"},
				Effect:   "ALLOW",
				Decision: "ALLOW",
			},
		},
	}
	if err := client.UpdateAcl(ctx, brokerAddr, acl); err != nil {
		log.Printf("failed to configure ACL: %v", err)
	} else {
		fmt.Println("ACL configured")
	}

	fmt.Println("\n=== list ACL rules ===")
	acls, err := client.ListAcl(ctx, brokerAddr)
	if err != nil {
		log.Printf("failed to list ACLs: %v", err)
	} else {
		for _, a := range acls.Acls {
			fmt.Printf("subject: %s, policies: %d\n", a.Subject, len(a.Policies))
		}
	}
}
