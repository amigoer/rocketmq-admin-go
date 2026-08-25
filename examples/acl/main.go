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
		log.Fatalf("创建客户端失败: %v", err)
	}

	if err := client.Start(); err != nil {
		log.Fatalf("启动客户端失败: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	// ACL calls address one Broker directly, so its address is needed here.
	brokerAddr := "127.0.0.1:10911"

	fmt.Println("=== 创建用户: test_user ===")
	user := admin.UserInfo{
		Username:   "test_user",
		Password:   "12345678",
		UserType:   "NORMAL",
		UserStatus: "OPEN", // enabled
	}
	if err := client.UpdateUser(ctx, brokerAddr, user); err != nil {
		log.Printf("创建用户失败: %v", err)
	} else {
		fmt.Println("用户创建成功")
	}

	fmt.Println("\n=== 获取用户信息 ===")
	userInfo, err := client.GetUser(ctx, brokerAddr, "test_user")
	if err != nil {
		log.Printf("获取用户失败: %v", err)
	} else {
		fmt.Printf("用户: %s, 状态: %s\n", userInfo.Username, userInfo.UserStatus)
	}

	fmt.Println("\n=== 配置 ACL 权限 ===")
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
		log.Printf("配置 ACL 失败: %v", err)
	} else {
		fmt.Println("ACL 配置成功")
	}

	fmt.Println("\n=== 列出 ACL 规则 ===")
	acls, err := client.ListAcl(ctx, brokerAddr)
	if err != nil {
		log.Printf("列出 ACL 失败: %v", err)
	} else {
		for _, a := range acls.Acls {
			fmt.Printf("主体: %s, 策略数: %d\n", a.Subject, len(a.Policies))
		}
	}
}
