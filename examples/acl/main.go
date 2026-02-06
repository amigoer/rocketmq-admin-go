//go:build ignore
// +build ignore

// ACL 权限管理示例
package main

import (
	"context"
	"fmt"
	"log"

	admin "github.com/codermast/rocketmq-admin-go"
)

func main() {
	// 连接支持 ACL 的 NameServer/Broker
	client, err := admin.NewClient(
		admin.WithNameServers([]string{"127.0.0.1:9876"}),
		// admin.WithACL("accessKey", "secretKey"), // 如果需要鉴权
	)
	if err != nil {
		log.Fatalf("创建客户端失败: %v", err)
	}

	if err := client.Start(); err != nil {
		log.Fatalf("启动客户端失败: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	// 注意：ACL 操作通常需要 Broker 地址，或者配置了自动寻找 Controller/Broker
	// 这里假设直接操作某个 Broker
	brokerAddr := "127.0.0.1:10911"

	// 1. 创建/更新用户
	fmt.Println("=== 创建用户: test_user ===")
	user := admin.UserInfo{
		Username:   "test_user",
		Password:   "12345678",
		UserType:   "NORMAL",
		UserStatus: "OPEN", // 启用
	}
	if err := client.UpdateUser(ctx, brokerAddr, user); err != nil {
		log.Printf("创建用户失败: %v", err)
	} else {
		fmt.Println("用户创建成功")
	}

	// 2. 获取用户信息
	fmt.Println("\n=== 获取用户信息 ===")
	userInfo, err := client.GetUser(ctx, brokerAddr, "test_user")
	if err != nil {
		log.Printf("获取用户失败: %v", err)
	} else {
		fmt.Printf("用户: %s, 状态: %s\n", userInfo.Username, userInfo.UserStatus)
	}

	// 3. 配置 ACL 权限
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

	// 4. 列出 ACL
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
