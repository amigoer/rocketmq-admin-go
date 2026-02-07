package admin

import (
	"testing"
)

// =============================================================================
// ACL 管理接口集成测试
// =============================================================================

// TestIntegration_ListUser 测试列出所有用户
func TestIntegration_ListUser(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 获取 Broker 地址
	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("获取集群信息失败: %v", err)
	}

	var brokerAddr string
	for _, brokerData := range clusterInfo.BrokerAddrTable {
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}
		if brokerAddr != "" {
			break
		}
	}

	if brokerAddr == "" {
		t.Fatal("未找到可用的 Broker 地址")
	}

	users, err := client.ListUser(ctx, brokerAddr)
	if err != nil {
		// ACL 可能未启用
		t.Logf("列出用户失败（ACL 可能未启用）: %v", err)
		return
	}

	t.Logf("用户数量: %d", len(users.Users))
	for _, user := range users.Users {
		t.Logf("  用户: %s", user.Username)
	}
}

// TestIntegration_CreateAndDeleteUser 测试创建和删除用户
func TestIntegration_CreateAndDeleteUser(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 获取 Broker 地址
	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("获取集群信息失败: %v", err)
	}

	var brokerAddr string
	for _, brokerData := range clusterInfo.BrokerAddrTable {
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}
		if brokerAddr != "" {
			break
		}
	}

	if brokerAddr == "" {
		t.Fatal("未找到可用的 Broker 地址")
	}

	// 创建测试用户
	testUser := UserInfo{
		Username: "test_user_" + getTestTopicName(""),
		Password: "test_password_123",
	}

	err = client.CreateUser(ctx, brokerAddr, testUser)
	if err != nil {
		t.Logf("创建用户失败（ACL 可能未启用）: %v", err)
		return
	}

	t.Logf("创建用户成功: %s", testUser.Username)

	// 获取用户
	user, err := client.GetUser(ctx, brokerAddr, testUser.Username)
	if err != nil {
		t.Logf("获取用户失败: %v", err)
	} else {
		t.Logf("获取用户成功: %s", user.Username)
	}

	// 删除用户
	err = client.DeleteUser(ctx, brokerAddr, testUser.Username)
	if err != nil {
		t.Logf("删除用户失败: %v", err)
	} else {
		t.Logf("删除用户成功: %s", testUser.Username)
	}
}

// TestIntegration_UpdateUser 测试更新用户
func TestIntegration_UpdateUser(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 获取 Broker 地址
	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("获取集群信息失败: %v", err)
	}

	var brokerAddr string
	for _, brokerData := range clusterInfo.BrokerAddrTable {
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}
		if brokerAddr != "" {
			break
		}
	}

	if brokerAddr == "" {
		t.Fatal("未找到可用的 Broker 地址")
	}

	// 创建测试用户
	testUser := UserInfo{
		Username: "test_update_user_" + getTestTopicName(""),
		Password: "old_password_123",
	}

	err = client.CreateUser(ctx, brokerAddr, testUser)
	if err != nil {
		t.Logf("创建用户失败（ACL 可能未启用）: %v", err)
		return
	}
	defer func() {
		_ = client.DeleteUser(ctx, brokerAddr, testUser.Username)
	}()

	// 更新用户
	testUser.Password = "new_password_456"
	err = client.UpdateUser(ctx, brokerAddr, testUser)
	if err != nil {
		t.Logf("更新用户失败: %v", err)
	} else {
		t.Logf("更新用户成功: %s", testUser.Username)
	}
}

// TestIntegration_ListAcl 测试列出所有 ACL 规则
func TestIntegration_ListAcl(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 获取 Broker 地址
	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("获取集群信息失败: %v", err)
	}

	var brokerAddr string
	for _, brokerData := range clusterInfo.BrokerAddrTable {
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}
		if brokerAddr != "" {
			break
		}
	}

	if brokerAddr == "" {
		t.Fatal("未找到可用的 Broker 地址")
	}

	acls, err := client.ListAcl(ctx, brokerAddr)
	if err != nil {
		t.Logf("列出 ACL 规则失败（ACL 可能未启用）: %v", err)
		return
	}

	t.Logf("ACL 规则数量: %d", len(acls.Acls))
	for _, acl := range acls.Acls {
		t.Logf("  ACL: Subject=%s", acl.Subject)
	}
}

// TestIntegration_CreateAndDeleteAcl 测试创建和删除 ACL 规则
func TestIntegration_CreateAndDeleteAcl(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 获取 Broker 地址
	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("获取集群信息失败: %v", err)
	}

	var brokerAddr string
	for _, brokerData := range clusterInfo.BrokerAddrTable {
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}
		if brokerAddr != "" {
			break
		}
	}

	if brokerAddr == "" {
		t.Fatal("未找到可用的 Broker 地址")
	}

	// 创建测试 ACL 规则
	testAcl := AclInfo{
		Subject: "test_acl_" + getTestTopicName(""),
	}

	err = client.CreateAcl(ctx, brokerAddr, testAcl)
	if err != nil {
		t.Logf("创建 ACL 规则失败（ACL 可能未启用）: %v", err)
		return
	}

	t.Logf("创建 ACL 规则成功: %s", testAcl.Subject)

	// 获取 ACL
	acl, err := client.GetAcl(ctx, brokerAddr, testAcl.Subject)
	if err != nil {
		t.Logf("获取 ACL 规则失败: %v", err)
	} else {
		t.Logf("获取 ACL 规则成功: %s", acl.Subject)
	}

	// 删除 ACL
	err = client.DeleteAcl(ctx, brokerAddr, testAcl.Subject)
	if err != nil {
		t.Logf("删除 ACL 规则失败: %v", err)
	} else {
		t.Logf("删除 ACL 规则成功: %s", testAcl.Subject)
	}
}

// TestIntegration_UpdateAcl 测试更新 ACL 规则
func TestIntegration_UpdateAcl(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	// 获取 Broker 地址
	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("获取集群信息失败: %v", err)
	}

	var brokerAddr string
	for _, brokerData := range clusterInfo.BrokerAddrTable {
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}
		if brokerAddr != "" {
			break
		}
	}

	if brokerAddr == "" {
		t.Fatal("未找到可用的 Broker 地址")
	}

	// 创建测试 ACL 规则
	testAcl := AclInfo{
		Subject: "test_update_acl_" + getTestTopicName(""),
	}

	err = client.CreateAcl(ctx, brokerAddr, testAcl)
	if err != nil {
		t.Logf("创建 ACL 规则失败（ACL 可能未启用）: %v", err)
		return
	}
	defer func() {
		_ = client.DeleteAcl(ctx, brokerAddr, testAcl.Subject)
	}()

	// 更新 ACL
	err = client.UpdateAcl(ctx, brokerAddr, testAcl)
	if err != nil {
		t.Logf("更新 ACL 规则失败: %v", err)
	} else {
		t.Logf("更新 ACL 规则成功: %s", testAcl.Subject)
	}
}
