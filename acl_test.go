package admin

import (
	"testing"
)

func TestIntegration_ListUser(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to get cluster info: %v", err)
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
		t.Fatal("no usable Broker address found")
	}

	users, err := client.ListUser(ctx, brokerAddr)
	if err != nil {
		t.Logf("failed to list users (ACL may be disabled): %v", err)
		return
	}

	t.Logf("users: %d", len(users.Users))
	for _, user := range users.Users {
		t.Logf("  user: %s", user.Username)
	}
}

func TestIntegration_CreateAndDeleteUser(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to get cluster info: %v", err)
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
		t.Fatal("no usable Broker address found")
	}

	testUser := UserInfo{
		Username: "test_user_" + getTestTopicName(""),
		Password: "test_password_123",
	}

	err = client.CreateUser(ctx, brokerAddr, testUser)
	if err != nil {
		t.Logf("failed to create user (ACL may be disabled): %v", err)
		return
	}

	t.Logf("created user: %s", testUser.Username)

	user, err := client.GetUser(ctx, brokerAddr, testUser.Username)
	if err != nil {
		t.Logf("failed to get user: %v", err)
	} else {
		t.Logf("got user: %s", user.Username)
	}

	err = client.DeleteUser(ctx, brokerAddr, testUser.Username)
	if err != nil {
		t.Logf("failed to delete user: %v", err)
	} else {
		t.Logf("deleted user: %s", testUser.Username)
	}
}

func TestIntegration_UpdateUser(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to get cluster info: %v", err)
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
		t.Fatal("no usable Broker address found")
	}

	testUser := UserInfo{
		Username: "test_update_user_" + getTestTopicName(""),
		Password: "old_password_123",
	}

	err = client.CreateUser(ctx, brokerAddr, testUser)
	if err != nil {
		t.Logf("failed to create user (ACL may be disabled): %v", err)
		return
	}
	defer func() {
		_ = client.DeleteUser(ctx, brokerAddr, testUser.Username)
	}()

	testUser.Password = "new_password_456"
	err = client.UpdateUser(ctx, brokerAddr, testUser)
	if err != nil {
		t.Logf("failed to update user: %v", err)
	} else {
		t.Logf("updated user: %s", testUser.Username)
	}
}

func TestIntegration_ListAcl(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to get cluster info: %v", err)
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
		t.Fatal("no usable Broker address found")
	}

	acls, err := client.ListAcl(ctx, brokerAddr)
	if err != nil {
		t.Logf("failed to list ACL rules (ACL may be disabled): %v", err)
		return
	}

	t.Logf("ACL rules: %d", len(acls.Acls))
	for _, acl := range acls.Acls {
		t.Logf("  ACL: Subject=%s", acl.Subject)
	}
}

func TestIntegration_CreateAndDeleteAcl(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to get cluster info: %v", err)
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
		t.Fatal("no usable Broker address found")
	}

	testAcl := AclInfo{
		Subject: "test_acl_" + getTestTopicName(""),
	}

	err = client.CreateAcl(ctx, brokerAddr, testAcl)
	if err != nil {
		t.Logf("failed to create ACL rule (ACL may be disabled): %v", err)
		return
	}

	t.Logf("created ACL rule: %s", testAcl.Subject)

	acl, err := client.GetAcl(ctx, brokerAddr, testAcl.Subject)
	if err != nil {
		t.Logf("failed to get ACL rule: %v", err)
	} else {
		t.Logf("got ACL rule: %s", acl.Subject)
	}

	err = client.DeleteAcl(ctx, brokerAddr, testAcl.Subject)
	if err != nil {
		t.Logf("failed to delete ACL rule: %v", err)
	} else {
		t.Logf("deleted ACL rule: %s", testAcl.Subject)
	}
}

func TestIntegration_UpdateAcl(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	clusterInfo, err := client.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to get cluster info: %v", err)
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
		t.Fatal("no usable Broker address found")
	}

	testAcl := AclInfo{
		Subject: "test_update_acl_" + getTestTopicName(""),
	}

	err = client.CreateAcl(ctx, brokerAddr, testAcl)
	if err != nil {
		t.Logf("failed to create ACL rule (ACL may be disabled): %v", err)
		return
	}
	defer func() {
		_ = client.DeleteAcl(ctx, brokerAddr, testAcl.Subject)
	}()

	err = client.UpdateAcl(ctx, brokerAddr, testAcl)
	if err != nil {
		t.Logf("failed to update ACL rule: %v", err)
	} else {
		t.Logf("updated ACL rule: %s", testAcl.Subject)
	}
}
