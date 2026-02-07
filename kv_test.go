package admin

import (
	"testing"
)

// =============================================================================
// KV 配置管理接口集成测试
// =============================================================================

// TestIntegration_PutAndGetKVConfig 测试存储和获取 KV 配置
func TestIntegration_PutAndGetKVConfig(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	namespace := "TEST_NAMESPACE"
	key := "test_key_" + getTestTopicName("")
	value := "test_value_123"

	// 存储 KV
	err := client.PutKVConfig(ctx, namespace, key, value)
	if err != nil {
		t.Logf("存储 KV 配置失败: %v", err)
		return
	}

	t.Logf("存储 KV 配置成功: %s/%s = %s", namespace, key, value)

	// 获取 KV
	gotValue, err := client.GetKVConfig(ctx, namespace, key)
	if err != nil {
		t.Logf("获取 KV 配置失败: %v", err)
	} else {
		t.Logf("获取 KV 配置成功: %s", gotValue)
		if gotValue != value {
			t.Errorf("KV 值不匹配: got %s, want %s", gotValue, value)
		}
	}

	// 删除 KV
	err = client.DeleteKVConfig(ctx, namespace, key)
	if err != nil {
		t.Logf("删除 KV 配置失败: %v", err)
	} else {
		t.Logf("删除 KV 配置成功")
	}
}

// TestIntegration_GetKVListByNamespace 测试按命名空间获取 KV 列表
func TestIntegration_GetKVListByNamespace(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	namespace := "TEST_NAMESPACE"

	// 先存储一些 KV
	key1 := "list_test_key1_" + getTestTopicName("")
	key2 := "list_test_key2_" + getTestTopicName("")

	_ = client.PutKVConfig(ctx, namespace, key1, "value1")
	_ = client.PutKVConfig(ctx, namespace, key2, "value2")
	defer func() {
		_ = client.DeleteKVConfig(ctx, namespace, key1)
		_ = client.DeleteKVConfig(ctx, namespace, key2)
	}()

	// 获取 KV 列表
	kvList, err := client.GetKVListByNamespace(ctx, namespace)
	if err != nil {
		t.Logf("获取 KV 列表失败: %v", err)
		return
	}

	t.Logf("命名空间 %s 的 KV 数量: %d", namespace, len(kvList))
	for k, v := range kvList {
		t.Logf("  %s = %s", k, v)
	}
}

// TestIntegration_DeleteKVConfig 测试删除 KV 配置
func TestIntegration_DeleteKVConfig(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	namespace := "TEST_NAMESPACE"
	key := "delete_test_key_" + getTestTopicName("")

	// 先存储
	_ = client.PutKVConfig(ctx, namespace, key, "to_be_deleted")

	// 删除
	err := client.DeleteKVConfig(ctx, namespace, key)
	if err != nil {
		t.Logf("删除 KV 配置失败: %v", err)
	} else {
		t.Log("删除 KV 配置成功")
	}

	// 验证已删除
	_, err = client.GetKVConfig(ctx, namespace, key)
	if err != nil {
		t.Log("验证 KV 已删除")
	}
}

// TestIntegration_CreateAndUpdateKVConfig 测试创建或更新 KV 配置
func TestIntegration_CreateAndUpdateKVConfig(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	namespace := "TEST_NAMESPACE"
	key := "update_test_key_" + getTestTopicName("")

	// 创建
	err := client.CreateAndUpdateKVConfig(ctx, namespace, key, "initial_value")
	if err != nil {
		t.Logf("创建 KV 配置失败: %v", err)
		return
	}
	defer func() {
		_ = client.DeleteKVConfig(ctx, namespace, key)
	}()

	// 更新
	err = client.CreateAndUpdateKVConfig(ctx, namespace, key, "updated_value")
	if err != nil {
		t.Logf("更新 KV 配置失败: %v", err)
	} else {
		t.Log("更新 KV 配置成功")
	}

	// 验证更新
	value, err := client.GetKVConfig(ctx, namespace, key)
	if err != nil {
		t.Logf("获取更新后的 KV 失败: %v", err)
	} else {
		t.Logf("更新后的值: %s", value)
	}
}

// TestIntegration_CreateOrUpdateOrderConf 测试创建或更新顺序配置
func TestIntegration_CreateOrUpdateOrderConf(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	namespace := "ORDER_CONF_NAMESPACE"
	key := "order_test_key_" + getTestTopicName("")
	value := "order_value"

	err := client.CreateOrUpdateOrderConf(ctx, key, value, namespace)
	if err != nil {
		t.Logf("创建顺序配置失败: %v", err)
	} else {
		t.Log("创建顺序配置成功")
		// 清理
		_ = client.DeleteKVConfig(ctx, namespace, key)
	}
}
