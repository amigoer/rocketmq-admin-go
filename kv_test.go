package admin

import (
	"testing"
)

func TestIntegration_PutAndGetKVConfig(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	namespace := "TEST_NAMESPACE"
	key := "test_key_" + getTestTopicName("")
	value := "test_value_123"

	err := client.PutKVConfig(ctx, namespace, key, value)
	if err != nil {
		t.Logf("存储 KV 配置失败: %v", err)
		return
	}

	t.Logf("存储 KV 配置成功: %s/%s = %s", namespace, key, value)

	gotValue, err := client.GetKVConfig(ctx, namespace, key)
	if err != nil {
		t.Logf("获取 KV 配置失败: %v", err)
	} else {
		t.Logf("获取 KV 配置成功: %s", gotValue)
		if gotValue != value {
			t.Errorf("KV 值不匹配: got %s, want %s", gotValue, value)
		}
	}

	err = client.DeleteKVConfig(ctx, namespace, key)
	if err != nil {
		t.Logf("删除 KV 配置失败: %v", err)
	} else {
		t.Logf("删除 KV 配置成功")
	}
}

func TestIntegration_GetKVListByNamespace(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	namespace := "TEST_NAMESPACE"

	key1 := "list_test_key1_" + getTestTopicName("")
	key2 := "list_test_key2_" + getTestTopicName("")

	_ = client.PutKVConfig(ctx, namespace, key1, "value1")
	_ = client.PutKVConfig(ctx, namespace, key2, "value2")
	defer func() {
		_ = client.DeleteKVConfig(ctx, namespace, key1)
		_ = client.DeleteKVConfig(ctx, namespace, key2)
	}()

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

func TestIntegration_DeleteKVConfig(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	namespace := "TEST_NAMESPACE"
	key := "delete_test_key_" + getTestTopicName("")

	_ = client.PutKVConfig(ctx, namespace, key, "to_be_deleted")

	err := client.DeleteKVConfig(ctx, namespace, key)
	if err != nil {
		t.Logf("删除 KV 配置失败: %v", err)
	} else {
		t.Log("删除 KV 配置成功")
	}

	_, err = client.GetKVConfig(ctx, namespace, key)
	if err != nil {
		t.Log("验证 KV 已删除")
	}
}

func TestIntegration_CreateAndUpdateKVConfig(t *testing.T) {
	skipIfNoRocketMQ(t)
	client := getTestClient(t)
	defer client.Close()

	ctx, cancel := testContext()
	defer cancel()

	namespace := "TEST_NAMESPACE"
	key := "update_test_key_" + getTestTopicName("")

	err := client.CreateAndUpdateKVConfig(ctx, namespace, key, "initial_value")
	if err != nil {
		t.Logf("创建 KV 配置失败: %v", err)
		return
	}
	defer func() {
		_ = client.DeleteKVConfig(ctx, namespace, key)
	}()

	err = client.CreateAndUpdateKVConfig(ctx, namespace, key, "updated_value")
	if err != nil {
		t.Logf("更新 KV 配置失败: %v", err)
	} else {
		t.Log("更新 KV 配置成功")
	}

	value, err := client.GetKVConfig(ctx, namespace, key)
	if err != nil {
		t.Logf("获取更新后的 KV 失败: %v", err)
	} else {
		t.Logf("更新后的值: %s", value)
	}
}

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
		_ = client.DeleteKVConfig(ctx, namespace, key)
	}
}
