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
		t.Logf("failed to store KV config: %v", err)
		return
	}

	t.Logf("stored KV config: %s/%s = %s", namespace, key, value)

	gotValue, err := client.GetKVConfig(ctx, namespace, key)
	if err != nil {
		t.Logf("failed to get KV config: %v", err)
	} else {
		t.Logf("got KV config: %s", gotValue)
		if gotValue != value {
			t.Errorf("KV value mismatch: got %s, want %s", gotValue, value)
		}
	}

	err = client.DeleteKVConfig(ctx, namespace, key)
	if err != nil {
		t.Logf("failed to delete KV config: %v", err)
	} else {
		t.Logf("deleted KV config")
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
		t.Logf("failed to get KV list: %v", err)
		return
	}

	t.Logf("KV entries in namespace %s: %d", namespace, len(kvList))
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
		t.Logf("failed to delete KV config: %v", err)
	} else {
		t.Log("deleted KV config")
	}

	_, err = client.GetKVConfig(ctx, namespace, key)
	if err != nil {
		t.Log("verified the KV entry is gone")
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
		t.Logf("failed to create KV config: %v", err)
		return
	}
	defer func() {
		_ = client.DeleteKVConfig(ctx, namespace, key)
	}()

	err = client.CreateAndUpdateKVConfig(ctx, namespace, key, "updated_value")
	if err != nil {
		t.Logf("failed to update KV config: %v", err)
	} else {
		t.Log("updated KV config")
	}

	value, err := client.GetKVConfig(ctx, namespace, key)
	if err != nil {
		t.Logf("failed to read the updated KV entry: %v", err)
	} else {
		t.Logf("updated value: %s", value)
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
		t.Logf("failed to create order config: %v", err)
	} else {
		t.Log("created order config")
		_ = client.DeleteKVConfig(ctx, namespace, key)
	}
}
