package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/codermast/rocketmq-admin-go/protocol/remoting"
)

// =============================================================================
// KV 配置管理
// =============================================================================

// PutKVConfig 存储 KV 配置
func (c *Client) PutKVConfig(ctx context.Context, namespace, key, value string) error {
	extFields := map[string]string{
		"namespace": namespace,
		"key":       key,
		"value":     value,
	}
	cmd := remoting.NewRequest(remoting.PutKVConfig, extFields)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// GetKVConfig 获取 KV 配置
func (c *Client) GetKVConfig(ctx context.Context, namespace, key string) (string, error) {
	extFields := map[string]string{
		"namespace": namespace,
		"key":       key,
	}
	cmd := remoting.NewRequest(remoting.GetKVConfig, extFields)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return "", err
	}

	if resp.Code != remoting.Success {
		return "", NewAdminError(resp.Code, resp.Remark)
	}

	var result struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return string(resp.Body), nil
	}

	return result.Value, nil
}

// DeleteKVConfig 删除 KV 配置
func (c *Client) DeleteKVConfig(ctx context.Context, namespace, key string) error {
	extFields := map[string]string{
		"namespace": namespace,
		"key":       key,
	}
	cmd := remoting.NewRequest(remoting.DeleteKVConfig, extFields)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// GetKVListByNamespace 按命名空间获取 KV 列表
func (c *Client) GetKVListByNamespace(ctx context.Context, namespace string) (map[string]string, error) {
	extFields := map[string]string{
		"namespace": namespace,
	}
	cmd := remoting.NewRequest(remoting.GetKVListByNamespace, extFields)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	result := make(map[string]string)
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return nil, fmt.Errorf("解析 KV 列表失败: %w", err)
	}

	return result, nil
}

// CreateAndUpdateKVConfig 创建或更新 KV 配置（与 PutKVConfig 相同）
func (c *Client) CreateAndUpdateKVConfig(ctx context.Context, namespace, key, value string) error {
	return c.PutKVConfig(ctx, namespace, key, value)
}

// CreateOrUpdateOrderConf 创建或更新顺序配置
func (c *Client) CreateOrUpdateOrderConf(ctx context.Context, key, value, namespace string) error {
	return c.PutKVConfig(ctx, namespace, key, value)
}
