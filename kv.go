package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

// PutKVConfig stores value under namespace/key.
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

// GetKVConfig returns the value stored under namespace/key.
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

	// RocketMQ returns the value in ExtFields; some responses carry it in the body.
	if value, ok := resp.ExtFields["value"]; ok && value != "" {
		return value, nil
	}

	if len(resp.Body) > 0 {
		var result struct {
			Value string `json:"value"`
		}
		if err := json.Unmarshal(resp.Body, &result); err == nil && result.Value != "" {
			return result.Value, nil
		}
		return string(resp.Body), nil
	}

	return "", nil
}

// DeleteKVConfig removes the value stored under namespace/key.
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

// GetKVListByNamespace returns every key/value pair in a namespace.
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
		return nil, fmt.Errorf("failed to parse KV list: %w", err)
	}

	return result, nil
}

// CreateAndUpdateKVConfig is an alias for PutKVConfig.
func (c *Client) CreateAndUpdateKVConfig(ctx context.Context, namespace, key, value string) error {
	return c.PutKVConfig(ctx, namespace, key, value)
}

// CreateOrUpdateOrderConf stores an ordered-topic configuration under key.
func (c *Client) CreateOrUpdateOrderConf(ctx context.Context, key, value, namespace string) error {
	return c.PutKVConfig(ctx, namespace, key, value)
}
