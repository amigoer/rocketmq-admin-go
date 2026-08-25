package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

// ExamineBrokerClusterInfo returns the cluster topology known to the NameServer.
func (c *Client) ExamineBrokerClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	cmd := remoting.NewRequest(remoting.GetBrokerClusterInfo, nil)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	// RocketMQ emits numeric map keys without quotes, which is not valid JSON.
	fixedBody := fixJSONBody(resp.Body)

	var clusterInfo ClusterInfo
	if err := json.Unmarshal(fixedBody, &clusterInfo); err != nil {
		return nil, fmt.Errorf("解析集群信息失败: %w", err)
	}

	c.rememberClusterBrokerNames(&clusterInfo)

	return &clusterInfo, nil
}

// GetNameServerAddressList returns the configured NameServer addresses.
func (c *Client) GetNameServerAddressList() []string {
	return c.opts.NameServers
}

// UpdateNameServerConfig applies the given properties to every NameServer.
func (c *Client) UpdateNameServerConfig(ctx context.Context, properties map[string]string) error {
	cmd := remoting.NewRequest(remoting.UpdateNamesrvConfig, properties)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// GetNameServerConfig returns the NameServer configuration properties.
func (c *Client) GetNameServerConfig(ctx context.Context) (map[string]string, error) {
	cmd := remoting.NewRequest(remoting.GetNamesrvConfig, nil)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	config := make(map[string]string)
	if err := json.Unmarshal(resp.Body, &config); err != nil {
		// Not JSON: hand back the raw payload rather than failing.
		if len(resp.Body) > 0 {
			config["raw"] = string(resp.Body)
		}
	}

	return config, nil
}
