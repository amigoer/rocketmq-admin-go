package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/codermast/rocketmq-admin-go/protocol/remoting"
)

// =============================================================================
// 集群管理接口
// =============================================================================

// ExamineBrokerClusterInfo 查询集群信息
func (c *Client) ExamineBrokerClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	cmd := remoting.NewRequest(remoting.GetBrokerClusterInfo, nil)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	// 修复 RocketMQ 返回的非标准 JSON（数字 key 没有引号）
	fixedBody := fixJSONBody(resp.Body)

	var clusterInfo ClusterInfo
	if err := json.Unmarshal(fixedBody, &clusterInfo); err != nil {
		return nil, fmt.Errorf("解析集群信息失败: %w", err)
	}

	return &clusterInfo, nil
}

// GetNameServerAddressList 获取 NameServer 地址列表
func (c *Client) GetNameServerAddressList() []string {
	return c.opts.NameServers
}

// =============================================================================
// NameServer 配置管理
// =============================================================================

// UpdateNameServerConfig 更新 NameServer 配置
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

// GetNameServerConfig 获取 NameServer 配置
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
		// 尝试作为字符串处理
		if len(resp.Body) > 0 {
			config["raw"] = string(resp.Body)
		}
	}

	return config, nil
}
