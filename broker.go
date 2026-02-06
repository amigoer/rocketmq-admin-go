package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/codermast/rocketmq-admin-go/protocol/remoting"
)

// =============================================================================
// Broker 管理接口
// =============================================================================

// FetchBrokerRuntimeStats 获取 Broker 运行时统计信息
func (c *Client) FetchBrokerRuntimeStats(ctx context.Context, brokerAddr string) (*KVTable, error) {
	cmd := remoting.NewRequest(remoting.GetBrokerRuntimeInfo, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var kvTable KVTable
	if err := json.Unmarshal(resp.Body, &kvTable); err != nil {
		return nil, fmt.Errorf("解析 Broker 运行信息失败: %w", err)
	}

	return &kvTable, nil
}

// GetBrokerConfig 获取 Broker 配置
func (c *Client) GetBrokerConfig(ctx context.Context, brokerAddr string) (map[string]string, error) {
	cmd := remoting.NewRequest(remoting.GetBrokerConfig, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	// Broker 配置以 Properties 格式返回
	config := make(map[string]string)
	if err := json.Unmarshal(resp.Body, &config); err != nil {
		// 如果 JSON 解析失败，尝试解析为字符串
		configStr := string(resp.Body)
		if configStr != "" {
			config["raw"] = configStr
		}
	}

	return config, nil
}

// UpdateBrokerConfig 更新 Broker 配置
func (c *Client) UpdateBrokerConfig(ctx context.Context, brokerAddr string, properties map[string]string) error {
	extFields := make(map[string]string)
	for k, v := range properties {
		extFields[k] = v
	}

	cmd := remoting.NewRequest(remoting.UpdateBrokerConfig, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// WipeWritePermOfBroker 清除 Broker 写权限
func (c *Client) WipeWritePermOfBroker(ctx context.Context, brokerName string) (int, error) {
	extFields := map[string]string{
		"brokerName": brokerName,
	}
	cmd := remoting.NewRequest(remoting.WipeWritePermOfBroker, extFields)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return 0, err
	}

	if resp.Code != remoting.Success {
		return 0, NewAdminError(resp.Code, resp.Remark)
	}

	// 返回修改的队列数
	var result struct {
		WipeTopicCount int `json:"wipeTopicCount"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return 0, nil // 忽略解析错误
	}

	return result.WipeTopicCount, nil
}

// AddWritePermOfBroker 添加 Broker 写权限
func (c *Client) AddWritePermOfBroker(ctx context.Context, brokerName string) (int, error) {
	extFields := map[string]string{
		"brokerName": brokerName,
	}
	cmd := remoting.NewRequest(remoting.AddWritePermOfBroker, extFields)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return 0, err
	}

	if resp.Code != remoting.Success {
		return 0, NewAdminError(resp.Code, resp.Remark)
	}

	var result struct {
		AddTopicCount int `json:"addTopicCount"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return 0, nil
	}

	return result.AddTopicCount, nil
}

// ViewBrokerStatsData 查看 Broker 统计数据
func (c *Client) ViewBrokerStatsData(ctx context.Context, brokerAddr, statsName, statsKey string) (*BrokerStatsData, error) {
	extFields := map[string]string{
		"statsName": statsName,
		"statsKey":  statsKey,
	}
	cmd := remoting.NewRequest(remoting.ViewBrokerStatsData, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var stats BrokerStatsData
	if err := json.Unmarshal(resp.Body, &stats); err != nil {
		return nil, fmt.Errorf("解析统计数据失败: %w", err)
	}

	return &stats, nil
}

// GetBrokerHAStatus 获取 Broker HA 状态
func (c *Client) GetBrokerHAStatus(ctx context.Context, brokerAddr string) (*BrokerHAStatus, error) {
	cmd := remoting.NewRequest(remoting.GetBrokerHAStatus, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var status BrokerHAStatus
	if err := json.Unmarshal(resp.Body, &status); err != nil {
		return nil, fmt.Errorf("解析 HA 状态失败: %w", err)
	}

	return &status, nil
}

// =============================================================================
// Broker 容器管理
// =============================================================================

// AddBrokerToContainer 添加 Broker 到容器
func (c *Client) AddBrokerToContainer(ctx context.Context, brokerContainerAddr, brokerConfig string) error {
	extFields := map[string]string{
		"brokerConfigPath": brokerConfig,
	}
	cmd := remoting.NewRequest(remoting.AddBrokerToContainer, extFields)

	conn, err := c.pool.GetOrCreate(brokerContainerAddr)
	if err != nil {
		return err
	}

	resp, err := conn.InvokeSync(ctx, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// RemoveBrokerFromContainer 从容器移除 Broker
func (c *Client) RemoveBrokerFromContainer(ctx context.Context, brokerContainerAddr, clusterName, brokerName string, brokerId int) error {
	extFields := map[string]string{
		"clusterName": clusterName,
		"brokerName":  brokerName,
		"brokerId":    fmt.Sprintf("%d", brokerId),
	}
	cmd := remoting.NewRequest(remoting.RemoveBrokerFromContainer, extFields)

	conn, err := c.pool.GetOrCreate(brokerContainerAddr)
	if err != nil {
		return err
	}

	resp, err := conn.InvokeSync(ctx, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// BrokerEpochInfo Broker Epoch 信息
type BrokerEpochInfo struct {
	Epoch         int64 `json:"epoch"`         // Epoch
	MaxOffset     int64 `json:"maxOffset"`     // 最大偏移
	ConfirmOffset int64 `json:"confirmOffset"` // 确认偏移
}

// GetBrokerEpochCache 获取 Broker Epoch 缓存
func (c *Client) GetBrokerEpochCache(ctx context.Context, brokerAddr string) (*BrokerEpochInfo, error) {
	cmd := remoting.NewRequest(remoting.GetBrokerEpochCache, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var info BrokerEpochInfo
	if err := json.Unmarshal(resp.Body, &info); err != nil {
		return nil, fmt.Errorf("解析 Epoch 缓存失败: %w", err)
	}

	return &info, nil
}
