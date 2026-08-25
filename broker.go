package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

// FetchBrokerRuntimeStats returns the runtime statistics of one Broker.
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

// GetBrokerConfig returns the configuration of one Broker.
func (c *Client) GetBrokerConfig(ctx context.Context, brokerAddr string) (map[string]string, error) {
	cmd := remoting.NewRequest(remoting.GetBrokerConfig, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	config := make(map[string]string)
	if err := json.Unmarshal(resp.Body, &config); err != nil {
		// Not JSON: a Broker may answer with a raw properties blob.
		configStr := string(resp.Body)
		if configStr != "" {
			config["raw"] = configStr
		}
	}

	return config, nil
}

// UpdateBrokerConfig applies the given properties to one Broker.
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

// WipeWritePermOfBroker removes write permission from a Broker and returns
// the number of affected topics.
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

	var result struct {
		WipeTopicCount int `json:"wipeTopicCount"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return 0, nil // count is best effort; the permission change already succeeded
	}

	return result.WipeTopicCount, nil
}

// AddWritePermOfBroker restores write permission on a Broker and returns the
// number of affected topics.
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

// ViewBrokerStatsData returns one statistics series from a Broker.
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

// GetBrokerHAStatus returns the master/slave replication status of a Broker.
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

// AddBrokerToContainer starts a Broker inside a Broker container.
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

// RemoveBrokerFromContainer stops a Broker running inside a Broker container.
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

// BrokerEpochInfo is a Broker's replication epoch and the offsets it covers.
type BrokerEpochInfo struct {
	Epoch         int64 `json:"epoch"`
	MaxOffset     int64 `json:"maxOffset"`
	ConfirmOffset int64 `json:"confirmOffset"`
}

// GetBrokerEpochCache returns the cached epoch information of a Broker.
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
