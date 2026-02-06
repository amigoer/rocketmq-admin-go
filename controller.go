package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/codermast/rocketmq-admin-go/protocol/remoting"
)

// =============================================================================
// Controller 管理 (RocketMQ 5.x)
// =============================================================================

// ControllerMetaData Controller 元数据
type ControllerMetaData struct {
	ControllerAddrs map[string]string `json:"controllerAddrs"` // Controller 地址
	LeaderAddr      string            `json:"leaderAddr"`      // Leader 地址
	LeaderId        string            `json:"leaderId"`        // Leader ID
	IsLeader        bool              `json:"isLeader"`        // 是否 Leader
}

// GetControllerMetaData 获取 Controller 元数据
func (c *Client) GetControllerMetaData(ctx context.Context, controllerAddr string) (*ControllerMetaData, error) {
	cmd := remoting.NewRequest(remoting.ControllerGetMetadataInfo, nil)

	conn, err := c.pool.GetOrCreate(controllerAddr)
	if err != nil {
		return nil, err
	}

	resp, err := conn.InvokeSync(ctx, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var meta ControllerMetaData
	if err := json.Unmarshal(resp.Body, &meta); err != nil {
		return nil, fmt.Errorf("解析 Controller 元数据失败: %w", err)
	}

	return &meta, nil
}

// GetControllerConfig 获取 Controller 配置
func (c *Client) GetControllerConfig(ctx context.Context, controllerAddr string) (map[string]string, error) {
	cmd := remoting.NewRequest(remoting.ControllerGetConfig, nil)

	conn, err := c.pool.GetOrCreate(controllerAddr)
	if err != nil {
		return nil, err
	}

	resp, err := conn.InvokeSync(ctx, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	config := make(map[string]string)
	if err := json.Unmarshal(resp.Body, &config); err != nil {
		if len(resp.Body) > 0 {
			config["raw"] = string(resp.Body)
		}
	}

	return config, nil
}

// UpdateControllerConfig 更新 Controller 配置
func (c *Client) UpdateControllerConfig(ctx context.Context, controllerAddr string, properties map[string]string) error {
	cmd := remoting.NewRequest(remoting.ControllerUpdateConfig, properties)

	conn, err := c.pool.GetOrCreate(controllerAddr)
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

// ElectMaster 选举 Master
func (c *Client) ElectMaster(ctx context.Context, controllerAddr, clusterName, brokerName string, brokerId int) error {
	extFields := map[string]string{
		"clusterName": clusterName,
		"brokerName":  brokerName,
		"brokerId":    fmt.Sprintf("%d", brokerId),
	}
	cmd := remoting.NewRequest(remoting.ControllerElectMaster, extFields)

	conn, err := c.pool.GetOrCreate(controllerAddr)
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

// CleanControllerBrokerData 清理 Controller Broker 数据
func (c *Client) CleanControllerBrokerData(ctx context.Context, controllerAddr, clusterName, brokerName string) error {
	extFields := map[string]string{
		"clusterName": clusterName,
		"brokerName":  brokerName,
	}
	cmd := remoting.NewRequest(remoting.CleanControllerBrokerData, extFields)

	conn, err := c.pool.GetOrCreate(controllerAddr)
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

// =============================================================================
// 同步状态
// =============================================================================

// InSyncStateData 同步状态数据
type InSyncStateData struct {
	MasterFlushOffset int64            `json:"masterFlushOffset"` // Master 刷盘偏移
	InSyncMembers     []string         `json:"inSyncMembers"`     // 同步成员
	MasterAddr        string           `json:"masterAddr"`        // Master 地址
	MasterEpoch       int64            `json:"masterEpoch"`       // Master Epoch
	SyncStateSet      map[string]int64 `json:"syncStateSet"`      // 同步状态集
}

// GetInSyncStateData 获取同步状态数据
func (c *Client) GetInSyncStateData(ctx context.Context, controllerAddr string, brokerNames []string) (map[string]*InSyncStateData, error) {
	result := make(map[string]*InSyncStateData)

	for _, brokerName := range brokerNames {
		extFields := map[string]string{
			"brokerName": brokerName,
		}
		cmd := remoting.NewRequest(remoting.GetInSyncStateData, extFields)

		conn, err := c.pool.GetOrCreate(controllerAddr)
		if err != nil {
			continue
		}

		resp, err := conn.InvokeSync(ctx, cmd)
		if err != nil {
			continue
		}

		if resp.Code != remoting.Success {
			continue
		}

		var data InSyncStateData
		if err := json.Unmarshal(resp.Body, &data); err != nil {
			continue
		}

		result[brokerName] = &data
	}

	return result, nil
}
