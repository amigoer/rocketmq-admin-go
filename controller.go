package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

// ControllerMetaData describes the controller quorum and its current leader.
// Controllers exist only in RocketMQ 5.x.
type ControllerMetaData struct {
	ControllerAddrs map[string]string `json:"controllerAddrs"`
	LeaderAddr      string            `json:"leaderAddr"`
	LeaderId        string            `json:"leaderId"`
	IsLeader        bool              `json:"isLeader"`
}

// GetControllerMetaData returns the metadata held by one controller.
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
		return nil, fmt.Errorf("failed to parse controller metadata: %w", err)
	}

	return &meta, nil
}

// GetControllerConfig returns the configuration of one controller.
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

// UpdateControllerConfig applies the given properties to one controller.
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

// ElectMaster asks the controller to elect a new master for a broker group.
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

// CleanControllerBrokerData drops a Broker's metadata from the controller.
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

// InSyncStateData is the in-sync replica set of one broker group.
type InSyncStateData struct {
	MasterFlushOffset int64            `json:"masterFlushOffset"`
	InSyncMembers     []string         `json:"inSyncMembers"`
	MasterAddr        string           `json:"masterAddr"`
	MasterEpoch       int64            `json:"masterEpoch"`
	SyncStateSet      map[string]int64 `json:"syncStateSet"`
}

// GetInSyncStateData returns the in-sync state of each named broker group.
// Groups the controller fails to answer for are left out of the result.
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
