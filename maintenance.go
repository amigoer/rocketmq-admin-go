package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

// CleanExpiredConsumerQueue cleans expired consume queues on every Broker in a cluster.
func (c *Client) CleanExpiredConsumerQueue(ctx context.Context, clusterName string) error {
	clusterInfo, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return err
	}

	brokerNames, ok := clusterInfo.ClusterAddrTable[clusterName]
	if !ok {
		return fmt.Errorf("集群 %s 不存在", clusterName)
	}

	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}

		for _, brokerAddr := range brokerData.BrokerAddrs {
			if err := c.CleanExpiredConsumerQueueByAddr(ctx, brokerAddr); err != nil {
				return err
			}
		}
	}

	return nil
}

// CleanExpiredConsumerQueueByAddr cleans expired consume queues on one Broker.
func (c *Client) CleanExpiredConsumerQueueByAddr(ctx context.Context, brokerAddr string) error {
	cmd := remoting.NewRequest(remoting.CleanExpiredConsumeQueue, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// DeleteExpiredCommitLog deletes expired CommitLog files across a cluster.
func (c *Client) DeleteExpiredCommitLog(ctx context.Context, clusterName string) error {
	clusterInfo, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return err
	}

	brokerNames, ok := clusterInfo.ClusterAddrTable[clusterName]
	if !ok {
		return fmt.Errorf("集群 %s 不存在", clusterName)
	}

	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}

		for _, brokerAddr := range brokerData.BrokerAddrs {
			if err := c.DeleteExpiredCommitLogByAddr(ctx, brokerAddr); err != nil {
				return err
			}
		}
	}

	return nil
}

// DeleteExpiredCommitLogByAddr deletes expired CommitLog files on one Broker.
func (c *Client) DeleteExpiredCommitLogByAddr(ctx context.Context, brokerAddr string) error {
	cmd := remoting.NewRequest(remoting.DeleteExpiredCommitLog, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// CleanUnusedTopic drops unused topics across a cluster.
func (c *Client) CleanUnusedTopic(ctx context.Context, clusterName string) error {
	clusterInfo, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return err
	}

	brokerNames, ok := clusterInfo.ClusterAddrTable[clusterName]
	if !ok {
		return fmt.Errorf("集群 %s 不存在", clusterName)
	}

	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}

		for _, brokerAddr := range brokerData.BrokerAddrs {
			if err := c.CleanUnusedTopicByAddr(ctx, brokerAddr); err != nil {
				return err
			}
		}
	}

	return nil
}

// CleanUnusedTopicByAddr drops unused topics on one Broker.
func (c *Client) CleanUnusedTopicByAddr(ctx context.Context, brokerAddr string) error {
	cmd := remoting.NewRequest(remoting.CleanUnusedTopic, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// SetCommitLogReadAheadMode sets the CommitLog read-ahead mode on one Broker:
// 0 off, 1 sequential, 2 random.
func (c *Client) SetCommitLogReadAheadMode(ctx context.Context, brokerAddr string, mode int) error {
	extFields := map[string]string{
		"readAheadMode": fmt.Sprintf("%d", mode),
	}
	cmd := remoting.NewRequest(remoting.SetCommitLogReadAheadMode, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// SetCommitLogReadAheadModeInCluster sets the read-ahead mode on every Broker
// in a cluster, stopping at the first failure.
func (c *Client) SetCommitLogReadAheadModeInCluster(ctx context.Context, clusterName string, mode int) error {
	clusterInfo, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return err
	}

	brokerNames, ok := clusterInfo.ClusterAddrTable[clusterName]
	if !ok {
		return fmt.Errorf("集群 %s 不存在", clusterName)
	}

	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}

		for _, brokerAddr := range brokerData.BrokerAddrs {
			if err := c.SetCommitLogReadAheadMode(ctx, brokerAddr, mode); err != nil {
				return fmt.Errorf("设置 %s 预读模式失败: %w", brokerAddr, err)
			}
		}
	}

	return nil
}

// RocksDBConfig is a Broker's RocksDB store tuning.
type RocksDBConfig struct {
	BlockCacheSize       int64  `json:"blockCacheSize"`
	WriteBufferSize      int64  `json:"writeBufferSize"`
	MaxWriteBufferNumber int    `json:"maxWriteBufferNumber"`
	Level0FileNumCompact int    `json:"level0FileNumCompact"` // L0 file count that triggers compaction
	MaxBackgroundJobs    int    `json:"maxBackgroundJobs"`
	CompactionStyle      string `json:"compactionStyle"`
}

// ExportRocksDBConfigToJson returns a Broker's RocksDB configuration as JSON.
func (c *Client) ExportRocksDBConfigToJson(ctx context.Context, brokerAddr string) (string, error) {
	cmd := remoting.NewRequest(remoting.ExportRocksDBConfigToJson, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return "", err
	}

	if resp.Code != remoting.Success {
		return "", NewAdminError(resp.Code, resp.Remark)
	}

	return string(resp.Body), nil
}

// RocksDBCQWriteProgress is the RocksDB consume-queue write progress of one queue.
type RocksDBCQWriteProgress struct {
	Topic       string  `json:"topic"`
	QueueId     int     `json:"queueId"`
	CqOffset    int64   `json:"cqOffset"`
	Progress    float64 `json:"progress"` // percent, 0-100
	IsCompleted bool    `json:"isCompleted"`
}

// CheckRocksdbCqWriteProgress reports RocksDB consume-queue write progress for a topic.
func (c *Client) CheckRocksdbCqWriteProgress(ctx context.Context, brokerAddr, topic string) ([]RocksDBCQWriteProgress, error) {
	extFields := map[string]string{
		"topic": topic,
	}
	cmd := remoting.NewRequest(remoting.CheckRocksdbCqWriteProgress, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var progress []RocksDBCQWriteProgress
	if err := json.Unmarshal(resp.Body, &progress); err != nil {
		return nil, fmt.Errorf("解析 RocksDB CQ 写入进度失败: %w", err)
	}

	return progress, nil
}
