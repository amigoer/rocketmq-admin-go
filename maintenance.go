package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/codermast/rocketmq-admin-go/protocol/remoting"
)

// =============================================================================
// 高级清理操作
// =============================================================================

// CleanExpiredConsumerQueue 清理过期消费队列
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

// CleanExpiredConsumerQueueByAddr 按地址清理过期消费队列
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

// DeleteExpiredCommitLog 删除过期 CommitLog
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

// DeleteExpiredCommitLogByAddr 按地址删除过期 CommitLog
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

// CleanUnusedTopic 清理未使用 Topic
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

// CleanUnusedTopicByAddr 按地址清理未使用 Topic
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

// =============================================================================
// CommitLog 预读
// =============================================================================

// SetCommitLogReadAheadMode 设置 CommitLog 预读模式
// mode: 0-关闭, 1-顺序预读, 2-随机预读
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

// SetCommitLogReadAheadModeInCluster 在集群中设置 CommitLog 预读模式
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

// =============================================================================
// RocksDB 配置
// =============================================================================

// RocksDBConfig RocksDB 配置
type RocksDBConfig struct {
	BlockCacheSize       int64  `json:"blockCacheSize"`       // 块缓存大小
	WriteBufferSize      int64  `json:"writeBufferSize"`      // 写缓冲区大小
	MaxWriteBufferNumber int    `json:"maxWriteBufferNumber"` // 最大写缓冲区数
	Level0FileNumCompact int    `json:"level0FileNumCompact"` // L0 文件数触发压缩
	MaxBackgroundJobs    int    `json:"maxBackgroundJobs"`    // 最大后台任务数
	CompactionStyle      string `json:"compactionStyle"`      // 压缩风格
}

// ExportRocksDBConfigToJson 导出 RocksDB 配置为 JSON
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

// RocksDBCQWriteProgress RocksDB CQ 写入进度
type RocksDBCQWriteProgress struct {
	Topic       string  `json:"topic"`       // Topic
	QueueId     int     `json:"queueId"`     // 队列 ID
	CqOffset    int64   `json:"cqOffset"`    // CQ 偏移
	Progress    float64 `json:"progress"`    // 进度 (0-100)
	IsCompleted bool    `json:"isCompleted"` // 是否完成
}

// CheckRocksdbCqWriteProgress 检查 RocksDB CQ 写入进度
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

// =============================================================================
// 定时器引擎
// =============================================================================

// SwitchTimerEngine 切换定时器引擎
// role: "master" 或 "slave"
func (c *Client) SwitchTimerEngine(ctx context.Context, brokerAddr, role string) error {
	extFields := map[string]string{
		"role": role,
	}
	cmd := remoting.NewRequest(remoting.SwitchTimerEngine, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}
