package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/codermast/rocketmq-admin-go/protocol/remoting"
)

// =============================================================================
// P3 边缘功能 - 冷数据流控
// =============================================================================

// ColdDataFlowCtrConfig 冷数据流控配置
type ColdDataFlowCtrConfig struct {
	ConsumerGroup   string `json:"consumerGroup"`   // 消费者组
	ThresholdPerSec int64  `json:"thresholdPerSec"` // 每秒阈值
	GlobalThreshold int64  `json:"globalThreshold"` // 全局阈值
	EnableFlowCtr   bool   `json:"enableFlowCtr"`   // 是否启用流控
}

// ColdDataFlowCtrInfo 冷数据流控信息
type ColdDataFlowCtrInfo struct {
	ConsumerGroup    string `json:"consumerGroup"`    // 消费者组
	CurrentQPS       int64  `json:"currentQPS"`       // 当前 QPS
	ThresholdPerSec  int64  `json:"thresholdPerSec"`  // 每秒阈值
	IsFlowCtrEnabled bool   `json:"isFlowCtrEnabled"` // 是否启用
	IsColdData       bool   `json:"isColdData"`       // 是否冷数据
}

// UpdateColdDataFlowCtrGroupConfig 更新冷数据流控配置
func (c *Client) UpdateColdDataFlowCtrGroupConfig(ctx context.Context, brokerAddr string, config ColdDataFlowCtrConfig) error {
	body, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("序列化冷数据流控配置失败: %w", err)
	}

	cmd := remoting.NewRequest(remoting.UpdateColdDataFlowCtrGroupConfig, nil)
	cmd.Body = body

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// RemoveColdDataFlowCtrGroupConfig 移除冷数据流控配置
func (c *Client) RemoveColdDataFlowCtrGroupConfig(ctx context.Context, brokerAddr, consumerGroup string) error {
	extFields := map[string]string{
		"consumerGroup": consumerGroup,
	}
	cmd := remoting.NewRequest(remoting.RemoveColdDataFlowCtrGroupConfig, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// GetColdDataFlowCtrInfo 获取冷数据流控信息
func (c *Client) GetColdDataFlowCtrInfo(ctx context.Context, brokerAddr string) ([]ColdDataFlowCtrInfo, error) {
	cmd := remoting.NewRequest(remoting.GetColdDataFlowCtrInfo, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var infos []ColdDataFlowCtrInfo
	if err := json.Unmarshal(resp.Body, &infos); err != nil {
		return nil, fmt.Errorf("解析冷数据流控信息失败: %w", err)
	}

	return infos, nil
}

// =============================================================================
// P3 边缘功能 - CommitLog 预读
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

// =============================================================================
// P3 边缘功能 - RocksDB 配置
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
// P3 边缘功能 - 定时器引擎
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

// =============================================================================
// P3 边缘功能 - Pop 记录
// =============================================================================

// PopRecord Pop 消费记录
type PopRecord struct {
	Topic         string `json:"topic"`         // Topic
	ConsumerGroup string `json:"consumerGroup"` // 消费者组
	QueueId       int    `json:"queueId"`       // 队列 ID
	StartOffset   int64  `json:"startOffset"`   // 开始偏移
	MsgCount      int    `json:"msgCount"`      // 消息数
	PopTime       int64  `json:"popTime"`       // Pop 时间
	InvisibleTime int64  `json:"invisibleTime"` // 不可见时间
	BornHost      string `json:"bornHost"`      // 客户端地址
}

// ExportPopRecords 导出 Pop 记录
func (c *Client) ExportPopRecords(ctx context.Context, brokerAddr, topic, consumerGroup string) ([]PopRecord, error) {
	extFields := map[string]string{
		"topic":         topic,
		"consumerGroup": consumerGroup,
	}
	cmd := remoting.NewRequest(remoting.ExportPopRecords, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var records []PopRecord
	if err := json.Unmarshal(resp.Body, &records); err != nil {
		return nil, fmt.Errorf("解析 Pop 记录失败: %w", err)
	}

	return records, nil
}

// =============================================================================
// P3 边缘功能 - 搜索偏移（已废弃，保留兼容）
// =============================================================================

// SearchOffset 搜索偏移（已废弃）
// Deprecated: 此方法已废弃，建议直接使用时间戳查询
func (c *Client) SearchOffset(ctx context.Context, brokerAddr, topic string, queueId int, timestamp int64) (int64, error) {
	extFields := map[string]string{
		"topic":     topic,
		"queueId":   fmt.Sprintf("%d", queueId),
		"timestamp": fmt.Sprintf("%d", timestamp),
	}
	cmd := remoting.NewRequest(remoting.SearchOffset, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return 0, err
	}

	if resp.Code != remoting.Success {
		return 0, NewAdminError(resp.Code, resp.Remark)
	}

	var result struct {
		Offset int64 `json:"offset"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return 0, fmt.Errorf("解析偏移结果失败: %w", err)
	}

	return result.Offset, nil
}

// =============================================================================
// 辅助功能 - 集群批量操作
// =============================================================================

// UpdateColdDataFlowCtrGroupConfigInCluster 在集群中更新冷数据流控配置
func (c *Client) UpdateColdDataFlowCtrGroupConfigInCluster(ctx context.Context, clusterName string, config ColdDataFlowCtrConfig) error {
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
			if err := c.UpdateColdDataFlowCtrGroupConfig(ctx, brokerAddr, config); err != nil {
				return fmt.Errorf("更新 %s 冷数据流控失败: %w", brokerAddr, err)
			}
		}
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
