package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/codermast/rocketmq-admin-go/protocol/remoting"
)

// =============================================================================
// 消费队列查询
// =============================================================================

// ConsumeQueueData 消费队列数据
type ConsumeQueueData struct {
	PhysicalOffset int64  `json:"physicOffset"` // 物理偏移
	Size           int32  `json:"size"`         // 大小
	TagsCode       int64  `json:"tagsCode"`     // Tags 哈希码
	ExtendData     string `json:"extendData"`   // 扩展数据
	BitMap         string `json:"bitMap"`       // 位图
	Eval           bool   `json:"eval"`         // 是否有效
	Msg            string `json:"msg"`          // 消息
}

// QueryConsumeQueue 查询消费队列
func (c *Client) QueryConsumeQueue(ctx context.Context, brokerAddr, topic string, queueId int, index, count int, consumerGroup string) ([]ConsumeQueueData, error) {
	extFields := map[string]string{
		"topic":         topic,
		"queueId":       fmt.Sprintf("%d", queueId),
		"index":         fmt.Sprintf("%d", index),
		"count":         fmt.Sprintf("%d", count),
		"consumerGroup": consumerGroup,
	}
	cmd := remoting.NewRequest(remoting.QueryConsumeQueue, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var wrapper struct {
		QueueData []ConsumeQueueData `json:"queueData"`
	}
	if err := json.Unmarshal(resp.Body, &wrapper); err != nil {
		return nil, fmt.Errorf("解析消费队列数据失败: %w", err)
	}

	return wrapper.QueueData, nil
}

// =============================================================================
// 消息高级操作
// =============================================================================

// ConsumeMessageDirectlyResult 直接消费消息结果
type ConsumeMessageDirectlyResult struct {
	Order          bool   `json:"order"`          // 是否顺序消费
	AutoCommit     bool   `json:"autoCommit"`     // 是否自动提交
	SpentTimeMills int64  `json:"spentTimeMills"` // 消费耗时
	ConsumeResult  string `json:"consumeResult"`  // 消费结果
	Remark         string `json:"remark"`         // 备注
}

// ConsumeMessageDirectly 直接消费消息
func (c *Client) ConsumeMessageDirectly(ctx context.Context, consumerGroup, clientId, topic, msgId string) (*ConsumeMessageDirectlyResult, error) {
	extFields := map[string]string{
		"consumerGroup": consumerGroup,
		"clientId":      clientId,
		"topic":         topic,
		"msgId":         msgId,
	}
	cmd := remoting.NewRequest(remoting.ConsumeMessageDirectly, extFields)

	// 获取集群信息找到 Broker
	clusterInfo, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return nil, err
	}

	for _, brokerData := range clusterInfo.BrokerAddrTable {
		var brokerAddr string
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}

		resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
		if err != nil {
			continue
		}

		if resp.Code != remoting.Success {
			continue
		}

		var result ConsumeMessageDirectlyResult
		if err := json.Unmarshal(resp.Body, &result); err != nil {
			continue
		}

		return &result, nil
	}

	return nil, fmt.Errorf("消费消息失败")
}

// ResumeCheckHalfMessage 恢复检查半消息
func (c *Client) ResumeCheckHalfMessage(ctx context.Context, topic, msgId string) (bool, error) {
	extFields := map[string]string{
		"topic": topic,
		"msgId": msgId,
	}
	cmd := remoting.NewRequest(remoting.ResumeCheckHalfMessage, extFields)

	// 获取路由信息
	routeData, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return false, err
	}

	for _, brokerData := range routeData.BrokerDatas {
		var brokerAddr string
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}

		resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
		if err != nil {
			continue
		}

		if resp.Code == remoting.Success {
			return true, nil
		}
	}

	return false, fmt.Errorf("恢复半消息失败")
}

// SetMessageRequestMode 设置消息请求模式
func (c *Client) SetMessageRequestMode(ctx context.Context, brokerAddr, topic, consumerGroup string, mode int, popShareQueueNum int) error {
	extFields := map[string]string{
		"topic":            topic,
		"consumerGroup":    consumerGroup,
		"mode":             fmt.Sprintf("%d", mode),
		"popShareQueueNum": fmt.Sprintf("%d", popShareQueueNum),
	}
	cmd := remoting.NewRequest(remoting.SetMessageRequestMode, extFields)

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
// Pop 记录
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
// 搜索偏移（已废弃）
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
// 消息查询与详情
// =============================================================================

// QueryMessage 按 Key 查询消息
func (c *Client) QueryMessage(ctx context.Context, topic, key string, maxNum int, begin, end int64) ([]*MessageExt, error) {
	routeData, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, err
	}

	var allMessages []*MessageExt
	for _, brokerData := range routeData.BrokerDatas {
		// 仅查询 Master
		brokerAddr := brokerData.BrokerAddrs["0"]
		if brokerAddr == "" {
			continue
		}

		extFields := map[string]string{
			"topic":  topic,
			"key":    key,
			"maxNum": fmt.Sprintf("%d", maxNum),
			"begin":  fmt.Sprintf("%d", begin),
			"end":    fmt.Sprintf("%d", end),
		}

		cmd := remoting.NewRequest(remoting.QueryMessage, extFields)
		resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
		if err != nil {
			continue
		}

		if resp.Code != remoting.Success {
			continue
		}

		var msgs []*MessageExt
		if err := json.Unmarshal(resp.Body, &msgs); err == nil {
			allMessages = append(allMessages, msgs...)
		}
	}

	return allMessages, nil
}

// ViewMessage 按 ID 查询消息详情
func (c *Client) ViewMessage(ctx context.Context, topic, msgId string) (*MessageExt, error) {
	routeData, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, err
	}

	extFields := map[string]string{
		"topic": topic,
		"msgId": msgId,
	}
	cmd := remoting.NewRequest(remoting.ViewMessageById, extFields)

	for _, brokerData := range routeData.BrokerDatas {
		// 查询任意可用节点（虽然 ID 可能指定了存储节点，但简单遍历也是一种策略）
		// 更严谨的做法是解析 offsetMsgId 找到具体 broker，或者遍历所有 master
		for _, brokerAddr := range brokerData.BrokerAddrs {
			resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
			if err != nil {
				continue
			}

			if resp.Code == remoting.Success {
				var msg MessageExt
				if err := json.Unmarshal(resp.Body, &msg); err == nil {
					return &msg, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("未找到消息: %s", msgId)
}
