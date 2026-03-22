package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/apache/rocketmq-client-go/v2/primitive"
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
// 搜索偏移
// =============================================================================

// SearchOffset 根据时间戳搜索指定队列的偏移量
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

	// RocketMQ 的 SearchOffset 响应将 offset 放在 ExtFields 中
	offsetStr, ok := resp.ExtFields["offset"]
	if ok && offsetStr != "" {
		offset, parseErr := strconv.ParseInt(offsetStr, 10, 64)
		if parseErr == nil {
			return offset, nil
		}
	}

	// 兼容：尝试从 Body JSON 解析
	if len(resp.Body) > 0 {
		var result struct {
			Offset int64 `json:"offset"`
		}
		if err := json.Unmarshal(resp.Body, &result); err == nil {
			return result.Offset, nil
		}
	}

	return 0, fmt.Errorf("解析偏移结果失败: 响应中未包含 offset 字段")
}

// =============================================================================
// 按偏移量拉取消息
// =============================================================================

// PullMessageResult 拉取消息结果
type PullMessageResult struct {
	Messages       []*MessageExt // 消息列表
	NextBeginOffset int64        // 下次拉取起始偏移量
	MinOffset      int64         // 队列最小偏移量
	MaxOffset      int64         // 队列最大偏移量
}

// PullMessage 从指定 Broker 的指定队列、指定偏移量拉取消息
func (c *Client) PullMessage(ctx context.Context, brokerAddr, topic string, queueId int, offset int64, maxMsgNums int) (*PullMessageResult, error) {
	// sysFlag = 6: bit1(suspend=true) | bit2(subscription=true)
	extFields := map[string]string{
		"consumerGroup":        "TOOLS_CONSUMER",
		"topic":                topic,
		"queueId":              fmt.Sprintf("%d", queueId),
		"queueOffset":          fmt.Sprintf("%d", offset),
		"maxMsgNums":           fmt.Sprintf("%d", maxMsgNums),
		"sysFlag":              "6",
		"subExpression":        "*",
		"expressionType":       "TAG",
		"subVersion":           "0",
		"commitOffset":         "0",
		"suspendTimeoutMillis": "0",
	}
	cmd := remoting.NewRequest(remoting.PullMessage, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	result := &PullMessageResult{}

	// 解析 ExtFields 中的偏移量信息
	if v, ok := resp.ExtFields["nextBeginOffset"]; ok {
		result.NextBeginOffset, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := resp.ExtFields["minOffset"]; ok {
		result.MinOffset, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := resp.ExtFields["maxOffset"]; ok {
		result.MaxOffset, _ = strconv.ParseInt(v, 10, 64)
	}

	// resp.Code == 0(FOUND) 表示找到消息，Body 中包含二进制编码的消息数据
	if resp.Code == remoting.Success && len(resp.Body) > 0 {
		// 使用 rocketmq-client-go 的 DecodeMessage 解码二进制消息
		decodedMsgs := primitive.DecodeMessage(resp.Body)
		for _, pm := range decodedMsgs {
			msg := &MessageExt{
				Topic:          pm.Topic,
				QueueId:        pm.Queue.QueueId,
				QueueOffset:    pm.QueueOffset,
				MsgId:          pm.MsgId,
				OffsetMsgId:    pm.OffsetMsgId,
				Body:           pm.Body,
				Flag:           int(pm.Flag),
				BornTimestamp:  pm.BornTimestamp,
				StoreTimestamp: pm.StoreTimestamp,
				BornHost:       pm.BornHost,
				StoreHost:      pm.StoreHost,
				SysFlag:        int(pm.SysFlag),
				Properties:     pm.GetProperties(),
			}
			result.Messages = append(result.Messages, msg)
		}
	}
	// resp.Code == 1(NO_NEW_MSG) 或其他码表示没有更多消息，返回空列表即可

	return result, nil
}

// =============================================================================
// 按时间范围浏览消息
// =============================================================================

// QueryMessageByTime 按时间范围浏览消息
// beginTime/endTime 为 Unix 毫秒时间戳，maxNum 为最大返回数量
func (c *Client) QueryMessageByTime(ctx context.Context, topic string, beginTime, endTime int64, maxNum int) ([]*MessageExt, error) {
	routeData, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("获取 Topic 路由信息失败: %w", err)
	}

	if maxNum <= 0 {
		maxNum = 32
	}

	// 收集所有 (brokerAddr, queueId) 对
	type queueInfo struct {
		brokerAddr string
		queueId    int
	}
	var queues []queueInfo

	for _, qd := range routeData.QueueDatas {
		// 找到该 Broker 的 Master 地址
		var brokerAddr string
		for _, bd := range routeData.BrokerDatas {
			if bd.BrokerName == qd.BrokerName {
				brokerAddr = bd.BrokerAddrs["0"] // Master
				break
			}
		}
		if brokerAddr == "" {
			continue
		}

		for i := 0; i < qd.ReadQueueNums; i++ {
			queues = append(queues, queueInfo{brokerAddr: brokerAddr, queueId: i})
		}
	}

	if len(queues) == 0 {
		return nil, fmt.Errorf("未找到可用的消息队列")
	}

	// 并发查询每个队列
	type queueResult struct {
		msgs []*MessageExt
		err  error
	}
	results := make([]queueResult, len(queues))
	var wg sync.WaitGroup

	perQueueLimit := maxNum/len(queues) + 1
	if perQueueLimit < 4 {
		perQueueLimit = 4
	}

	for i, q := range queues {
		wg.Add(1)
		go func(idx int, qi queueInfo) {
			defer wg.Done()

			// 1. 根据 beginTime 定位起始偏移量
			startOffset, err := c.SearchOffset(ctx, qi.brokerAddr, topic, qi.queueId, beginTime)
			if err != nil {
				results[idx] = queueResult{err: err}
				return
			}

			// 2. 从起始偏移量拉取消息
			var collected []*MessageExt
			currentOffset := startOffset

			for len(collected) < perQueueLimit {
				batchSize := perQueueLimit - len(collected)
				if batchSize > 32 {
					batchSize = 32
				}

				pullResult, pullErr := c.PullMessage(ctx, qi.brokerAddr, topic, qi.queueId, currentOffset, batchSize)
				if pullErr != nil {
					// 拉取失败不阻塞整体，返回已收集的消息
					break
				}

				if len(pullResult.Messages) == 0 {
					break
				}

				reachedEnd := false
				for _, msg := range pullResult.Messages {
					// 过滤超出 endTime 的消息
					if endTime > 0 && msg.StoreTimestamp > endTime {
						reachedEnd = true
						break
					}
					collected = append(collected, msg)
				}

				if reachedEnd {
					break
				}

				// 更新偏移量继续拉取
				if pullResult.NextBeginOffset <= currentOffset {
					break // 防止死循环
				}
				currentOffset = pullResult.NextBeginOffset
			}

			results[idx] = queueResult{msgs: collected}
		}(i, q)
	}

	wg.Wait()

	// 汇总所有队列的消息
	var allMessages []*MessageExt
	for _, r := range results {
		if r.err != nil {
			continue // 跳过失败的队列
		}
		allMessages = append(allMessages, r.msgs...)
	}

	// 按存储时间排序
	sort.Slice(allMessages, func(i, j int) bool {
		return allMessages[i].StoreTimestamp < allMessages[j].StoreTimestamp
	})

	// 截断到 maxNum
	if len(allMessages) > maxNum {
		allMessages = allMessages[:maxNum]
	}

	return allMessages, nil
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

		// QueryMessage 的响应体也是二进制编码的消息，使用 DecodeMessage 解码
		if len(resp.Body) > 0 {
			decodedMsgs := primitive.DecodeMessage(resp.Body)
			for _, pm := range decodedMsgs {
				msg := &MessageExt{
					Topic:          pm.Topic,
					QueueId:        pm.Queue.QueueId,
					QueueOffset:    pm.QueueOffset,
					MsgId:          pm.MsgId,
					OffsetMsgId:    pm.OffsetMsgId,
					Body:           pm.Body,
					Flag:           int(pm.Flag),
					BornTimestamp:  pm.BornTimestamp,
					StoreTimestamp: pm.StoreTimestamp,
					BornHost:       pm.BornHost,
					StoreHost:      pm.StoreHost,
					SysFlag:        int(pm.SysFlag),
					Properties:     pm.GetProperties(),
				}
				allMessages = append(allMessages, msg)
			}
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
