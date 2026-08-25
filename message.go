package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

// ConsumeQueueData is one entry of a broker's consume queue index.
type ConsumeQueueData struct {
	PhysicalOffset int64  `json:"physicOffset"` // offset into the CommitLog
	Size           int32  `json:"size"`
	TagsCode       int64  `json:"tagsCode"` // tag hash, used for broker-side filtering
	ExtendData     string `json:"extendData"`
	BitMap         string `json:"bitMap"`
	Eval           bool   `json:"eval"`
	Msg            string `json:"msg"`
}

// QueryConsumeQueue returns consume queue entries starting at index.
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

// ConsumeMessageDirectlyResult is the outcome of a forced re-consumption.
type ConsumeMessageDirectlyResult struct {
	Order          bool   `json:"order"`
	AutoCommit     bool   `json:"autoCommit"`
	SpentTimeMills int64  `json:"spentTimeMills"`
	ConsumeResult  string `json:"consumeResult"`
	Remark         string `json:"remark"`
}

// ConsumeMessageDirectly makes one consumer client re-consume a message now.
func (c *Client) ConsumeMessageDirectly(ctx context.Context, consumerGroup, clientId, topic, msgId string) (*ConsumeMessageDirectlyResult, error) {
	extFields := map[string]string{
		"consumerGroup": consumerGroup,
		"clientId":      clientId,
		"topic":         topic,
		"msgId":         msgId,
	}
	cmd := remoting.NewRequest(remoting.ConsumeMessageDirectly, extFields)

	// Any Broker in the cluster can route the request; try them in turn.
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

// ResumeCheckHalfMessage retriggers the transaction check on a half message.
func (c *Client) ResumeCheckHalfMessage(ctx context.Context, topic, msgId string) (bool, error) {
	extFields := map[string]string{
		"topic": topic,
		"msgId": msgId,
	}
	cmd := remoting.NewRequest(remoting.ResumeCheckHalfMessage, extFields)

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

// SetMessageRequestMode switches a topic/group between pull and pop consumption.
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

// MessageTrackDetail reports, for each consumer group subscribed to the
// message's topic, whether that group consumed it.
func (c *Client) MessageTrackDetail(ctx context.Context, msg *MessageExt) ([]MessageTrack, error) {
	if msg == nil {
		return nil, fmt.Errorf("消息不能为空")
	}

	groups, err := c.QueryTopicConsumeByWho(ctx, msg.Topic)
	if err != nil {
		return nil, fmt.Errorf("查询 Topic 消费者失败: %w", err)
	}

	var tracks []MessageTrack
	for _, group := range groups {
		track := MessageTrack{
			ConsumerGroup: group,
			TrackType:     "UNKNOWN",
		}

		// A group with no live connection cannot be tracked; report it as such.
		connInfo, err := c.ExamineConsumerConnectionInfo(ctx, group)
		if err != nil {
			track.TrackType = "NOT_ONLINE"
			track.ExceptionDesc = err.Error()
			tracks = append(tracks, track)
			continue
		}

		if connInfo.ConsumeType == "CONSUME_ACTIVELY" {
			track.TrackType = "PULL"
		} else {
			if sub, ok := connInfo.SubscriptionTable[msg.Topic]; ok {
				if sub.ExpressionType == "TAG" {
					if sub.SubString == "*" {
						track.TrackType = "CONSUMED"
					} else {
						track.TrackType = "CONSUMED"
					}
				} else {
					track.TrackType = "CONSUMED"
				}
			} else {
				track.TrackType = "NOT_CONSUME_YET"
			}
		}

		tracks = append(tracks, track)
	}

	return tracks, nil
}

// SearchOffset returns the offset of the first message stored at or after
// timestamp in one queue.
func (c *Client) SearchOffset(ctx context.Context, brokerAddr, topic string, queueId int, timestamp int64) (int64, error) {
	extFields := map[string]string{
		"topic":     topic,
		"queueId":   fmt.Sprintf("%d", queueId),
		"timestamp": fmt.Sprintf("%d", timestamp),
	}
	cmd := remoting.NewRequest(remoting.SearchOffsetByTimestamp, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return 0, err
	}

	if resp.Code != remoting.Success {
		return 0, NewAdminError(resp.Code, resp.Remark)
	}

	// RocketMQ puts the offset in ExtFields, not the body.
	offsetStr, ok := resp.ExtFields["offset"]
	if ok && offsetStr != "" {
		offset, parseErr := strconv.ParseInt(offsetStr, 10, 64)
		if parseErr == nil {
			return offset, nil
		}
	}

	// Some versions answer with a JSON body instead.
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

// PullMessageResult is one batch of pulled messages plus the queue's bounds.
type PullMessageResult struct {
	Messages        []*MessageExt
	NextBeginOffset int64 // where to resume pulling
	MinOffset       int64
	MaxOffset       int64
}

// PullMessage pulls up to maxMsgNums messages from one queue at one offset.
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

	// Offsets come back in ExtFields, not the body.
	if v, ok := resp.ExtFields["nextBeginOffset"]; ok {
		result.NextBeginOffset, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := resp.ExtFields["minOffset"]; ok {
		result.MinOffset, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := resp.ExtFields["maxOffset"]; ok {
		result.MaxOffset, _ = strconv.ParseInt(v, 10, 64)
	}

	// Success (FOUND) means the body holds binary-encoded messages.
	if resp.Code == remoting.Success && len(resp.Body) > 0 {
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
	// Any other code means no new message; an empty result is correct.

	return result, nil
}

// QueryMessageByTime returns up to maxNum messages stored between beginTime
// and endTime, given as Unix milliseconds. Queues are pulled concurrently and
// the result is sorted by store time.
func (c *Client) QueryMessageByTime(ctx context.Context, topic string, beginTime, endTime int64, maxNum int) ([]*MessageExt, error) {
	routeData, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("获取 Topic 路由信息失败: %w", err)
	}

	if maxNum <= 0 {
		maxNum = 32
	}

	// Collect every (brokerAddr, queueId) pair the topic routes to.
	type queueInfo struct {
		brokerAddr string
		queueId    int
	}
	var queues []queueInfo

	for _, qd := range routeData.QueueDatas {
		// Broker id "0" is the master.
		var brokerAddr string
		for _, bd := range routeData.BrokerDatas {
			if bd.BrokerName == qd.BrokerName {
				brokerAddr = bd.BrokerAddrs["0"]
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

	// Pull each queue concurrently.
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

			// Locate where beginTime falls in this queue.
			startOffset, err := c.SearchOffset(ctx, qi.brokerAddr, topic, qi.queueId, beginTime)
			if err != nil {
				results[idx] = queueResult{err: err}
				return
			}

			var collected []*MessageExt
			currentOffset := startOffset

			for len(collected) < perQueueLimit {
				batchSize := perQueueLimit - len(collected)
				if batchSize > 32 {
					batchSize = 32
				}

				pullResult, pullErr := c.PullMessage(ctx, qi.brokerAddr, topic, qi.queueId, currentOffset, batchSize)
				if pullErr != nil {
					// A failed pull must not sink the whole query; keep what we have.
					break
				}

				if len(pullResult.Messages) == 0 {
					break
				}

				reachedEnd := false
				for _, msg := range pullResult.Messages {
					// Stop this queue once messages pass endTime.
					if endTime > 0 && msg.StoreTimestamp > endTime {
						reachedEnd = true
						break
					}
					collected = append(collected, msg)
				}

				if reachedEnd {
					break
				}

				if pullResult.NextBeginOffset <= currentOffset {
					break // NextBeginOffset did not advance; stop rather than spin
				}
				currentOffset = pullResult.NextBeginOffset
			}

			results[idx] = queueResult{msgs: collected}
		}(i, q)
	}

	wg.Wait()

	var allMessages []*MessageExt
	for _, r := range results {
		if r.err != nil {
			continue
		}
		allMessages = append(allMessages, r.msgs...)
	}

	sort.Slice(allMessages, func(i, j int) bool {
		return allMessages[i].StoreTimestamp < allMessages[j].StoreTimestamp
	})

	if len(allMessages) > maxNum {
		allMessages = allMessages[:maxNum]
	}

	return allMessages, nil
}

// QueryMessage returns messages carrying key, stored between begin and end.
func (c *Client) QueryMessage(ctx context.Context, topic, key string, maxNum int, begin, end int64) ([]*MessageExt, error) {
	routeData, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, err
	}

	var allMessages []*MessageExt
	for _, brokerData := range routeData.BrokerDatas {
		// Only masters answer message queries.
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

		// The response body is binary-encoded, same as a pull.
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

// ViewMessage returns one message by id.
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
		// The id may name its storage node, but walking every node is simpler.
		// A stricter version would parse offsetMsgId, or scan masters only.
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
