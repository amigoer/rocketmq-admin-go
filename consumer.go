package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

// =============================================================================
// 消费者组管理接口
// =============================================================================

// CreateSubscriptionGroup 创建订阅组
func (c *Client) CreateSubscriptionGroup(ctx context.Context, addr string, config SubscriptionGroupConfig) error {
	extFields := map[string]string{
		"groupName":                      config.GroupName,
		"consumeEnable":                  fmt.Sprintf("%t", config.ConsumeEnable),
		"consumeFromMinEnable":           fmt.Sprintf("%t", config.ConsumeFromMinEnable),
		"consumeBroadcastEnable":         fmt.Sprintf("%t", config.ConsumeBroadcastEnable),
		"retryQueueNums":                 fmt.Sprintf("%d", config.RetryQueueNums),
		"retryMaxTimes":                  fmt.Sprintf("%d", config.RetryMaxTimes),
		"brokerId":                       fmt.Sprintf("%d", config.BrokerId),
		"whichBrokerWhenConsumeSlowly":   fmt.Sprintf("%d", config.WhichBrokerWhenConsumeSlowly),
		"notifyConsumerIdsChangedEnable": fmt.Sprintf("%t", config.NotifyConsumerIdsChangedEnable),
	}

	cmd := remoting.NewRequest(remoting.UpdateAndCreateSubscriptionGroup, extFields)

	resp, err := c.invokeBroker(ctx, addr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// DeleteSubscriptionGroup 删除订阅组
func (c *Client) DeleteSubscriptionGroup(ctx context.Context, addr, groupName string) error {
	extFields := map[string]string{
		"groupName": groupName,
	}
	cmd := remoting.NewRequest(remoting.DeleteSubscriptionGroup, extFields)

	resp, err := c.invokeBroker(ctx, addr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// ExamineSubscriptionGroupConfig 查询订阅组配置
func (c *Client) ExamineSubscriptionGroupConfig(ctx context.Context, addr, group string) (*SubscriptionGroupConfig, error) {
	extFields := map[string]string{
		"group": group,
	}
	cmd := remoting.NewRequest(remoting.GetSubscriptionGroupConfig, extFields)

	resp, err := c.invokeBroker(ctx, addr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var config SubscriptionGroupConfig
	if err := json.Unmarshal(resp.Body, &config); err != nil {
		return nil, fmt.Errorf("解析订阅组配置失败: %w", err)
	}

	return &config, nil
}

// ExamineConsumeStats 查询消费统计
func (c *Client) ExamineConsumeStats(ctx context.Context, consumerGroup string) (*ConsumeStats, error) {
	// 先获取集群信息
	clusterInfo, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return nil, err
	}

	// 遍历所有 Broker 查询消费统计
	result := &ConsumeStats{
		OffsetTable: make(map[string]*OffsetWrapper),
	}

	for _, brokerData := range clusterInfo.BrokerAddrTable {
		var brokerAddr string
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}

		if brokerAddr == "" {
			continue
		}

		extFields := map[string]string{
			"consumerGroup": consumerGroup,
		}
		cmd := remoting.NewRequest(remoting.GetConsumeStats, extFields)

		resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
		if err != nil {
			continue
		}

		if resp.Code != remoting.Success {
			continue
		}

		var stats ConsumeStats
		if err := json.Unmarshal(resp.Body, &stats); err != nil {
			continue
		}

		// 合并结果
		for k, v := range stats.OffsetTable {
			result.OffsetTable[k] = v
		}
		result.ConsumeTps += stats.ConsumeTps
	}

	return result, nil
}

// ExamineConsumerConnectionInfo 查询消费者连接信息
func (c *Client) ExamineConsumerConnectionInfo(ctx context.Context, consumerGroup string) (*ConsumerConnection, error) {
	// 先获取集群信息
	clusterInfo, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return nil, err
	}

	// 尝试从任意 Broker 获取消费者连接信息
	for _, brokerData := range clusterInfo.BrokerAddrTable {
		var brokerAddr string
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}

		if brokerAddr == "" {
			continue
		}

		extFields := map[string]string{
			"consumerGroup": consumerGroup,
		}
		cmd := remoting.NewRequest(remoting.GetConsumerConnectionList, extFields)

		resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
		if err != nil {
			continue
		}

		if resp.Code == remoting.ConsumerNotOnline {
			return nil, ErrConsumerGroupNotFound
		}

		if resp.Code != remoting.Success {
			continue
		}

		var connInfo ConsumerConnection
		if err := json.Unmarshal(resp.Body, &connInfo); err != nil {
			continue
		}

		return &connInfo, nil
	}

	return nil, ErrConsumerGroupNotFound
}

// ExamineConsumeStatsByTopic 按 Topic 过滤查询消费统计
func (c *Client) ExamineConsumeStatsByTopic(ctx context.Context, consumerGroup, topic string) (*ConsumeStats, error) {
	// 获取集群信息
	clusterInfo, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return nil, err
	}

	result := &ConsumeStats{
		OffsetTable: make(map[string]*OffsetWrapper),
	}

	for _, brokerData := range clusterInfo.BrokerAddrTable {
		var brokerAddr string
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}
		if brokerAddr == "" {
			continue
		}

		extFields := map[string]string{
			"consumerGroup": consumerGroup,
			"topic":         topic,
		}
		cmd := remoting.NewRequest(remoting.GetConsumeStats, extFields)

		resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
		if err != nil {
			continue
		}
		if resp.Code != remoting.Success {
			continue
		}

		var stats ConsumeStats
		if err := json.Unmarshal(resp.Body, &stats); err != nil {
			continue
		}

		for k, v := range stats.OffsetTable {
			result.OffsetTable[k] = v
		}
		result.ConsumeTps += stats.ConsumeTps
	}

	return result, nil
}

// FetchConsumeStatsInBroker 获取单个 Broker 上的所有消费统计
// Java: GET_BROKER_CONSUME_STATS = 317
func (c *Client) FetchConsumeStatsInBroker(ctx context.Context, brokerAddr string, isOrder bool) (*ConsumeStatsList, error) {
	extFields := map[string]string{
		"isOrder": fmt.Sprintf("%t", isOrder),
	}
	cmd := remoting.NewRequest(remoting.GetBrokerConsumeStats, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var result ConsumeStatsList
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return nil, fmt.Errorf("解析 Broker 消费统计失败: %w", err)
	}

	return &result, nil
}

// QuerySubscription 查询消费者对某 Topic 的订阅详情
// Java: QUERY_SUBSCRIPTION_BY_CONSUMER = 345
func (c *Client) QuerySubscription(ctx context.Context, consumerGroup, topic string) (*SubscriptionData, error) {
	extFields := map[string]string{
		"group": consumerGroup,
		"topic": topic,
	}
	cmd := remoting.NewRequest(remoting.QuerySubscription, extFields)

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
		if brokerAddr == "" {
			continue
		}

		resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
		if err != nil {
			continue
		}
		if resp.Code != remoting.Success {
			continue
		}

		var sub SubscriptionData
		if err := json.Unmarshal(resp.Body, &sub); err != nil {
			continue
		}

		return &sub, nil
	}

	return nil, fmt.Errorf("未找到消费者组 %s 对 Topic %s 的订阅信息", consumerGroup, topic)
}

// GetConsumeStatus 获取消费状态（每个 queue 的 offset）
// Java: INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223
// 通过 broker 转发到 consumer 客户端获取实时消费状态
func (c *Client) GetConsumeStatus(ctx context.Context, topic, consumerGroup, clientAddr string) (map[string]map[string]int64, error) {
	extFields := map[string]string{
		"topic": topic,
		"group": consumerGroup,
	}
	if clientAddr != "" {
		extFields["clientAddr"] = clientAddr
	}
	cmd := remoting.NewRequest(remoting.InvokeBrokerToGetConsumerStatus, extFields)

	// 获取 Topic 路由找到 Broker
	routeData, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, err
	}

	result := make(map[string]map[string]int64)

	for _, brokerData := range routeData.BrokerDatas {
		brokerAddr := brokerData.BrokerAddrs["0"]
		if brokerAddr == "" {
			continue
		}

		resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
		if err != nil {
			continue
		}
		if resp.Code != remoting.Success {
			continue
		}

		// 响应格式: map[clientId] -> map[queueKey] -> offset
		var statusTable map[string]map[string]int64
		if err := json.Unmarshal(resp.Body, &statusTable); err != nil {
			continue
		}

		for k, v := range statusTable {
			result[k] = v
		}
	}

	return result, nil
}

// =============================================================================
// Offset 管理接口
// =============================================================================

// ResetOffsetByTimestamp 按时间戳重置消费位点
func (c *Client) ResetOffsetByTimestamp(ctx context.Context, topic, group string, timestamp int64, force bool) (map[MessageQueue]int64, error) {
	// 获取 Topic 路由信息
	routeData, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, err
	}

	result := make(map[MessageQueue]int64)

	for _, brokerData := range routeData.BrokerDatas {
		var brokerAddr string
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}

		if brokerAddr == "" {
			continue
		}

		extFields := map[string]string{
			"topic":     topic,
			"group":     group,
			"timestamp": fmt.Sprintf("%d", timestamp),
			"isForce":   fmt.Sprintf("%t", force),
		}
		cmd := remoting.NewRequest(remoting.ResetConsumerOffset, extFields)

		resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
		if err != nil {
			continue
		}

		if resp.Code != remoting.Success {
			continue
		}

		// 解析重置结果
		var offsetTable map[string]int64
		if err := json.Unmarshal(resp.Body, &offsetTable); err != nil {
			continue
		}

		// 转换为 MessageQueue 格式
		// queueKey 格式: {"brokerName":"xxx","queueId":0,"topic":"xxx"} 或 brokerName-queueId
		for queueKey, offset := range offsetTable {
			mq := MessageQueue{
				Topic:      topic,
				BrokerName: brokerData.BrokerName,
			}
			// 尝试从 JSON 格式解析
			var mqParsed MessageQueue
			if err := json.Unmarshal([]byte(queueKey), &mqParsed); err == nil {
				mq = mqParsed
			} else {
				// 尝试从简单格式解析 queueId
				parts := strings.Split(queueKey, "-")
				if len(parts) >= 2 {
					if qid, err := strconv.Atoi(parts[len(parts)-1]); err == nil {
						mq.QueueId = qid
					}
				}
			}
			result[mq] = offset
		}
	}

	return result, nil
}

// =============================================================================
// 消费者管理扩展
// =============================================================================

// GetConsumerRunningInfo 获取消费者运行时信息
func (c *Client) GetConsumerRunningInfo(ctx context.Context, consumerGroup, clientId string, jstack bool) (*ConsumerRunningInfo, error) {
	// 先获取消费者连接信息
	connInfo, err := c.ExamineConsumerConnectionInfo(ctx, consumerGroup)
	if err != nil {
		return nil, err
	}

	if len(connInfo.ConnectionSet) == 0 {
		return nil, ErrConsumerGroupNotFound
	}

	// 找到目标客户端
	var targetConn *Connection
	for _, conn := range connInfo.ConnectionSet {
		if clientId == "" || conn.ClientId == clientId {
			targetConn = conn
			break
		}
	}

	if targetConn == nil {
		return nil, fmt.Errorf("客户端 %s 未找到", clientId)
	}

	// 向客户端发送请求获取运行时信息
	extFields := map[string]string{
		"consumerGroup": consumerGroup,
		"clientId":      targetConn.ClientId,
		"jstackEnable":  fmt.Sprintf("%t", jstack),
	}
	cmd := remoting.NewRequest(remoting.GetConsumerRunningInfo, extFields)

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

		var runningInfo ConsumerRunningInfo
		if err := json.Unmarshal(resp.Body, &runningInfo); err != nil {
			continue
		}

		return &runningInfo, nil
	}

	return nil, fmt.Errorf("获取消费者运行信息失败")
}

// QueryTopicsByConsumer 查询消费者订阅的 Topic
func (c *Client) QueryTopicsByConsumer(ctx context.Context, consumerGroup string) (*TopicList, error) {
	extFields := map[string]string{
		"consumerGroup": consumerGroup,
	}
	cmd := remoting.NewRequest(remoting.QueryTopicsByConsumer, extFields)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var topicList TopicList
	if err := json.Unmarshal(resp.Body, &topicList); err != nil {
		return nil, fmt.Errorf("解析 Topic 列表失败: %w", err)
	}

	return &topicList, nil
}

// QueryConsumeTimeSpan 查询消费时间跨度
func (c *Client) QueryConsumeTimeSpan(ctx context.Context, topic, consumerGroup string) ([]ConsumeTimeSpan, error) {
	// 先获取 Topic 路由信息
	routeData, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, err
	}

	var result []ConsumeTimeSpan

	for _, brokerData := range routeData.BrokerDatas {
		var brokerAddr string
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}

		extFields := map[string]string{
			"topic":         topic,
			"consumerGroup": consumerGroup,
		}
		cmd := remoting.NewRequest(remoting.QueryConsumeTimeSpan, extFields)

		resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
		if err != nil {
			continue
		}

		if resp.Code != remoting.Success {
			continue
		}

		var spans []ConsumeTimeSpan
		if err := json.Unmarshal(resp.Body, &spans); err != nil {
			continue
		}

		result = append(result, spans...)
	}

	return result, nil
}

// GetAllSubscriptionGroup 获取所有订阅组
func (c *Client) GetAllSubscriptionGroup(ctx context.Context, brokerAddr string) (map[string]*SubscriptionGroupConfig, error) {
	cmd := remoting.NewRequest(remoting.GetAllSubscriptionGroupConfig, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var wrapper struct {
		SubscriptionGroupTable map[string]*SubscriptionGroupConfig `json:"subscriptionGroupTable"`
	}
	if err := json.Unmarshal(resp.Body, &wrapper); err != nil {
		return nil, fmt.Errorf("解析订阅组列表失败: %w", err)
	}

	return wrapper.SubscriptionGroupTable, nil
}

// UpdateConsumeOffset 更新消费 Offset
func (c *Client) UpdateConsumeOffset(ctx context.Context, brokerAddr, consumerGroup, topic string, queueId int, offset int64) error {
	extFields := map[string]string{
		"consumerGroup": consumerGroup,
		"topic":         topic,
		"queueId":       fmt.Sprintf("%d", queueId),
		"commitOffset":  fmt.Sprintf("%d", offset),
	}
	cmd := remoting.NewRequest(remoting.UpdateConsumerOffset, extFields)

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
// 高级消费者操作 (并发/批量)
// =============================================================================

// ExamineConsumeStatsConcurrent 并发查询消费统计
func (c *Client) ExamineConsumeStatsConcurrent(ctx context.Context, consumerGroup, topic string) (*ConsumeStats, error) {
	// 内部实现与 ExamineConsumeStats 相同，但可扩展为真正并发
	return c.ExamineConsumeStats(ctx, consumerGroup)
}

// QueryConsumeTimeSpanConcurrent 并发查询消费时间跨度
func (c *Client) QueryConsumeTimeSpanConcurrent(ctx context.Context, topic, consumerGroup string) ([]ConsumeTimeSpan, error) {
	return c.QueryConsumeTimeSpan(ctx, topic, consumerGroup)
}

// QueryTopicsByConsumerConcurrent 并发查询消费者订阅的 Topic
func (c *Client) QueryTopicsByConsumerConcurrent(ctx context.Context, consumerGroup string) (*TopicList, error) {
	return c.QueryTopicsByConsumer(ctx, consumerGroup)
}

// GetUserSubscriptionGroup 获取用户订阅组
func (c *Client) GetUserSubscriptionGroup(ctx context.Context, brokerAddr string) (map[string]*SubscriptionGroupConfig, error) {
	allGroups, err := c.GetAllSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		return nil, err
	}

	// 过滤系统订阅组
	userGroups := make(map[string]*SubscriptionGroupConfig)
	for name, config := range allGroups {
		if !isSystemGroup(name) {
			userGroups[name] = config
		}
	}

	return userGroups, nil
}

// isSystemGroup 判断是否为系统消费组
func isSystemGroup(groupName string) bool {
	systemGroups := []string{
		"CID_ONSAPI_OWNER",
		"CID_ONSAPI_PULL",
		"CID_ONSAPI_PERMISSION",
		"SELF_TEST_C_GROUP",
		"CID_ONS-HTTP-PROXY",
		"CID_ONSAPI_SCHEDULE",
		"DEFAULT_CONSUMER",
		"TOOLS_CONSUMER",
		"FILTERSRV_CONSUMER",
	}
	for _, g := range systemGroups {
		if groupName == g {
			return true
		}
	}
	return false
}

// CloneGroupOffset 克隆消费组偏移
// Java: CLONE_GROUP_OFFSET = 314，发送到 broker 由 broker 端执行克隆
func (c *Client) CloneGroupOffset(ctx context.Context, srcGroup, destGroup, topic string, isOffline bool) error {
	// 获取 Topic 路由找到所有 Broker
	routeData, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return fmt.Errorf("获取 Topic 路由信息失败: %w", err)
	}

	for _, brokerData := range routeData.BrokerDatas {
		brokerAddr := brokerData.BrokerAddrs["0"] // Master
		if brokerAddr == "" {
			continue
		}

		extFields := map[string]string{
			"srcGroup":  srcGroup,
			"destGroup": destGroup,
			"topic":     topic,
			"offline":   fmt.Sprintf("%t", isOffline),
		}
		cmd := remoting.NewRequest(remoting.CloneGroupOffset, extFields)

		resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
		if err != nil {
			return fmt.Errorf("克隆偏移到 %s 失败: %w", brokerAddr, err)
		}
		if resp.Code != remoting.Success {
			return NewAdminError(resp.Code, resp.Remark)
		}
	}

	return nil
}

// UpdateAndGetGroupReadForbidden 更新并获取组读取禁止状态
// Java: UPDATE_AND_GET_GROUP_FORBIDDEN = 353
func (c *Client) UpdateAndGetGroupReadForbidden(ctx context.Context, brokerAddr, groupName, topic string, readable bool) (bool, error) {
	extFields := map[string]string{
		"group":    groupName,
		"topic":    topic,
		"readable": fmt.Sprintf("%t", readable),
	}
	cmd := remoting.NewRequest(remoting.UpdateAndGetGroupForbidden, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return false, err
	}

	if resp.Code != remoting.Success {
		return false, NewAdminError(resp.Code, resp.Remark)
	}

	// 响应中返回实际的 readable 状态
	if v, ok := resp.ExtFields["readable"]; ok {
		return v == "true", nil
	}

	return readable, nil
}

// =============================================================================
// 冷数据流控
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
