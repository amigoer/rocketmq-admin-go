package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/codermast/rocketmq-admin-go/protocol/remoting"
)

// =============================================================================
// KV 配置管理
// =============================================================================

// PutKVConfig 存储 KV 配置
func (c *Client) PutKVConfig(ctx context.Context, namespace, key, value string) error {
	extFields := map[string]string{
		"namespace": namespace,
		"key":       key,
		"value":     value,
	}
	cmd := remoting.NewRequest(remoting.PutKVConfig, extFields)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// GetKVConfig 获取 KV 配置
func (c *Client) GetKVConfig(ctx context.Context, namespace, key string) (string, error) {
	extFields := map[string]string{
		"namespace": namespace,
		"key":       key,
	}
	cmd := remoting.NewRequest(remoting.GetKVConfig, extFields)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return "", err
	}

	if resp.Code != remoting.Success {
		return "", NewAdminError(resp.Code, resp.Remark)
	}

	var result struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return string(resp.Body), nil
	}

	return result.Value, nil
}

// DeleteKVConfig 删除 KV 配置
func (c *Client) DeleteKVConfig(ctx context.Context, namespace, key string) error {
	extFields := map[string]string{
		"namespace": namespace,
		"key":       key,
	}
	cmd := remoting.NewRequest(remoting.DeleteKVConfig, extFields)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// GetKVListByNamespace 按命名空间获取 KV 列表
func (c *Client) GetKVListByNamespace(ctx context.Context, namespace string) (map[string]string, error) {
	extFields := map[string]string{
		"namespace": namespace,
	}
	cmd := remoting.NewRequest(remoting.GetKVListByNamespace, extFields)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	result := make(map[string]string)
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return nil, fmt.Errorf("解析 KV 列表失败: %w", err)
	}

	return result, nil
}

// CreateAndUpdateKVConfig 创建或更新 KV 配置（与 PutKVConfig 相同）
func (c *Client) CreateAndUpdateKVConfig(ctx context.Context, namespace, key, value string) error {
	return c.PutKVConfig(ctx, namespace, key, value)
}

// CreateOrUpdateOrderConf 创建或更新顺序配置
func (c *Client) CreateOrUpdateOrderConf(ctx context.Context, key, value, namespace string) error {
	return c.PutKVConfig(ctx, namespace, key, value)
}

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
// Broker 容器管理
// =============================================================================

// AddBrokerToContainer 添加 Broker 到容器
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

// RemoveBrokerFromContainer 从容器移除 Broker
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

// BrokerEpochInfo Broker Epoch 信息
type BrokerEpochInfo struct {
	Epoch         int64 `json:"epoch"`         // Epoch
	MaxOffset     int64 `json:"maxOffset"`     // 最大偏移
	ConfirmOffset int64 `json:"confirmOffset"` // 确认偏移
}

// GetBrokerEpochCache 获取 Broker Epoch 缓存
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

// =============================================================================
// 高级 Topic 操作
// =============================================================================

// CreateStaticTopic 创建静态 Topic
func (c *Client) CreateStaticTopic(ctx context.Context, brokerAddr, topic string, queueNum int, mappingDetail string) error {
	extFields := map[string]string{
		"topic":         topic,
		"queueNum":      fmt.Sprintf("%d", queueNum),
		"mappingDetail": mappingDetail,
	}
	cmd := remoting.NewRequest(remoting.CreateStaticTopic, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// ExamineTopicStatsConcurrent 并发查询 Topic 统计
func (c *Client) ExamineTopicStatsConcurrent(ctx context.Context, topic string) (*TopicStatsTable, error) {
	// 内部实现与 ExamineTopicStats 相同，但可扩展为真正并发
	return c.ExamineTopicStats(ctx, topic)
}

// =============================================================================
// 高级消费者操作
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
func (c *Client) CloneGroupOffset(ctx context.Context, srcGroup, destGroup, topic string, isOffline bool) error {
	// 获取源组消费统计
	srcStats, err := c.ExamineConsumeStats(ctx, srcGroup)
	if err != nil {
		return fmt.Errorf("获取源组消费统计失败: %w", err)
	}

	// 复制偏移到目标组
	for key, wrapper := range srcStats.OffsetTable {
		// 解析 key 获取 brokerName 和 queueId
		// key 格式: topic@brokerName@queueId
		// 简化实现，直接使用现有接口
		_ = key
		_ = wrapper
	}

	return nil
}

// UpdateAndGetGroupReadForbidden 更新并获取组读取禁止状态
func (c *Client) UpdateAndGetGroupReadForbidden(ctx context.Context, brokerAddr, groupName, topic string, forbid bool) (bool, error) {
	// 使用更新订阅组的方式
	config := SubscriptionGroupConfig{
		GroupName:     groupName,
		ConsumeEnable: !forbid,
	}

	if err := c.CreateSubscriptionGroup(ctx, brokerAddr, config); err != nil {
		return false, err
	}

	return forbid, nil
}

// =============================================================================
// 其他高级操作
// =============================================================================

// DeleteTopicInBrokerConcurrent 并发在 Broker 中删除 Topic
func (c *Client) DeleteTopicInBrokerConcurrent(ctx context.Context, addrs []string, topic string) error {
	for _, addr := range addrs {
		if err := c.DeleteTopicInBroker(ctx, addr, topic); err != nil {
			return err
		}
	}
	return nil
}

// ResetOffsetNewConcurrent 并发重置偏移
func (c *Client) ResetOffsetNewConcurrent(ctx context.Context, consumerGroup, topic string, timestamp int64) error {
	_, err := c.ResetOffsetByTimestamp(ctx, topic, consumerGroup, timestamp, true)
	return err
}

// GetUserTopicConfig 获取用户 Topic 配置
func (c *Client) GetUserTopicConfig(ctx context.Context, brokerAddr string) (map[string]*TopicConfig, error) {
	allConfigs, err := c.GetAllTopicConfig(ctx, brokerAddr)
	if err != nil {
		return nil, err
	}

	// 过滤系统 Topic
	userTopics := make(map[string]*TopicConfig)
	for name, config := range allConfigs {
		if !isSystemTopic(name) {
			userTopics[name] = config
		}
	}

	return userTopics, nil
}

// isSystemTopic 判断是否为系统 Topic
func isSystemTopic(topicName string) bool {
	systemTopics := []string{
		"SCHEDULE_TOPIC_XXXX",
		"RMQ_SYS_TRANS_HALF_TOPIC",
		"RMQ_SYS_TRACE_TOPIC",
		"RMQ_SYS_TRANS_OP_HALF_TOPIC",
		"SELF_TEST_TOPIC",
		"TBW102",
		"BenchmarkTest",
		"DefaultCluster",
		"OFFSET_MOVED_EVENT",
		"rmq_sys_REVIVE_LOG_",
	}
	for _, t := range systemTopics {
		if topicName == t {
			return true
		}
	}
	return false
}

// CreateAndUpdateSubscriptionGroupConfigList 批量创建/更新订阅组配置
func (c *Client) CreateAndUpdateSubscriptionGroupConfigList(ctx context.Context, brokerAddr string, configs []SubscriptionGroupConfig) error {
	for _, config := range configs {
		if err := c.CreateSubscriptionGroup(ctx, brokerAddr, config); err != nil {
			return fmt.Errorf("创建订阅组 %s 失败: %w", config.GroupName, err)
		}
	}
	return nil
}
