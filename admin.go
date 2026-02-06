// Package admin 提供 RocketMQ 运维管理客户端
//
// 本库专注于 RocketMQ 运维管理接口，不包含消息生产/消费功能。
// 如需消息功能，请使用官方 rocketmq-client-go。
//
// 快速开始:
//
//	client, err := admin.NewClient(
//	    admin.WithNameServers([]string{"127.0.0.1:9876"}),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	// 查询集群信息
//	clusterInfo, err := client.ExamineBrokerClusterInfo(context.Background())
package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/codermast/rocketmq-admin-go/protocol/remoting"
)

// Client 是 RocketMQ 运维管理客户端
type Client struct {
	opts    *Options                 // 客户端配置
	pool    *remoting.ConnectionPool // 连接池
	mu      sync.RWMutex             // 保护内部状态
	started bool                     // 是否已启动
	closed  bool                     // 是否已关闭
}

// NewClient 创建新的运维管理客户端
func NewClient(opts ...Option) (*Client, error) {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	if err := options.validate(); err != nil {
		return nil, err
	}

	client := &Client{
		opts: options,
		pool: remoting.NewConnectionPool(options.Timeout),
	}

	return client, nil
}

// Start 启动客户端
func (c *Client) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return ErrAlreadyStarted
	}
	if c.closed {
		return ErrClientClosed
	}

	c.started = true
	return nil
}

// Close 关闭客户端，释放资源
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	// 关闭连接池
	if c.pool != nil {
		c.pool.Close()
	}

	c.closed = true
	return nil
}

// IsStarted 返回客户端是否已启动
func (c *Client) IsStarted() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.started
}

// IsClosed 返回客户端是否已关闭
func (c *Client) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

// =============================================================================
// 内部辅助方法
// =============================================================================

// invokeNameServer 向 NameServer 发送请求
func (c *Client) invokeNameServer(ctx context.Context, cmd *remoting.RemotingCommand) (*remoting.RemotingCommand, error) {
	var lastErr error
	for _, addr := range c.opts.NameServers {
		conn, err := c.pool.GetOrCreate(addr)
		if err != nil {
			lastErr = err
			continue
		}

		resp, err := conn.InvokeSync(ctx, cmd)
		if err != nil {
			lastErr = err
			c.pool.Remove(addr)
			continue
		}

		return resp, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("所有 NameServer 请求失败: %w", lastErr)
	}
	return nil, ErrConnectionFailed
}

// invokeBroker 向 Broker 发送请求
func (c *Client) invokeBroker(ctx context.Context, brokerAddr string, cmd *remoting.RemotingCommand) (*remoting.RemotingCommand, error) {
	conn, err := c.pool.GetOrCreate(brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("连接 Broker 失败: %w", err)
	}

	resp, err := conn.InvokeSync(ctx, cmd)
	if err != nil {
		c.pool.Remove(brokerAddr)
		return nil, fmt.Errorf("请求 Broker 失败: %w", err)
	}

	return resp, nil
}

// =============================================================================
// 集群管理接口
// =============================================================================

// ExamineBrokerClusterInfo 查询集群信息
func (c *Client) ExamineBrokerClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	cmd := remoting.NewRequest(remoting.GetBrokerClusterInfo, nil)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var clusterInfo ClusterInfo
	if err := json.Unmarshal(resp.Body, &clusterInfo); err != nil {
		return nil, fmt.Errorf("解析集群信息失败: %w", err)
	}

	return &clusterInfo, nil
}

// GetNameServerAddressList 获取 NameServer 地址列表
func (c *Client) GetNameServerAddressList() []string {
	return c.opts.NameServers
}

// =============================================================================
// Broker 管理接口
// =============================================================================

// FetchBrokerRuntimeStats 获取 Broker 运行时统计信息
func (c *Client) FetchBrokerRuntimeStats(ctx context.Context, brokerAddr string) (*KVTable, error) {
	cmd := remoting.NewRequest(remoting.GetBrokerRuntimeInfo, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var kvTable KVTable
	if err := json.Unmarshal(resp.Body, &kvTable); err != nil {
		return nil, fmt.Errorf("解析 Broker 运行信息失败: %w", err)
	}

	return &kvTable, nil
}

// GetBrokerConfig 获取 Broker 配置
func (c *Client) GetBrokerConfig(ctx context.Context, brokerAddr string) (map[string]string, error) {
	cmd := remoting.NewRequest(remoting.GetBrokerConfig, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	// Broker 配置以 Properties 格式返回
	config := make(map[string]string)
	if err := json.Unmarshal(resp.Body, &config); err != nil {
		// 如果 JSON 解析失败，尝试解析为字符串
		configStr := string(resp.Body)
		if configStr != "" {
			config["raw"] = configStr
		}
	}

	return config, nil
}

// UpdateBrokerConfig 更新 Broker 配置
func (c *Client) UpdateBrokerConfig(ctx context.Context, brokerAddr string, properties map[string]string) error {
	extFields := make(map[string]string)
	for k, v := range properties {
		extFields[k] = v
	}

	cmd := remoting.NewRequest(remoting.UpdateBrokerConfig, extFields)

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
// Topic 管理接口
// =============================================================================

// CreateTopic 创建 Topic
func (c *Client) CreateTopic(ctx context.Context, addr string, config TopicConfig) error {
	extFields := map[string]string{
		"topic":           config.TopicName,
		"readQueueNums":   fmt.Sprintf("%d", config.ReadQueueNums),
		"writeQueueNums":  fmt.Sprintf("%d", config.WriteQueueNums),
		"perm":            fmt.Sprintf("%d", config.Perm),
		"topicFilterType": config.TopicFilterType,
		"topicSysFlag":    fmt.Sprintf("%d", config.TopicSysFlag),
		"order":           fmt.Sprintf("%t", config.Order),
	}

	cmd := remoting.NewRequest(remoting.UpdateAndCreateTopic, extFields)

	resp, err := c.invokeBroker(ctx, addr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// DeleteTopic 删除 Topic
func (c *Client) DeleteTopic(ctx context.Context, topicName, clusterName string) error {
	// 1. 先获取集群信息，找到所有 Broker
	clusterInfo, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return fmt.Errorf("获取集群信息失败: %w", err)
	}

	// 2. 在所有 Broker 上删除 Topic
	brokerNames, ok := clusterInfo.ClusterAddrTable[clusterName]
	if !ok {
		return fmt.Errorf("集群 %s 不存在", clusterName)
	}

	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}

		// 向 Master Broker 发送删除请求
		if masterAddr, ok := brokerData.BrokerAddrs[0]; ok {
			extFields := map[string]string{
				"topic": topicName,
			}
			cmd := remoting.NewRequest(remoting.DeleteTopicInBroker, extFields)

			if _, err := c.invokeBroker(ctx, masterAddr, cmd); err != nil {
				return fmt.Errorf("在 Broker %s 删除 Topic 失败: %w", brokerName, err)
			}
		}
	}

	// 3. 在 NameServer 删除 Topic
	extFields := map[string]string{
		"topic": topicName,
	}
	cmd := remoting.NewRequest(remoting.DeleteTopicInNamesrv, extFields)

	if _, err := c.invokeNameServer(ctx, cmd); err != nil {
		return fmt.Errorf("在 NameServer 删除 Topic 失败: %w", err)
	}

	return nil
}

// FetchAllTopicList 获取所有 Topic 列表
func (c *Client) FetchAllTopicList(ctx context.Context) (*TopicList, error) {
	cmd := remoting.NewRequest(remoting.GetAllTopicListFromNamesrv, nil)

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

// FetchTopicsByCluster 按集群获取 Topic 列表
func (c *Client) FetchTopicsByCluster(ctx context.Context, clusterName string) (*TopicList, error) {
	extFields := map[string]string{
		"clusterName": clusterName,
	}
	cmd := remoting.NewRequest(remoting.GetTopicsByCluster, extFields)

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

// ExamineTopicRouteInfo 查询 Topic 路由信息
func (c *Client) ExamineTopicRouteInfo(ctx context.Context, topic string) (*TopicRouteData, error) {
	extFields := map[string]string{
		"topic": topic,
	}
	cmd := remoting.NewRequest(remoting.GetRouteInfoByTopic, extFields)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code == remoting.TopicNotExist {
		return nil, ErrTopicNotFound
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var routeData TopicRouteData
	if err := json.Unmarshal(resp.Body, &routeData); err != nil {
		return nil, fmt.Errorf("解析 Topic 路由失败: %w", err)
	}

	return &routeData, nil
}

// ExamineTopicStats 查询 Topic 统计信息
func (c *Client) ExamineTopicStats(ctx context.Context, topic string) (*TopicStatsTable, error) {
	// 先获取路由信息
	routeData, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, err
	}

	if len(routeData.BrokerDatas) == 0 {
		return nil, ErrBrokerNotFound
	}

	// 向第一个 Broker 查询统计信息
	brokerData := routeData.BrokerDatas[0]
	var brokerAddr string
	for _, addr := range brokerData.BrokerAddrs {
		brokerAddr = addr
		break
	}

	extFields := map[string]string{
		"topic": topic,
	}
	cmd := remoting.NewRequest(remoting.GetTopicStatsInfo, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var statsTable TopicStatsTable
	if err := json.Unmarshal(resp.Body, &statsTable); err != nil {
		return nil, fmt.Errorf("解析 Topic 统计失败: %w", err)
	}

	return &statsTable, nil
}

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
		for queueKey, offset := range offsetTable {
			mq := MessageQueue{
				Topic:      topic,
				BrokerName: brokerData.BrokerName,
			}
			result[mq] = offset
			_ = queueKey // 忽略具体队列解析
		}
	}

	return result, nil
}
