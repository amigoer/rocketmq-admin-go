package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/codermast/rocketmq-admin-go/protocol/remoting"
)

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
		if masterAddr, ok := brokerData.BrokerAddrs["0"]; ok {
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

	// 修复 RocketMQ 返回的非标准 JSON（数字 key 没有引号）
	fixedBody := fixJSONBody(resp.Body)

	var routeData TopicRouteData
	if err := json.Unmarshal(fixedBody, &routeData); err != nil {
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

	// 修复 RocketMQ 返回的非标准 JSON（数字 key 没有引号）
	fixedBody := fixJSONBody(resp.Body)

	var statsTable TopicStatsTable
	if err := json.Unmarshal(fixedBody, &statsTable); err != nil {
		return nil, fmt.Errorf("解析 Topic 统计失败: %w", err)
	}

	return &statsTable, nil
}

// =============================================================================
// Topic 管理扩展
// =============================================================================

// DeleteTopicInBroker 在 Broker 中删除 Topic
func (c *Client) DeleteTopicInBroker(ctx context.Context, brokerAddr, topic string) error {
	extFields := map[string]string{
		"topic": topic,
	}
	cmd := remoting.NewRequest(remoting.DeleteTopicInBroker, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// DeleteTopicInNameServer 在 NameServer 中删除 Topic
func (c *Client) DeleteTopicInNameServer(ctx context.Context, topic string) error {
	extFields := map[string]string{
		"topic": topic,
	}
	cmd := remoting.NewRequest(remoting.DeleteTopicInNamesrv, extFields)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// ExamineTopicConfig 查询 Topic 配置
func (c *Client) ExamineTopicConfig(ctx context.Context, brokerAddr, topic string) (*TopicConfig, error) {
	extFields := map[string]string{
		"topic": topic,
	}
	cmd := remoting.NewRequest(remoting.GetAllTopicConfig, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	// 解析所有配置，然后找到目标 Topic
	var wrapper struct {
		TopicConfigTable map[string]*TopicConfig `json:"topicConfigTable"`
	}
	if err := json.Unmarshal(resp.Body, &wrapper); err != nil {
		return nil, fmt.Errorf("解析 Topic 配置失败: %w", err)
	}

	if config, ok := wrapper.TopicConfigTable[topic]; ok {
		return config, nil
	}

	return nil, ErrTopicNotFound
}

// QueryTopicConsumeByWho 查询 Topic 被哪些消费者消费
func (c *Client) QueryTopicConsumeByWho(ctx context.Context, topic string) ([]string, error) {
	extFields := map[string]string{
		"topic": topic,
	}
	cmd := remoting.NewRequest(remoting.QueryTopicConsumeByWho, extFields)

	// 先获取路由信息
	routeData, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, err
	}

	if len(routeData.BrokerDatas) == 0 {
		return nil, ErrBrokerNotFound
	}

	// 向第一个 Broker 查询
	brokerData := routeData.BrokerDatas[0]
	var brokerAddr string
	for _, addr := range brokerData.BrokerAddrs {
		brokerAddr = addr
		break
	}

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var groups struct {
		GroupList []string `json:"groupList"`
	}
	if err := json.Unmarshal(resp.Body, &groups); err != nil {
		return nil, fmt.Errorf("解析消费组列表失败: %w", err)
	}

	return groups.GroupList, nil
}

// GetAllTopicConfig 获取所有 Topic 配置
func (c *Client) GetAllTopicConfig(ctx context.Context, brokerAddr string) (map[string]*TopicConfig, error) {
	cmd := remoting.NewRequest(remoting.GetAllTopicConfig, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var wrapper struct {
		TopicConfigTable map[string]*TopicConfig `json:"topicConfigTable"`
	}
	if err := json.Unmarshal(resp.Body, &wrapper); err != nil {
		return nil, fmt.Errorf("解析 Topic 配置失败: %w", err)
	}

	return wrapper.TopicConfigTable, nil
}

// CreateAndUpdateTopicConfigList 批量创建/更新 Topic 配置
func (c *Client) CreateAndUpdateTopicConfigList(ctx context.Context, brokerAddr string, configs []TopicConfig) error {
	for _, config := range configs {
		if err := c.CreateTopic(ctx, brokerAddr, config); err != nil {
			return fmt.Errorf("创建 Topic %s 失败: %w", config.TopicName, err)
		}
	}
	return nil
}

// GetTopicClusterList 获取 Topic 所属集群列表
func (c *Client) GetTopicClusterList(ctx context.Context, topic string) ([]string, error) {
	routeData, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, err
	}

	clusterSet := make(map[string]bool)
	for _, brokerData := range routeData.BrokerDatas {
		if brokerData.Cluster != "" {
			clusterSet[brokerData.Cluster] = true
		}
	}

	clusters := make([]string, 0, len(clusterSet))
	for cluster := range clusterSet {
		clusters = append(clusters, cluster)
	}

	return clusters, nil
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
