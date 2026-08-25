package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

// CreateTopic creates or updates a topic on one Broker.
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

// DeleteTopic removes a topic from every Broker in a cluster, then drops its
// route from the NameServer.
func (c *Client) DeleteTopic(ctx context.Context, topicName, clusterName string) error {
	clusterInfo, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return fmt.Errorf("获取集群信息失败: %w", err)
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

		// Broker id "0" is the master.
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

	extFields := map[string]string{
		"topic": topicName,
	}
	cmd := remoting.NewRequest(remoting.DeleteTopicInNamesrv, extFields)

	if _, err := c.invokeNameServer(ctx, cmd); err != nil {
		return fmt.Errorf("在 NameServer 删除 Topic 失败: %w", err)
	}

	return nil
}

// FetchAllTopicList returns every topic known to the NameServer.
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

// FetchTopicsByCluster returns the topics belonging to one cluster.
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

// ExamineTopicRouteInfo returns a topic's route from the NameServer.
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

	// RocketMQ emits numeric map keys without quotes, which is not valid JSON.
	fixedBody := fixJSONBody(resp.Body)

	var routeData TopicRouteData
	if err := json.Unmarshal(fixedBody, &routeData); err != nil {
		return nil, fmt.Errorf("解析 Topic 路由失败: %w", err)
	}

	// Record name-to-address pairs so later requests to these Brokers carry bname.
	c.rememberRouteBrokerNames(&routeData)

	return &routeData, nil
}

// ExamineTopicStats returns per-queue offsets for a topic.
func (c *Client) ExamineTopicStats(ctx context.Context, topic string) (*TopicStatsTable, error) {
	routeData, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, err
	}

	if len(routeData.BrokerDatas) == 0 {
		return nil, ErrBrokerNotFound
	}

	// Stats come from the first Broker in the route only.
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

	// RocketMQ emits numeric map keys without quotes, which is not valid JSON.
	fixedBody := fixJSONBody(resp.Body)

	var statsTable TopicStatsTable
	if err := json.Unmarshal(fixedBody, &statsTable); err != nil {
		return nil, fmt.Errorf("解析 Topic 统计失败: %w", err)
	}

	return &statsTable, nil
}

// DeleteTopicInBroker removes a topic from one Broker, leaving its route intact.
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

// DeleteTopicInNameServer removes a topic's route, leaving Broker data intact.
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

// ExamineTopicConfig returns one topic's configuration from one Broker.
// Java: GET_TOPIC_CONFIG = 351
func (c *Client) ExamineTopicConfig(ctx context.Context, brokerAddr, topic string) (*TopicConfig, error) {
	extFields := map[string]string{
		"topic": topic,
	}
	cmd := remoting.NewRequest(remoting.GetTopicConfig, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var config TopicConfig
	if err := json.Unmarshal(resp.Body, &config); err != nil {
		return nil, fmt.Errorf("解析 Topic 配置失败: %w", err)
	}

	return &config, nil
}

// QueryTopicConsumeByWho returns the consumer groups subscribed to a topic.
func (c *Client) QueryTopicConsumeByWho(ctx context.Context, topic string) ([]string, error) {
	extFields := map[string]string{
		"topic": topic,
	}
	cmd := remoting.NewRequest(remoting.QueryTopicConsumeByWho, extFields)

	routeData, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, err
	}

	if len(routeData.BrokerDatas) == 0 {
		return nil, ErrBrokerNotFound
	}

	// Any Broker in the route can answer; use the first.
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

// GetAllTopicConfig returns every topic configuration held by one Broker.
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

// CreateAndUpdateTopicConfigList creates or updates several topics on one
// Broker, stopping at the first failure.
func (c *Client) CreateAndUpdateTopicConfigList(ctx context.Context, brokerAddr string, configs []TopicConfig) error {
	for _, config := range configs {
		if err := c.CreateTopic(ctx, brokerAddr, config); err != nil {
			return fmt.Errorf("创建 Topic %s 失败: %w", config.TopicName, err)
		}
	}
	return nil
}

// GetTopicClusterList returns the clusters a topic is routed to.
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

// CreateStaticTopic creates a static (logical queue) topic on one Broker.
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

// ExamineTopicStatsConcurrent is an alias for ExamineTopicStats. Despite the
// name it is not concurrent yet.
func (c *Client) ExamineTopicStatsConcurrent(ctx context.Context, topic string) (*TopicStatsTable, error) {
	return c.ExamineTopicStats(ctx, topic)
}
