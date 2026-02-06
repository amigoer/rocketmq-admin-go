package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/codermast/rocketmq-admin-go/protocol/remoting"
)

// =============================================================================
// ACL 用户管理接口
// =============================================================================

// CreateUser 创建用户
func (c *Client) CreateUser(ctx context.Context, brokerAddr string, user UserInfo) error {
	body, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("序列化用户信息失败: %w", err)
	}

	cmd := remoting.NewRequest(remoting.CreateUser, nil)
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

// UpdateUser 更新用户
func (c *Client) UpdateUser(ctx context.Context, brokerAddr string, user UserInfo) error {
	body, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("序列化用户信息失败: %w", err)
	}

	cmd := remoting.NewRequest(remoting.UpdateUser, nil)
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

// DeleteUser 删除用户
func (c *Client) DeleteUser(ctx context.Context, brokerAddr, username string) error {
	extFields := map[string]string{
		"username": username,
	}
	cmd := remoting.NewRequest(remoting.DeleteUser, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// GetUser 获取用户信息
func (c *Client) GetUser(ctx context.Context, brokerAddr, username string) (*UserInfo, error) {
	extFields := map[string]string{
		"username": username,
	}
	cmd := remoting.NewRequest(remoting.GetUser, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var user UserInfo
	if err := json.Unmarshal(resp.Body, &user); err != nil {
		return nil, fmt.Errorf("解析用户信息失败: %w", err)
	}

	return &user, nil
}

// ListUser 列出所有用户
func (c *Client) ListUser(ctx context.Context, brokerAddr string) (*UserList, error) {
	cmd := remoting.NewRequest(remoting.ListUser, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var users UserList
	if err := json.Unmarshal(resp.Body, &users); err != nil {
		return nil, fmt.Errorf("解析用户列表失败: %w", err)
	}

	return &users, nil
}

// =============================================================================
// ACL 规则管理接口
// =============================================================================

// CreateAcl 创建 ACL 规则
func (c *Client) CreateAcl(ctx context.Context, brokerAddr string, acl AclInfo) error {
	body, err := json.Marshal(acl)
	if err != nil {
		return fmt.Errorf("序列化 ACL 信息失败: %w", err)
	}

	cmd := remoting.NewRequest(remoting.CreateAcl, nil)
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

// UpdateAcl 更新 ACL 规则
func (c *Client) UpdateAcl(ctx context.Context, brokerAddr string, acl AclInfo) error {
	body, err := json.Marshal(acl)
	if err != nil {
		return fmt.Errorf("序列化 ACL 信息失败: %w", err)
	}

	cmd := remoting.NewRequest(remoting.UpdateAcl, nil)
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

// DeleteAcl 删除 ACL 规则
func (c *Client) DeleteAcl(ctx context.Context, brokerAddr, subject string) error {
	extFields := map[string]string{
		"subject": subject,
	}
	cmd := remoting.NewRequest(remoting.DeleteAcl, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// GetAcl 获取 ACL 规则
func (c *Client) GetAcl(ctx context.Context, brokerAddr, subject string) (*AclInfo, error) {
	extFields := map[string]string{
		"subject": subject,
	}
	cmd := remoting.NewRequest(remoting.GetAcl, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var acl AclInfo
	if err := json.Unmarshal(resp.Body, &acl); err != nil {
		return nil, fmt.Errorf("解析 ACL 信息失败: %w", err)
	}

	return &acl, nil
}

// ListAcl 列出所有 ACL 规则
func (c *Client) ListAcl(ctx context.Context, brokerAddr string) (*AclList, error) {
	cmd := remoting.NewRequest(remoting.ListAcl, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var acls AclList
	if err := json.Unmarshal(resp.Body, &acls); err != nil {
		return nil, fmt.Errorf("解析 ACL 列表失败: %w", err)
	}

	return &acls, nil
}

// =============================================================================
// Broker 管理扩展
// =============================================================================

// WipeWritePermOfBroker 清除 Broker 写权限
func (c *Client) WipeWritePermOfBroker(ctx context.Context, brokerName string) (int, error) {
	extFields := map[string]string{
		"brokerName": brokerName,
	}
	cmd := remoting.NewRequest(remoting.WipeWritePermOfBroker, extFields)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return 0, err
	}

	if resp.Code != remoting.Success {
		return 0, NewAdminError(resp.Code, resp.Remark)
	}

	// 返回修改的队列数
	var result struct {
		WipeTopicCount int `json:"wipeTopicCount"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return 0, nil // 忽略解析错误
	}

	return result.WipeTopicCount, nil
}

// AddWritePermOfBroker 添加 Broker 写权限
func (c *Client) AddWritePermOfBroker(ctx context.Context, brokerName string) (int, error) {
	extFields := map[string]string{
		"brokerName": brokerName,
	}
	cmd := remoting.NewRequest(remoting.AddWritePermOfBroker, extFields)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return 0, err
	}

	if resp.Code != remoting.Success {
		return 0, NewAdminError(resp.Code, resp.Remark)
	}

	var result struct {
		AddTopicCount int `json:"addTopicCount"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return 0, nil
	}

	return result.AddTopicCount, nil
}

// ViewBrokerStatsData 查看 Broker 统计数据
func (c *Client) ViewBrokerStatsData(ctx context.Context, brokerAddr, statsName, statsKey string) (*BrokerStatsData, error) {
	extFields := map[string]string{
		"statsName": statsName,
		"statsKey":  statsKey,
	}
	cmd := remoting.NewRequest(remoting.ViewBrokerStatsData, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var stats BrokerStatsData
	if err := json.Unmarshal(resp.Body, &stats); err != nil {
		return nil, fmt.Errorf("解析统计数据失败: %w", err)
	}

	return &stats, nil
}

// GetBrokerHAStatus 获取 Broker HA 状态
func (c *Client) GetBrokerHAStatus(ctx context.Context, brokerAddr string) (*BrokerHAStatus, error) {
	cmd := remoting.NewRequest(remoting.GetBrokerHAStatus, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var status BrokerHAStatus
	if err := json.Unmarshal(resp.Body, &status); err != nil {
		return nil, fmt.Errorf("解析 HA 状态失败: %w", err)
	}

	return &status, nil
}

// =============================================================================
// NameServer 配置管理
// =============================================================================

// UpdateNameServerConfig 更新 NameServer 配置
func (c *Client) UpdateNameServerConfig(ctx context.Context, properties map[string]string) error {
	cmd := remoting.NewRequest(remoting.UpdateNamesrvConfig, properties)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// GetNameServerConfig 获取 NameServer 配置
func (c *Client) GetNameServerConfig(ctx context.Context) (map[string]string, error) {
	cmd := remoting.NewRequest(remoting.GetNamesrvConfig, nil)

	resp, err := c.invokeNameServer(ctx, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	config := make(map[string]string)
	if err := json.Unmarshal(resp.Body, &config); err != nil {
		// 尝试作为字符串处理
		if len(resp.Body) > 0 {
			config["raw"] = string(resp.Body)
		}
	}

	return config, nil
}

// =============================================================================
// 生产者管理
// =============================================================================

// ExamineProducerConnectionInfo 查询生产者连接信息
func (c *Client) ExamineProducerConnectionInfo(ctx context.Context, producerGroup, topic string) (*ProducerConnection, error) {
	// 先获取集群信息
	clusterInfo, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return nil, err
	}

	// 尝试从任意 Broker 获取生产者连接信息
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
			"producerGroup": producerGroup,
			"topic":         topic,
		}
		cmd := remoting.NewRequest(remoting.GetProducerConnectionList, extFields)

		resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
		if err != nil {
			continue
		}

		if resp.Code != remoting.Success {
			continue
		}

		var connInfo ProducerConnection
		if err := json.Unmarshal(resp.Body, &connInfo); err != nil {
			continue
		}

		return &connInfo, nil
	}

	return nil, fmt.Errorf("未找到生产者组 %s 的连接信息", producerGroup)
}

// GetAllProducerInfo 获取所有生产者信息
func (c *Client) GetAllProducerInfo(ctx context.Context, brokerAddr string) (map[string][]Connection, error) {
	cmd := remoting.NewRequest(remoting.GetProducerInfo, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	result := make(map[string][]Connection)
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return nil, fmt.Errorf("解析生产者信息失败: %w", err)
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
	cmd := remoting.NewRequest(remoting.GetAllSubscriptionGroup, nil)

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
// Offset 管理扩展
// =============================================================================

// UpdateConsumeOffset 更新消费 Offset
func (c *Client) UpdateConsumeOffset(ctx context.Context, brokerAddr string, consumerGroup, topic string, queueId int, offset int64) error {
	extFields := map[string]string{
		"consumerGroup": consumerGroup,
		"topic":         topic,
		"queueId":       fmt.Sprintf("%d", queueId),
		"commitOffset":  fmt.Sprintf("%d", offset),
	}
	cmd := remoting.NewRequest(remoting.UpdateConsumeOffset, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// ResetOffsetByQueueId 按队列 ID 重置 Offset
func (c *Client) ResetOffsetByQueueId(ctx context.Context, brokerAddr, topic, group string, queueId int, offset int64) error {
	extFields := map[string]string{
		"topic":   topic,
		"group":   group,
		"queueId": fmt.Sprintf("%d", queueId),
		"offset":  fmt.Sprintf("%d", offset),
	}
	cmd := remoting.NewRequest(remoting.ResetOffsetByQueueId, extFields)

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
// 消息操作
// =============================================================================

// MessageTrackDetail 消息轨迹详情
// 注意：此方法需要 MessageExt 类型，将在消息查询功能完善后实现
// func (c *Client) MessageTrackDetail(ctx context.Context, msg MessageExt) ([]MessageTrack, error)
