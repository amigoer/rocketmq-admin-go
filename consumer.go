package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

// CreateSubscriptionGroup creates or updates a subscription group on one Broker.
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

// DeleteSubscriptionGroup removes a subscription group from one Broker.
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

// ExamineSubscriptionGroupConfig returns one subscription group's configuration.
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
		return nil, fmt.Errorf("failed to parse subscription group config: %w", err)
	}

	return &config, nil
}

// ExamineConsumeStats returns a consumer group's progress, merged across every
// Broker in the cluster.
func (c *Client) ExamineConsumeStats(ctx context.Context, consumerGroup string) (*ConsumeStats, error) {
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
		if err := json.Unmarshal(fixJSONBody(resp.Body), &stats); err != nil {
			continue
		}

		for k, v := range stats.OffsetTable {
			result.OffsetTable[k] = v
		}
		result.ConsumeTps += stats.ConsumeTps
	}

	return result, nil
}

// ExamineConsumerConnectionInfo returns the live connections of a consumer group.
func (c *Client) ExamineConsumerConnectionInfo(ctx context.Context, consumerGroup string) (*ConsumerConnection, error) {
	clusterInfo, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return nil, err
	}

	// Any Broker holding the group can answer; try them in turn.
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

// ExamineConsumeStatsByTopic returns a consumer group's progress on one topic only.
func (c *Client) ExamineConsumeStatsByTopic(ctx context.Context, consumerGroup, topic string) (*ConsumeStats, error) {
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
		if err := json.Unmarshal(fixJSONBody(resp.Body), &stats); err != nil {
			continue
		}

		for k, v := range stats.OffsetTable {
			result.OffsetTable[k] = v
		}
		result.ConsumeTps += stats.ConsumeTps
	}

	return result, nil
}

// FetchConsumeStatsInBroker returns every consumer group's stats on one Broker.
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
	if err := json.Unmarshal(fixJSONBody(resp.Body), &result); err != nil {
		return nil, fmt.Errorf("failed to parse Broker consume stats: %w", err)
	}

	return &result, nil
}

// QuerySubscription returns one group's subscription to one topic.
// Java: QUERY_SUBSCRIPTION_BY_CONSUMER = 345
func (c *Client) QuerySubscription(ctx context.Context, consumerGroup, topic string) (*SubscriptionData, error) {
	extFields := map[string]string{
		"group": consumerGroup,
		"topic": topic,
	}
	cmd := remoting.NewRequest(remoting.QuerySubscription, extFields)

	// Any Broker in the cluster can answer; try them in turn.
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

	return nil, fmt.Errorf("no subscription found for consumer group %s on topic %s", consumerGroup, topic)
}

// GetConsumeStatus returns each client's per-queue offsets for a consumer group.
// Java: INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223
// The Broker forwards the request to the live consumer clients.
func (c *Client) GetConsumeStatus(ctx context.Context, topic, consumerGroup, clientAddr string) (map[string]map[string]int64, error) {
	extFields := map[string]string{
		"topic": topic,
		"group": consumerGroup,
	}
	if clientAddr != "" {
		extFields["clientAddr"] = clientAddr
	}
	cmd := remoting.NewRequest(remoting.InvokeBrokerToGetConsumerStatus, extFields)

	// Any Broker in the topic's route can forward; try them in turn.
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

		// Response shape: clientId -> queueKey -> offset.
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

// ResetOffsetByTimestamp rewinds a consumer group's offsets to a timestamp.
func (c *Client) ResetOffsetByTimestamp(ctx context.Context, topic, group string, timestamp int64, force bool) (map[MessageQueue]int64, error) {
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

		var offsetTable map[string]int64
		if err := json.Unmarshal(resp.Body, &offsetTable); err != nil {
			continue
		}

		// queueKey is either a JSON MessageQueue or "brokerName-queueId".
		for queueKey, offset := range offsetTable {
			mq := MessageQueue{
				Topic:      topic,
				BrokerName: brokerData.BrokerName,
			}
			var mqParsed MessageQueue
			if err := json.Unmarshal([]byte(queueKey), &mqParsed); err == nil {
				mq = mqParsed
			} else {
				// Fall back to the trailing queueId of the simple form.
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

// GetConsumerRunningInfo asks one consumer client for its runtime snapshot.
func (c *Client) GetConsumerRunningInfo(ctx context.Context, consumerGroup, clientId string, jstack bool) (*ConsumerRunningInfo, error) {
	connInfo, err := c.ExamineConsumerConnectionInfo(ctx, consumerGroup)
	if err != nil {
		return nil, err
	}

	if len(connInfo.ConnectionSet) == 0 {
		return nil, ErrConsumerGroupNotFound
	}

	var targetConn *Connection
	for _, conn := range connInfo.ConnectionSet {
		if clientId == "" || conn.ClientId == clientId {
			targetConn = conn
			break
		}
	}

	if targetConn == nil {
		return nil, fmt.Errorf("client %s not found", clientId)
	}

	extFields := map[string]string{
		"consumerGroup": consumerGroup,
		"clientId":      targetConn.ClientId,
		"jstackEnable":  fmt.Sprintf("%t", jstack),
	}
	cmd := remoting.NewRequest(remoting.GetConsumerRunningInfo, extFields)

	// The request goes through a Broker, which forwards it to the client.
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

	return nil, fmt.Errorf("failed to get consumer running info")
}

// QueryTopicsByConsumer returns the topics a consumer group subscribes to.
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
		return nil, fmt.Errorf("failed to parse topic list: %w", err)
	}

	return &topicList, nil
}

// QueryConsumeTimeSpan returns how far a group's consumption lags, per queue.
func (c *Client) QueryConsumeTimeSpan(ctx context.Context, topic, consumerGroup string) ([]ConsumeTimeSpan, error) {
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

// GetAllSubscriptionGroup returns every subscription group on one Broker.
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
		return nil, fmt.Errorf("failed to parse subscription group list: %w", err)
	}

	return wrapper.SubscriptionGroupTable, nil
}

// UpdateConsumeOffset sets a consumer group's offset for one queue.
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

// ExamineConsumeStatsConcurrent returns a consumer group's progress, scoped to
// topic when topic is non-empty. Despite the name it does not fan out; it
// exists to mirror the Java admin API.
func (c *Client) ExamineConsumeStatsConcurrent(ctx context.Context, consumerGroup, topic string) (*ConsumeStats, error) {
	if topic != "" {
		return c.ExamineConsumeStatsByTopic(ctx, consumerGroup, topic)
	}
	return c.ExamineConsumeStats(ctx, consumerGroup)
}

// QueryConsumeTimeSpanConcurrent is an alias for QueryConsumeTimeSpan. Despite
// the name it is not concurrent.
func (c *Client) QueryConsumeTimeSpanConcurrent(ctx context.Context, topic, consumerGroup string) ([]ConsumeTimeSpan, error) {
	return c.QueryConsumeTimeSpan(ctx, topic, consumerGroup)
}

// QueryTopicsByConsumerConcurrent is an alias for QueryTopicsByConsumer.
// Despite the name it is not concurrent.
func (c *Client) QueryTopicsByConsumerConcurrent(ctx context.Context, consumerGroup string) (*TopicList, error) {
	return c.QueryTopicsByConsumer(ctx, consumerGroup)
}

// GetUserSubscriptionGroup returns the subscription groups on one Broker,
// excluding RocketMQ's own system groups.
func (c *Client) GetUserSubscriptionGroup(ctx context.Context, brokerAddr string) (map[string]*SubscriptionGroupConfig, error) {
	allGroups, err := c.GetAllSubscriptionGroup(ctx, brokerAddr)
	if err != nil {
		return nil, err
	}

	userGroups := make(map[string]*SubscriptionGroupConfig)
	for name, config := range allGroups {
		if !isSystemGroup(name) {
			userGroups[name] = config
		}
	}

	return userGroups, nil
}

// isSystemGroup reports whether groupName is one of RocketMQ's built-in groups.
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

// CloneGroupOffset copies one consumer group's offsets onto another.
// Java: CLONE_GROUP_OFFSET = 314; the Broker performs the copy.
func (c *Client) CloneGroupOffset(ctx context.Context, srcGroup, destGroup, topic string, isOffline bool) error {
	routeData, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to get topic route info: %w", err)
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
			return fmt.Errorf("failed to clone offsets to %s: %w", brokerAddr, err)
		}
		if resp.Code != remoting.Success {
			return NewAdminError(resp.Code, resp.Remark)
		}
	}

	return nil
}

// UpdateAndGetGroupReadForbidden sets a group's read permission on a topic and
// returns the permission actually in force.
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

	if v, ok := resp.ExtFields["readable"]; ok {
		return v == "true", nil
	}

	return readable, nil
}

// ColdDataFlowCtrConfig throttles a consumer group's reads of cold data.
type ColdDataFlowCtrConfig struct {
	ConsumerGroup   string `json:"consumerGroup"`
	ThresholdPerSec int64  `json:"thresholdPerSec"`
	GlobalThreshold int64  `json:"globalThreshold"`
	EnableFlowCtr   bool   `json:"enableFlowCtr"`
}

// ColdDataFlowCtrInfo is a consumer group's current cold-data throttling state.
type ColdDataFlowCtrInfo struct {
	ConsumerGroup    string `json:"consumerGroup"`
	CurrentQPS       int64  `json:"currentQPS"`
	ThresholdPerSec  int64  `json:"thresholdPerSec"`
	IsFlowCtrEnabled bool   `json:"isFlowCtrEnabled"`
	IsColdData       bool   `json:"isColdData"`
}

// UpdateColdDataFlowCtrGroupConfig applies a cold-data throttle on one Broker.
func (c *Client) UpdateColdDataFlowCtrGroupConfig(ctx context.Context, brokerAddr string, config ColdDataFlowCtrConfig) error {
	body, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal cold data flow control config: %w", err)
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

// RemoveColdDataFlowCtrGroupConfig removes a consumer group's cold-data throttle.
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

// GetColdDataFlowCtrInfo returns the cold-data throttling state of one Broker.
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
		return nil, fmt.Errorf("failed to parse cold data flow control info: %w", err)
	}

	return infos, nil
}

// UpdateColdDataFlowCtrGroupConfigInCluster applies a cold-data throttle to
// every Broker in a cluster, stopping at the first failure.
func (c *Client) UpdateColdDataFlowCtrGroupConfigInCluster(ctx context.Context, clusterName string, config ColdDataFlowCtrConfig) error {
	clusterInfo, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return err
	}

	brokerNames, ok := clusterInfo.ClusterAddrTable[clusterName]
	if !ok {
		return fmt.Errorf("cluster %s does not exist", clusterName)
	}

	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}

		for _, brokerAddr := range brokerData.BrokerAddrs {
			if err := c.UpdateColdDataFlowCtrGroupConfig(ctx, brokerAddr, config); err != nil {
				return fmt.Errorf("failed to update cold data flow control on %s: %w", brokerAddr, err)
			}
		}
	}

	return nil
}
