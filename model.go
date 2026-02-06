package admin

// =============================================================================
// 集群相关模型
// =============================================================================

// ClusterInfo 集群信息
type ClusterInfo struct {
	// BrokerAddrTable Broker 地址表 key: brokerName, value: BrokerData
	BrokerAddrTable map[string]*BrokerData `json:"brokerAddrTable"`

	// ClusterAddrTable 集群地址表 key: clusterName, value: brokerNames
	ClusterAddrTable map[string][]string `json:"clusterAddrTable"`
}

// BrokerData Broker 数据
type BrokerData struct {
	// Cluster 所属集群名称
	Cluster string `json:"cluster"`

	// BrokerName Broker 名称
	BrokerName string `json:"brokerName"`

	// BrokerAddrs Broker 地址 key: brokerId, value: address
	BrokerAddrs map[int64]string `json:"brokerAddrs"`
}

// KVTable 键值表
type KVTable struct {
	Table map[string]string `json:"table"`
}

// =============================================================================
// Topic 相关模型
// =============================================================================

// TopicConfig Topic 配置
type TopicConfig struct {
	// TopicName Topic 名称
	TopicName string `json:"topicName"`

	// ReadQueueNums 读队列数量
	ReadQueueNums int `json:"readQueueNums"`

	// WriteQueueNums 写队列数量
	WriteQueueNums int `json:"writeQueueNums"`

	// Perm 权限
	Perm int `json:"perm"`

	// TopicFilterType Topic 过滤类型
	TopicFilterType string `json:"topicFilterType"`

	// TopicSysFlag Topic 系统标志
	TopicSysFlag int `json:"topicSysFlag"`

	// Order 是否顺序消息
	Order bool `json:"order"`
}

// TopicList Topic 列表
type TopicList struct {
	TopicList  []string `json:"topicList"`
	BrokerAddr string   `json:"brokerAddr,omitempty"`
}

// TopicRouteData Topic 路由数据
type TopicRouteData struct {
	// OrderTopicConf 顺序 Topic 配置
	OrderTopicConf string `json:"orderTopicConf"`

	// QueueDatas 队列数据列表
	QueueDatas []*QueueData `json:"queueDatas"`

	// BrokerDatas Broker 数据列表
	BrokerDatas []*BrokerData `json:"brokerDatas"`

	// FilterServerTable 过滤服务器表
	FilterServerTable map[string][]string `json:"filterServerTable"`
}

// QueueData 队列数据
type QueueData struct {
	// BrokerName Broker 名称
	BrokerName string `json:"brokerName"`

	// ReadQueueNums 读队列数量
	ReadQueueNums int `json:"readQueueNums"`

	// WriteQueueNums 写队列数量
	WriteQueueNums int `json:"writeQueueNums"`

	// Perm 权限
	Perm int `json:"perm"`

	// TopicSysFlag Topic 系统标志
	TopicSysFlag int `json:"topicSysFlag"`
}

// TopicStatsTable Topic 统计表
type TopicStatsTable struct {
	// OffsetTable 偏移表 key: MessageQueue, value: TopicOffset
	OffsetTable map[string]*TopicOffset `json:"offsetTable"`
}

// TopicOffset Topic 偏移
type TopicOffset struct {
	// MinOffset 最小偏移
	MinOffset int64 `json:"minOffset"`

	// MaxOffset 最大偏移
	MaxOffset int64 `json:"maxOffset"`

	// LastUpdateTimestamp 最后更新时间戳
	LastUpdateTimestamp int64 `json:"lastUpdateTimestamp"`
}

// =============================================================================
// 消费者相关模型
// =============================================================================

// SubscriptionGroupConfig 订阅组配置
type SubscriptionGroupConfig struct {
	// GroupName 消费组名称
	GroupName string `json:"groupName"`

	// ConsumeEnable 是否允许消费
	ConsumeEnable bool `json:"consumeEnable"`

	// ConsumeFromMinEnable 是否允许从最小偏移消费
	ConsumeFromMinEnable bool `json:"consumeFromMinEnable"`

	// ConsumeBroadcastEnable 是否允许广播消费
	ConsumeBroadcastEnable bool `json:"consumeBroadcastEnable"`

	// RetryQueueNums 重试队列数量
	RetryQueueNums int `json:"retryQueueNums"`

	// RetryMaxTimes 最大重试次数
	RetryMaxTimes int `json:"retryMaxTimes"`

	// BrokerId Broker ID
	BrokerId int64 `json:"brokerId"`

	// WhichBrokerWhenConsumeSlowly 消费慢时使用的 Broker
	WhichBrokerWhenConsumeSlowly int64 `json:"whichBrokerWhenConsumeSlowly"`

	// NotifyConsumerIdsChangedEnable 是否通知消费者 ID 变更
	NotifyConsumerIdsChangedEnable bool `json:"notifyConsumerIdsChangedEnable"`
}

// ConsumeStats 消费统计
type ConsumeStats struct {
	// OffsetTable 偏移表
	OffsetTable map[string]*OffsetWrapper `json:"offsetTable"`

	// ConsumeTps 消费 TPS
	ConsumeTps float64 `json:"consumeTps"`
}

// OffsetWrapper 偏移包装器
type OffsetWrapper struct {
	// BrokerOffset Broker 偏移
	BrokerOffset int64 `json:"brokerOffset"`

	// ConsumerOffset 消费者偏移
	ConsumerOffset int64 `json:"consumerOffset"`

	// LastTimestamp 最后时间戳
	LastTimestamp int64 `json:"lastTimestamp"`

	// PullOffset 拉取偏移
	PullOffset int64 `json:"pullOffset"`
}

// ConsumerConnection 消费者连接
type ConsumerConnection struct {
	// ConnectionSet 连接集合
	ConnectionSet []*Connection `json:"connectionSet"`

	// SubscriptionTable 订阅表
	SubscriptionTable map[string]*SubscriptionData `json:"subscriptionTable"`

	// ConsumeType 消费类型
	ConsumeType string `json:"consumeType"`

	// MessageModel 消息模型
	MessageModel string `json:"messageModel"`

	// ConsumeFromWhere 消费起始位置
	ConsumeFromWhere string `json:"consumeFromWhere"`
}

// Connection 连接信息
type Connection struct {
	// ClientId 客户端 ID
	ClientId string `json:"clientId"`

	// ClientAddr 客户端地址
	ClientAddr string `json:"clientAddr"`

	// Language 语言
	Language string `json:"language"`

	// Version 版本
	Version int `json:"version"`
}

// SubscriptionData 订阅数据
type SubscriptionData struct {
	// ClassFilterMode 类过滤模式
	ClassFilterMode bool `json:"classFilterMode"`

	// Topic Topic 名称
	Topic string `json:"topic"`

	// SubString 订阅表达式
	SubString string `json:"subString"`

	// TagsSet 标签集合
	TagsSet []string `json:"tagsSet"`

	// CodeSet 代码集合
	CodeSet []int `json:"codeSet"`

	// SubVersion 订阅版本
	SubVersion int64 `json:"subVersion"`

	// ExpressionType 表达式类型
	ExpressionType string `json:"expressionType"`
}

// =============================================================================
// 消息相关模型
// =============================================================================

// MessageQueue 消息队列
type MessageQueue struct {
	// Topic Topic 名称
	Topic string `json:"topic"`

	// BrokerName Broker 名称
	BrokerName string `json:"brokerName"`

	// QueueId 队列 ID
	QueueId int `json:"queueId"`
}

// String 返回消息队列的字符串表示
func (mq *MessageQueue) String() string {
	return mq.Topic + "-" + mq.BrokerName + "-" + string(rune(mq.QueueId))
}

