package admin

import "strconv"

// ClusterInfo is the cluster topology reported by the NameServer.
type ClusterInfo struct {
	BrokerAddrTable  map[string]*BrokerData `json:"brokerAddrTable"`  // keyed by broker name
	ClusterAddrTable map[string][]string    `json:"clusterAddrTable"` // cluster name -> broker names
}

// BrokerData is one broker group: a master and its slaves.
type BrokerData struct {
	Cluster     string            `json:"cluster"`
	BrokerName  string            `json:"brokerName"`
	BrokerAddrs map[string]string `json:"brokerAddrs"` // broker id (as a string) -> address
}

// KVTable is the generic string map RocketMQ uses for stats and properties.
type KVTable struct {
	Table map[string]string `json:"table"`
}

// TopicConfig is a topic's per-broker configuration.
type TopicConfig struct {
	TopicName       string `json:"topicName"`
	ReadQueueNums   int    `json:"readQueueNums"`
	WriteQueueNums  int    `json:"writeQueueNums"`
	Perm            int    `json:"perm"` // bit 2 write, bit 3 read
	TopicFilterType string `json:"topicFilterType"`
	TopicSysFlag    int    `json:"topicSysFlag"`
	Order           bool   `json:"order"`
}

// TopicList is a set of topic names, optionally scoped to one broker.
type TopicList struct {
	TopicList  []string `json:"topicList"`
	BrokerAddr string   `json:"brokerAddr,omitempty"`
}

// TopicRouteData is a topic's routing table: which brokers hold which queues.
type TopicRouteData struct {
	OrderTopicConf    string              `json:"orderTopicConf"`
	QueueDatas        []*QueueData        `json:"queueDatas"`
	BrokerDatas       []*BrokerData       `json:"brokerDatas"`
	FilterServerTable map[string][]string `json:"filterServerTable"`
}

// QueueData is a topic's queue layout on one broker.
type QueueData struct {
	BrokerName     string `json:"brokerName"`
	ReadQueueNums  int    `json:"readQueueNums"`
	WriteQueueNums int    `json:"writeQueueNums"`
	Perm           int    `json:"perm"` // bit 2 write, bit 3 read
	TopicSysFlag   int    `json:"topicSysFlag"`
}

// TopicStatsTable holds per-queue offsets for one topic.
type TopicStatsTable struct {
	OffsetTable map[string]*TopicOffset `json:"offsetTable"` // keyed by MessageQueue
}

// TopicOffset is the offset range currently stored for one queue.
type TopicOffset struct {
	MinOffset           int64 `json:"minOffset"`
	MaxOffset           int64 `json:"maxOffset"`
	LastUpdateTimestamp int64 `json:"lastUpdateTimestamp"`
}

// SubscriptionGroupConfig is a consumer group's per-broker configuration.
type SubscriptionGroupConfig struct {
	GroupName                      string `json:"groupName"`
	ConsumeEnable                  bool   `json:"consumeEnable"`
	ConsumeFromMinEnable           bool   `json:"consumeFromMinEnable"`
	ConsumeBroadcastEnable         bool   `json:"consumeBroadcastEnable"`
	RetryQueueNums                 int    `json:"retryQueueNums"`
	RetryMaxTimes                  int    `json:"retryMaxTimes"`
	BrokerId                       int64  `json:"brokerId"`
	WhichBrokerWhenConsumeSlowly   int64  `json:"whichBrokerWhenConsumeSlowly"`
	NotifyConsumerIdsChangedEnable bool   `json:"notifyConsumerIdsChangedEnable"`
}

// ConsumeStats is a consumer group's progress across the queues it consumes.
type ConsumeStats struct {
	OffsetTable map[string]*OffsetWrapper `json:"offsetTable"` // keyed by MessageQueue
	ConsumeTps  float64                   `json:"consumeTps"`
}

// OffsetWrapper compares a queue's stored offset with the group's consumed one;
// the difference is the backlog.
type OffsetWrapper struct {
	BrokerOffset   int64 `json:"brokerOffset"`
	ConsumerOffset int64 `json:"consumerOffset"`
	LastTimestamp  int64 `json:"lastTimestamp"`
	PullOffset     int64 `json:"pullOffset"`
}

// ConsumerConnection lists the live clients of one consumer group.
type ConsumerConnection struct {
	ConnectionSet     []*Connection                `json:"connectionSet"`
	SubscriptionTable map[string]*SubscriptionData `json:"subscriptionTable"` // keyed by topic
	ConsumeType       string                       `json:"consumeType"`
	MessageModel      string                       `json:"messageModel"`
	ConsumeFromWhere  string                       `json:"consumeFromWhere"`
}

// Connection is one connected producer or consumer client.
type Connection struct {
	ClientId   string `json:"clientId"`
	ClientAddr string `json:"clientAddr"`
	Language   string `json:"language"`
	Version    int    `json:"version"`
}

// SubscriptionData is one group's subscription to one topic.
type SubscriptionData struct {
	ClassFilterMode bool     `json:"classFilterMode"`
	Topic           string   `json:"topic"`
	SubString       string   `json:"subString"` // tag expression, or "*" for all
	TagsSet         []string `json:"tagsSet"`
	CodeSet         []int    `json:"codeSet"` // hashes of TagsSet, used for broker-side filtering
	SubVersion      int64    `json:"subVersion"`
	ExpressionType  string   `json:"expressionType"`
}

// MessageQueue identifies one queue of one topic on one broker.
type MessageQueue struct {
	Topic      string `json:"topic"`
	BrokerName string `json:"brokerName"`
	QueueId    int    `json:"queueId"`
}

// String returns topic-brokerName-queueId.
func (mq *MessageQueue) String() string {
	return mq.Topic + "-" + mq.BrokerName + "-" + strconv.Itoa(mq.QueueId)
}
