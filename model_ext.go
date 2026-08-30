package admin

// UserInfo is a RocketMQ 5.x ACL user.
type UserInfo struct {
	Username    string   `json:"username"`
	Password    string   `json:"password"` // encrypted
	UserType    string   `json:"userType"`
	UserStatus  string   `json:"userStatus"`
	Permissions []string `json:"permissions"`
}

// AclInfo is the set of policies attached to one subject.
type AclInfo struct {
	Subject     string      `json:"subject"` // a user or a group
	Policies    []AclPolicy `json:"policies"`
	Description string      `json:"description"`
}

// AclPolicy grants or denies actions on one resource.
type AclPolicy struct {
	Resource  string   `json:"resource"`  // a topic or a group
	Actions   []string `json:"actions"`   // PUB, SUB
	Effect    string   `json:"effect"`    // ALLOW, DENY
	SourceIPs []string `json:"sourceIps"` // empty means any source
	Decision  string   `json:"decision"`
}

// UserList is the response of a user listing.
type UserList struct {
	Users []UserInfo `json:"users"`
}

// AclList is the response of an ACL listing.
type AclList struct {
	Acls []AclInfo `json:"acls"`
}

// PlainAccessConfig is one entry of a RocketMQ 4.x plain_acl.yml file.
type PlainAccessConfig struct {
	AccessKey          string   `json:"accessKey"`
	SecretKey          string   `json:"secretKey"`
	WhiteRemoteAddress string   `json:"whiteRemoteAddress"`
	Admin              bool     `json:"admin"`
	DefaultTopicPerm   string   `json:"defaultTopicPerm"` // DENY, PUB, SUB or PUB|SUB
	DefaultGroupPerm   string   `json:"defaultGroupPerm"` // same values as DefaultTopicPerm
	TopicPerms         []string `json:"topicPerms"`       // e.g. ["topicA=PUB", "topicB=SUB"]
	GroupPerms         []string `json:"groupPerms"`       // e.g. ["groupA=SUB"]
}

// BrokerClusterAclVersionInfo is the response to request code 52.
type BrokerClusterAclVersionInfo struct {
	BrokerAddr        string            `json:"brokerAddr"`
	BrokerName        string            `json:"brokerName"`
	ClusterName       string            `json:"clusterName"`
	Version           string            `json:"version"`
	AllAclFileVersion map[string]string `json:"allAclFileVersion"` // keyed by ACL file path
}

// ConsumeStatsList is the response to GET_BROKER_CONSUME_STATS.
type ConsumeStatsList struct {
	BrokerAddr       string                   `json:"brokerAddr"`
	TotalDiff        int64                    `json:"totalDiff"` // total backlog across all groups
	ConsumeStatsList []map[string]interface{} `json:"consumeStatsList"`
}

// MessageTrack records what one consumer group did with a message.
type MessageTrack struct {
	ConsumerGroup  string `json:"consumerGroup"`
	TrackType      string `json:"trackType"`
	ExceptionDesc  string `json:"exceptionDesc"`
	ConsumedStatus bool   `json:"consumedStatus"`
}

// ConsumeTimeSpan is how far one queue's consumption lags behind its messages.
type ConsumeTimeSpan struct {
	MinTimeStamp     int64        `json:"minTimeStamp"`
	MaxTimeStamp     int64        `json:"maxTimeStamp"`
	ConsumeTimeStamp int64        `json:"consumeTimeStamp"`
	MessageQueue     MessageQueue `json:"messageQueue"`
	DelayTime        int64        `json:"delayTime"`
}

// ConsumerRunningInfo is a snapshot reported by a live consumer client.
type ConsumerRunningInfo struct {
	Properties      map[string]string        `json:"properties"`
	SubscriptionSet []SubscriptionData       `json:"subscriptionSet"`
	MqTable         map[string]ProcessQueue  `json:"mqTable"` // keyed by MessageQueue
	StatusTable     map[string]ConsumeStatus `json:"statusTable"`
	JStack          string                   `json:"jstack"` // consumer-side thread dump
}

// SubscriptionDataExt is the subscription form used by consumer runtime info.
type SubscriptionDataExt struct {
	Topic           string   `json:"topic"`
	SubString       string   `json:"subString"` // tag expression, or "*" for all
	TagsSet         []string `json:"tagsSet"`
	ClassFilterMode bool     `json:"classFilterMode"`
	ExpressionType  string   `json:"expressionType"`
}

// ProcessQueue is a consumer's local view of one queue it is processing.
//
// The names are RocketMQ's ProcessQueueInfo, including its "droped" spelling:
// this is the client's own snapshot, so anything else silently decodes to zero.
type ProcessQueue struct {
	CommitOffset       int64 `json:"commitOffset"`
	CachedMsgMinOffset int64 `json:"cachedMsgMinOffset"`
	CachedMsgMaxOffset int64 `json:"cachedMsgMaxOffset"`
	CachedMsgCount     int64 `json:"cachedMsgCount"`
	CachedMsgSizeInMiB int64 `json:"cachedMsgSizeInMiB"`

	TransactionMsgMinOffset int64 `json:"transactionMsgMinOffset"`
	TransactionMsgMaxOffset int64 `json:"transactionMsgMaxOffset"`
	TransactionMsgCount     int64 `json:"transactionMsgCount"`

	Locked            bool  `json:"locked"`
	TryUnlockTimes    int64 `json:"tryUnlockTimes"`
	LastLockTimestamp int64 `json:"lastLockTimestamp"`

	Dropped              bool  `json:"droped"` // rebalanced away; no longer consumed
	LastPullTimestamp    int64 `json:"lastPullTimestamp"`
	LastConsumeTimestamp int64 `json:"lastConsumeTimestamp"`
}

// ConsumeStatus is one consumer's throughput and latency counters.
type ConsumeStatus struct {
	PullRT            float64 `json:"pullRT"` // pull response time
	PullTPS           float64 `json:"pullTPS"`
	ConsumeRT         float64 `json:"consumeRT"` // consume response time
	ConsumeOKTPS      float64 `json:"consumeOKTPS"`
	ConsumeFailedTPS  float64 `json:"consumeFailedTPS"`
	ConsumeFailedMsgs int64   `json:"consumeFailedMsgs"`
}

// ProducerConnection lists the live clients of one producer group.
type ProducerConnection struct {
	ConnectionSet []Connection `json:"connectionSet"`
}

// BrokerHAStatus is a master's view of its replication to slaves.
type BrokerHAStatus struct {
	MasterAddr      string           `json:"masterAddr"`
	HaMaxGap        int64            `json:"haMaxGap"` // largest offset gap across slaves
	InSyncSlaveNum  int              `json:"inSyncSlaveNum"`
	HaConnectionSet []HaClientStatus `json:"haConnectionSet"`
}

// HaClientStatus is the replication state of one slave.
type HaClientStatus struct {
	Addr              string `json:"addr"`
	TransferredOffset int64  `json:"transferredOffset"`
	Diff              int64  `json:"diff"` // offsets behind the master
	InSync            bool   `json:"inSync"`
}

// BrokerStatsData is one statistics series sampled over three windows.
type BrokerStatsData struct {
	StatsMinute BrokerStatsItem `json:"statsMinute"`
	StatsHour   BrokerStatsItem `json:"statsHour"`
	StatsDay    BrokerStatsItem `json:"statsDay"`
	ClusterName string          `json:"clusterName"`
	BrokerName  string          `json:"brokerName"`
}

// BrokerStatsItem is one statistics window.
type BrokerStatsItem struct {
	Sum   int64   `json:"sum"`
	Tps   float64 `json:"tps"`
	Avgpt float64 `json:"avgpt"` // average processing time
}

// MessageExt is a stored message with the metadata the broker added to it.
type MessageExt struct {
	Topic          string            `json:"topic"`
	QueueId        int               `json:"queueId"`
	QueueOffset    int64             `json:"queueOffset"`
	MsgId          string            `json:"msgId"`
	OffsetMsgId    string            `json:"offsetMsgId"` // derived from the store host and CommitLog offset
	Body           []byte            `json:"body"`
	Flag           int               `json:"flag"`
	BornTimestamp  int64             `json:"bornTimestamp"`
	StoreTimestamp int64             `json:"storeTimestamp"`
	BornHost       string            `json:"bornHost"`
	StoreHost      string            `json:"storeHost"`
	SysFlag        int               `json:"sysFlag"`
	BrokerName     string            `json:"brokerName"`
	Properties     map[string]string `json:"properties"`
}
