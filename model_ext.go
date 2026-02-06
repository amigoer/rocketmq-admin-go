package admin

// =============================================================================
// ACL 用户管理
// =============================================================================

// UserInfo 用户信息
type UserInfo struct {
	Username    string   `json:"username"`    // 用户名
	Password    string   `json:"password"`    // 密码（加密）
	UserType    string   `json:"userType"`    // 用户类型
	UserStatus  string   `json:"userStatus"`  // 用户状态
	Permissions []string `json:"permissions"` // 权限列表
}

// AclInfo ACL 规则信息
type AclInfo struct {
	Subject     string      `json:"subject"`     // 主体（用户或组）
	Policies    []AclPolicy `json:"policies"`    // 策略列表
	Description string      `json:"description"` // 描述
}

// AclPolicy ACL 策略
type AclPolicy struct {
	Resource  string   `json:"resource"`  // 资源（Topic、Group）
	Actions   []string `json:"actions"`   // 操作（PUB、SUB）
	Effect    string   `json:"effect"`    // 效果（ALLOW、DENY）
	SourceIPs []string `json:"sourceIps"` // 来源 IP 限制
	Decision  string   `json:"decision"`  // 决策
}

// UserList 用户列表
type UserList struct {
	Users []UserInfo `json:"users"`
}

// AclList ACL 列表
type AclList struct {
	Acls []AclInfo `json:"acls"`
}

// =============================================================================
// 消息轨迹
// =============================================================================

// MessageTrack 消息轨迹
type MessageTrack struct {
	ConsumerGroup  string `json:"consumerGroup"`  // 消费者组
	TrackType      string `json:"trackType"`      // 轨迹类型
	ExceptionDesc  string `json:"exceptionDesc"`  // 异常描述
	ConsumedStatus bool   `json:"consumedStatus"` // 消费状态
}

// =============================================================================
// 消费时间跨度
// =============================================================================

// ConsumeTimeSpan 消费时间跨度
type ConsumeTimeSpan struct {
	MinTimeStamp     int64        `json:"minTimeStamp"`     // 最小时间戳
	MaxTimeStamp     int64        `json:"maxTimeStamp"`     // 最大时间戳
	ConsumeTimeStamp int64        `json:"consumeTimeStamp"` // 消费时间戳
	MessageQueue     MessageQueue `json:"messageQueue"`     // 消息队列
	DelayTime        int64        `json:"delayTime"`        // 延迟时间
}

// =============================================================================
// 消费者运行时信息
// =============================================================================

// ConsumerRunningInfo 消费者运行时信息
type ConsumerRunningInfo struct {
	Properties      map[string]string        `json:"properties"`      // 属性
	SubscriptionSet []SubscriptionData       `json:"subscriptionSet"` // 订阅集合
	MqTable         map[string]ProcessQueue  `json:"mqTable"`         // 队列表
	StatusTable     map[string]ConsumeStatus `json:"statusTable"`     // 状态表
	JStack          string                   `json:"jstack"`          // 堆栈信息
}

// SubscriptionDataExt 订阅数据扩展
type SubscriptionDataExt struct {
	Topic           string   `json:"topic"`           // Topic
	SubString       string   `json:"subString"`       // 订阅表达式
	TagsSet         []string `json:"tagsSet"`         // Tag 集合
	ClassFilterMode bool     `json:"classFilterMode"` // 类过滤模式
	ExpressionType  string   `json:"expressionType"`  // 表达式类型
}

// ProcessQueue 处理队列
type ProcessQueue struct {
	Locked          bool  `json:"locked"`          // 是否锁定
	TryUnlockTimes  int64 `json:"tryUnlockTimes"`  // 尝试解锁次数
	LastLockTime    int64 `json:"lastLockTime"`    // 最后锁定时间
	Dropped         bool  `json:"dropped"`         // 是否丢弃
	LastPullTime    int64 `json:"lastPullTime"`    // 最后拉取时间
	LastConsumeTime int64 `json:"lastConsumeTime"` // 最后消费时间
	MsgCount        int64 `json:"msgCount"`        // 消息数量
	MsgSize         int64 `json:"msgSize"`         // 消息大小
}

// ConsumeStatus 消费状态
type ConsumeStatus struct {
	PullRT            float64 `json:"pullRT"`            // 拉取 RT
	PullTPS           float64 `json:"pullTPS"`           // 拉取 TPS
	ConsumeRT         float64 `json:"consumeRT"`         // 消费 RT
	ConsumeOKTPS      float64 `json:"consumeOKTPS"`      // 消费成功 TPS
	ConsumeFailedTPS  float64 `json:"consumeFailedTPS"`  // 消费失败 TPS
	ConsumeFailedMsgs int64   `json:"consumeFailedMsgs"` // 消费失败消息数
}

// =============================================================================
// 生产者连接信息
// =============================================================================

// ProducerConnection 生产者连接信息
type ProducerConnection struct {
	ConnectionSet []Connection `json:"connectionSet"` // 连接集合
}

// =============================================================================
// Broker HA 状态
// =============================================================================

// BrokerHAStatus Broker HA 状态
type BrokerHAStatus struct {
	MasterAddr      string           `json:"masterAddr"`      // Master 地址
	HaMaxGap        int64            `json:"haMaxGap"`        // HA 最大差距
	InSyncSlaveNum  int              `json:"inSyncSlaveNum"`  // 同步 Slave 数量
	HaConnectionSet []HaClientStatus `json:"haConnectionSet"` // HA 连接状态
}

// HaClientStatus HA 客户端状态
type HaClientStatus struct {
	Addr              string `json:"addr"`              // 地址
	TransferredOffset int64  `json:"transferredOffset"` // 已传输偏移
	Diff              int64  `json:"diff"`              // 差距
	InSync            bool   `json:"inSync"`            // 是否同步
}

// =============================================================================
// Broker 统计数据
// =============================================================================

// BrokerStatsData Broker 统计数据
type BrokerStatsData struct {
	StatsMinute BrokerStatsItem `json:"statsMinute"` // 分钟统计
	StatsHour   BrokerStatsItem `json:"statsHour"`   // 小时统计
	StatsDay    BrokerStatsItem `json:"statsDay"`    // 天统计
	ClusterName string          `json:"clusterName"` // 集群名
	BrokerName  string          `json:"brokerName"`  // Broker 名
}

// BrokerStatsItem Broker 统计项
type BrokerStatsItem struct {
	Sum   int64   `json:"sum"`   // 总和
	Tps   float64 `json:"tps"`   // TPS
	Avgpt float64 `json:"avgpt"` // 平均 PT
}

// =============================================================================
// 消息相关
// =============================================================================

// MessageExt 消息扩展信息
type MessageExt struct {
	Topic          string            `json:"topic"`          // Topic
	QueueId        int               `json:"queueId"`        // 队列 ID
	QueueOffset    int64             `json:"queueOffset"`    // 队列偏移
	MsgId          string            `json:"msgId"`          // 消息 ID
	OffsetMsgId    string            `json:"offsetMsgId"`    // 偏移消息 ID
	Body           []byte            `json:"body"`           // 消息体
	Flag           int               `json:"flag"`           // 标志
	BornTimestamp  int64             `json:"bornTimestamp"`  // 发送时间戳
	StoreTimestamp int64             `json:"storeTimestamp"` // 存储时间戳
	BornHost       string            `json:"bornHost"`       // 发送方
	StoreHost      string            `json:"storeHost"`      // 存储方
	SysFlag        int               `json:"sysFlag"`        // 系统标志
	BrokerName     string            `json:"brokerName"`     // Broker 名称
	Properties     map[string]string `json:"properties"`     // 属性
}
