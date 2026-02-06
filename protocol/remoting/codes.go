// Package remoting RocketMQ 请求码定义
package remoting

// 请求码定义（对应 Java RequestCode）
const (
	// ========== NameServer 相关 ==========

	// GetRouteInfoByTopic 获取 Topic 路由信息
	GetRouteInfoByTopic = 105

	// GetBrokerClusterInfo 获取集群信息
	GetBrokerClusterInfo = 106

	// ========== Broker 相关 ==========

	// UpdateBrokerConfig 更新 Broker 配置
	UpdateBrokerConfig = 25

	// GetBrokerConfig 获取 Broker 配置
	GetBrokerConfig = 26

	// GetBrokerRuntimeInfo 获取 Broker 运行时信息
	GetBrokerRuntimeInfo = 28

	// ========== Topic 相关 ==========

	// UpdateAndCreateTopic 创建或更新 Topic
	UpdateAndCreateTopic = 17

	// DeleteTopicInBroker 在 Broker 中删除 Topic
	DeleteTopicInBroker = 215

	// DeleteTopicInNamesrv 在 NameServer 中删除 Topic
	DeleteTopicInNamesrv = 216

	// GetAllTopicListFromNamesrv 从 NameServer 获取所有 Topic 列表
	GetAllTopicListFromNamesrv = 206

	// GetTopicStatsInfo 获取 Topic 统计信息
	GetTopicStatsInfo = 202

	// GetTopicsByCluster 按集群获取 Topic 列表
	GetTopicsByCluster = 224

	// ========== 消费者相关 ==========

	// UpdateAndCreateSubscriptionGroup 创建或更新订阅组
	UpdateAndCreateSubscriptionGroup = 200

	// DeleteSubscriptionGroup 删除订阅组
	DeleteSubscriptionGroup = 207

	// GetAllSubscriptionGroupConfig 获取所有订阅组配置
	GetAllSubscriptionGroupConfig = 201

	// GetSubscriptionGroupConfig 获取订阅组配置
	GetSubscriptionGroupConfig = 209

	// GetConsumeStats 获取消费统计
	GetConsumeStats = 208

	// GetConsumerConnectionList 获取消费者连接列表
	GetConsumerConnectionList = 203

	// GetConsumerRunningInfo 获取消费者运行时信息
	GetConsumerRunningInfo = 307

	// ========== 生产者相关 ==========

	// GetProducerConnectionList 获取生产者连接列表
	GetProducerConnectionList = 204

	// ========== Offset 相关 ==========

	// SearchOffsetByTimestamp 按时间戳搜索 Offset
	SearchOffsetByTimestamp = 29

	// GetMaxOffset 获取最大 Offset
	GetMaxOffset = 30

	// GetMinOffset 获取最小 Offset
	GetMinOffset = 31

	// ResetConsumerOffset 重置消费者 Offset
	ResetConsumerOffset = 220

	// ========== ACL 相关 ==========

	// UpdateAclConfig 更新 ACL 配置
	UpdateAclConfig = 328

	// DeleteAclConfig 删除 ACL 配置
	DeleteAclConfig = 329

	// GetBrokerAclConfig 获取 Broker ACL 配置
	GetBrokerAclConfig = 330

	// GetBrokerAclConfigVersion Broker ACL 配置版本
	GetBrokerAclConfigVersion = 331

	// ========== NameServer 配置相关 ==========

	// UpdateNamesrvConfig 更新 NameServer 配置
	UpdateNamesrvConfig = 318

	// GetNamesrvConfig 获取 NameServer 配置
	GetNamesrvConfig = 319

	// ========== Controller 相关 (RocketMQ 5.x) ==========

	// ControllerGetMetadataInfo Controller 获取元数据
	ControllerGetMetadataInfo = 501

	// ControllerElectMaster Controller 选举 Master
	ControllerElectMaster = 503
)

// 响应码定义
const (
	// Success 成功
	Success = 0

	// SystemError 系统错误
	SystemError = 1

	// SystemBusy 系统繁忙
	SystemBusy = 2

	// RequestCodeNotSupported 请求码不支持
	RequestCodeNotSupported = 3

	// TopicNotExist Topic 不存在
	TopicNotExist = 17

	// SubscriptionNotExist 订阅不存在
	SubscriptionNotExist = 21

	// ConsumerNotOnline 消费者不在线
	ConsumerNotOnline = 206
)

