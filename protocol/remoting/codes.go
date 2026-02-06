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

	// ========== ACL 用户管理相关 ==========

	// CreateUser 创建用户
	CreateUser = 356

	// UpdateUser 更新用户
	UpdateUser = 357

	// DeleteUser 删除用户
	DeleteUser = 358

	// GetUser 获取用户
	GetUser = 359

	// ListUser 列出用户
	ListUser = 360

	// CreateAcl 创建 ACL
	CreateAcl = 361

	// UpdateAcl 更新 ACL
	UpdateAcl = 362

	// DeleteAcl 删除 ACL
	DeleteAcl = 363

	// GetAcl 获取 ACL
	GetAcl = 364

	// ListAcl 列出 ACL
	ListAcl = 365

	// ========== 更多 Topic 相关 ==========

	// GetAllTopicConfig 获取所有 Topic 配置
	GetAllTopicConfig = 21

	// QueryTopicConsumeByWho 查询 Topic 被谁消费
	QueryTopicConsumeByWho = 300

	// ========== 更多消费者相关 ==========

	// QueryTopicsByConsumer 查询消费者订阅的 Topic
	QueryTopicsByConsumer = 343

	// QuerySubscription 查询订阅信息
	QuerySubscription = 344

	// QueryConsumeTimeSpan 查询消费时间跨度
	QueryConsumeTimeSpan = 302

	// GetConsumeStatus 获取消费状态
	GetConsumeStatus = 304

	// GetAllSubscriptionGroup 获取所有订阅组
	GetAllSubscriptionGroup = 201

	// ========== 生产者相关 ==========

	// GetProducerInfo 获取生产者信息
	GetProducerInfo = 305

	// ========== 消息相关 ==========

	// QueryMessage 查询消息
	QueryMessage = 12

	// ViewMessageById 按 ID 查看消息
	ViewMessageById = 33

	// ========== Offset 扩展 ==========

	// UpdateConsumeOffset 更新消费 Offset
	UpdateConsumeOffset = 221

	// ResetOffsetByQueueId 按队列 ID 重置 Offset
	ResetOffsetByQueueId = 222

	// ========== Broker 扩展 ==========

	// WipeWritePermOfBroker 清除 Broker 写权限
	WipeWritePermOfBroker = 41

	// AddWritePermOfBroker 添加 Broker 写权限
	AddWritePermOfBroker = 42

	// ViewBrokerStatsData 查看 Broker 统计数据
	ViewBrokerStatsData = 210

	// GetBrokerHAStatus 获取 Broker HA 状态
	GetBrokerHAStatus = 339

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
