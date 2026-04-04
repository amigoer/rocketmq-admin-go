// Package remoting RocketMQ 请求码定义
// 对照 Java 源码 RequestCode.java (master branch + 4.9.8 branch)
package remoting

// 请求码定义
const (
	// ========== 消息基础操作 ==========

	// PullMessage 拉取消息
	PullMessage = 11

	// QueryMessage 查询消息
	QueryMessage = 12

	// UpdateConsumerOffset 更新消费 Offset (Java: UPDATE_CONSUMER_OFFSET)
	UpdateConsumerOffset = 15

	// UpdateAndCreateTopic 创建或更新 Topic
	UpdateAndCreateTopic = 17

	// GetAllTopicConfig 获取所有 Topic 配置
	GetAllTopicConfig = 21

	// UpdateBrokerConfig 更新 Broker 配置
	UpdateBrokerConfig = 25

	// GetBrokerConfig 获取 Broker 配置
	GetBrokerConfig = 26

	// GetBrokerRuntimeInfo 获取 Broker 运行时信息
	GetBrokerRuntimeInfo = 28

	// SearchOffsetByTimestamp 按时间戳搜索 Offset
	SearchOffsetByTimestamp = 29

	// GetMaxOffset 获取最大 Offset
	GetMaxOffset = 30

	// GetMinOffset 获取最小 Offset
	GetMinOffset = 31

	// ViewMessageById 按 ID 查看消息
	ViewMessageById = 33

	// ========== 旧版 ACL 相关（RocketMQ 4.x plain_acl.yml）==========

	// UpdateAndCreateAclConfig 创建或更新旧版 ACL 配置 (Java: UPDATE_AND_CREATE_ACL_CONFIG)
	UpdateAndCreateAclConfig = 50

	// DeleteAclConfig 删除旧版 ACL 配置 (Java: DELETE_ACL_CONFIG)
	DeleteAclConfig = 51

	// GetBrokerClusterAclInfo 获取 Broker 集群 ACL 信息 (Java: GET_BROKER_CLUSTER_ACL_INFO)
	GetBrokerClusterAclInfo = 52

	// UpdateGlobalWhiteAddrsConfig 更新全局白名单地址 (Java: UPDATE_GLOBAL_WHITE_ADDRS_CONFIG)
	UpdateGlobalWhiteAddrsConfig = 53

	// ========== KV 配置管理 ==========

	// PutKVConfig 存储 KV 配置
	PutKVConfig = 100

	// GetKVConfig 获取 KV 配置
	GetKVConfig = 101

	// DeleteKVConfig 删除 KV 配置
	DeleteKVConfig = 102

	// ========== NameServer 相关 ==========

	// GetRouteInfoByTopic 获取 Topic 路由信息
	GetRouteInfoByTopic = 105

	// GetBrokerClusterInfo 获取集群信息
	GetBrokerClusterInfo = 106

	// ========== 消费者/订阅组/Topic 管理 ==========

	// UpdateAndCreateSubscriptionGroup 创建或更新订阅组
	UpdateAndCreateSubscriptionGroup = 200

	// GetAllSubscriptionGroupConfig 获取所有订阅组配置
	GetAllSubscriptionGroupConfig = 201

	// GetTopicStatsInfo 获取 Topic 统计信息
	GetTopicStatsInfo = 202

	// GetConsumerConnectionList 获取消费者连接列表
	GetConsumerConnectionList = 203

	// GetProducerConnectionList 获取生产者连接列表
	GetProducerConnectionList = 204

	// WipeWritePermOfBroker 清除 Broker 写权限 (Java: WIPE_WRITE_PERM_OF_BROKER)
	WipeWritePermOfBroker = 205

	// GetAllTopicListFromNamesrv 从 NameServer 获取所有 Topic 列表
	GetAllTopicListFromNamesrv = 206

	// DeleteSubscriptionGroup 删除订阅组
	DeleteSubscriptionGroup = 207

	// GetConsumeStats 获取消费统计
	GetConsumeStats = 208

	// DeleteTopicInBroker 在 Broker 中删除 Topic
	DeleteTopicInBroker = 215

	// DeleteTopicInNamesrv 在 NameServer 中删除 Topic
	DeleteTopicInNamesrv = 216

	// GetKVListByNamespace 按命名空间获取 KV 列表
	GetKVListByNamespace = 219

	// ResetConsumerOffset 重置消费者 Offset（admin → broker → consumer）
	// Java: RESET_CONSUMER_CLIENT_OFFSET = 220（broker → consumer 方向）
	// admin 工具实际应使用 InvokeBrokerToResetOffset(222)，但此处保留名称兼容
	ResetConsumerOffset = 222

	// GetConsumerStatusFromClient 获取消费者状态（broker → consumer 方向）
	GetConsumerStatusFromClient = 221

	// InvokeBrokerToGetConsumerStatus admin → broker 获取消费者状态
	// Java: INVOKE_BROKER_TO_GET_CONSUMER_STATUS
	InvokeBrokerToGetConsumerStatus = 223

	// GetTopicsByCluster 按集群获取 Topic 列表
	GetTopicsByCluster = 224

	// ========== 300 段 ==========

	// QueryTopicConsumeByWho 查询 Topic 被谁消费
	QueryTopicConsumeByWho = 300

	// QueryConsumeTimeSpan 查询消费时间跨度 (Java: QUERY_CONSUME_TIME_SPAN)
	QueryConsumeTimeSpan = 303

	// CleanExpiredConsumeQueue 清理过期消费队列 (Java: CLEAN_EXPIRED_CONSUMEQUEUE)
	CleanExpiredConsumeQueue = 306

	// GetConsumerRunningInfo 获取消费者运行时信息
	GetConsumerRunningInfo = 307

	// ConsumeMessageDirectly 直接消费消息
	ConsumeMessageDirectly = 309

	// CloneGroupOffset 克隆消费组偏移 (Java: CLONE_GROUP_OFFSET)
	CloneGroupOffset = 314

	// ViewBrokerStatsData 查看 Broker 统计数据 (Java: VIEW_BROKER_STATS_DATA)
	ViewBrokerStatsData = 315

	// CleanUnusedTopic 清理未使用 Topic (Java: CLEAN_UNUSED_TOPIC)
	CleanUnusedTopic = 316

	// GetBrokerConsumeStats 获取 Broker 消费统计 (Java: GET_BROKER_CONSUME_STATS)
	GetBrokerConsumeStats = 317

	// UpdateNamesrvConfig 更新 NameServer 配置
	UpdateNamesrvConfig = 318

	// GetNamesrvConfig 获取 NameServer 配置
	GetNamesrvConfig = 319

	// QueryConsumeQueue 查询消费队列 (Java: QUERY_CONSUME_QUEUE)
	QueryConsumeQueue = 321

	// ResumeCheckHalfMessage 恢复检查半消息 (Java: RESUME_CHECK_HALF_MESSAGE)
	ResumeCheckHalfMessage = 323

	// AddWritePermOfBroker 添加 Broker 写权限 (Java: ADD_WRITE_PERM_OF_BROKER)
	AddWritePermOfBroker = 327

	// GetAllProducerInfo 获取所有生产者信息 (Java: GET_ALL_PRODUCER_INFO)
	GetProducerInfo = 328

	// DeleteExpiredCommitLog 删除过期 CommitLog (Java: DELETE_EXPIRED_COMMITLOG)
	DeleteExpiredCommitLog = 329

	// ========== 340+ ==========

	// QueryTopicsByConsumer 查询消费者订阅的 Topic
	QueryTopicsByConsumer = 343

	// QuerySubscription 查询订阅信息 (Java: QUERY_SUBSCRIPTION_BY_CONSUMER)
	QuerySubscription = 345

	// GetTopicConfig 获取单个 Topic 配置 (Java: GET_TOPIC_CONFIG)
	GetTopicConfig = 351

	// GetSubscriptionGroupConfig 获取订阅组配置 (Java: GET_SUBSCRIPTIONGROUP_CONFIG)
	GetSubscriptionGroupConfig = 352

	// UpdateAndGetGroupForbidden 更新并获取组禁止状态 (Java: UPDATE_AND_GET_GROUP_FORBIDDEN)
	UpdateAndGetGroupForbidden = 353

	// CheckRocksdbCqWriteProgress 检查 RocksDB CQ 写入进度 (Java: CHECK_ROCKSDB_CQ_WRITE_PROGRESS)
	CheckRocksdbCqWriteProgress = 354

	// ExportRocksDBConfigToJson 导出 RocksDB 配置 (Java: EXPORT_ROCKSDB_CONFIG_TO_JSON)
	ExportRocksDBConfigToJson = 355

	// ========== 消息请求模式 ==========

	// SetMessageRequestMode 设置消息请求模式 (Java: SET_MESSAGE_REQUEST_MODE)
	SetMessageRequestMode = 401

	// ========== 静态 Topic ==========

	// CreateStaticTopic 创建静态 Topic (Java: UPDATE_AND_CREATE_STATIC_TOPIC)
	CreateStaticTopic = 513

	// ========== Broker 容器/HA (RocketMQ 5.x) ==========

	// AddBrokerToContainer 添加 Broker 到容器 (Java: ADD_BROKER)
	AddBrokerToContainer = 902

	// RemoveBrokerFromContainer 从容器移除 Broker (Java: REMOVE_BROKER)
	RemoveBrokerFromContainer = 903

	// GetBrokerHAStatus 获取 Broker HA 状态 (Java: GET_BROKER_HA_STATUS)
	GetBrokerHAStatus = 907

	// ResetMasterFlushOffset 重置 Master Flush Offset (Java: RESET_MASTER_FLUSH_OFFSET)
	ResetMasterFlushOffset = 908

	// ========== Controller 相关 (RocketMQ 5.x) ==========

	// ControllerElectMaster Controller 选举 Master (Java: CONTROLLER_ELECT_MASTER)
	ControllerElectMaster = 1002

	// ControllerGetMetadataInfo Controller 获取元数据 (Java: CONTROLLER_GET_METADATA_INFO)
	ControllerGetMetadataInfo = 1005

	// GetInSyncStateData 获取同步状态数据 (Java: CONTROLLER_GET_SYNC_STATE_DATA)
	GetInSyncStateData = 1006

	// GetBrokerEpochCache 获取 Broker Epoch 缓存 (Java: GET_BROKER_EPOCH_CACHE)
	GetBrokerEpochCache = 1007

	// ControllerUpdateConfig 更新 Controller 配置 (Java: UPDATE_CONTROLLER_CONFIG)
	ControllerUpdateConfig = 1009

	// ControllerGetConfig 获取 Controller 配置 (Java: GET_CONTROLLER_CONFIG)
	ControllerGetConfig = 1010

	// CleanControllerBrokerData 清理 Controller Broker 数据 (Java: CLEAN_BROKER_DATA)
	CleanControllerBrokerData = 1011

	// ========== 冷数据流控 ==========

	// UpdateColdDataFlowCtrGroupConfig 更新冷数据流控配置 (Java: UPDATE_COLD_DATA_FLOW_CTR_CONFIG)
	UpdateColdDataFlowCtrGroupConfig = 2001

	// RemoveColdDataFlowCtrGroupConfig 移除冷数据流控配置 (Java: REMOVE_COLD_DATA_FLOW_CTR_CONFIG)
	RemoveColdDataFlowCtrGroupConfig = 2002

	// GetColdDataFlowCtrInfo 获取冷数据流控信息 (Java: GET_COLD_DATA_FLOW_CTR_INFO)
	GetColdDataFlowCtrInfo = 2003

	// SetCommitLogReadAheadMode 设置 CommitLog 预读模式 (Java: SET_COMMITLOG_READ_MODE)
	SetCommitLogReadAheadMode = 2004

	// ========== ACL 用户管理相关 (RocketMQ 5.x) ==========

	// CreateUser 创建用户 (Java: AUTH_CREATE_USER)
	CreateUser = 3001

	// UpdateUser 更新用户 (Java: AUTH_UPDATE_USER)
	UpdateUser = 3002

	// DeleteUser 删除用户 (Java: AUTH_DELETE_USER)
	DeleteUser = 3003

	// GetUser 获取用户 (Java: AUTH_GET_USER)
	GetUser = 3004

	// ListUser 列出用户 (Java: AUTH_LIST_USER)
	ListUser = 3005

	// CreateAcl 创建 ACL (Java: AUTH_CREATE_ACL)
	CreateAcl = 3006

	// UpdateAcl 更新 ACL (Java: AUTH_UPDATE_ACL)
	UpdateAcl = 3007

	// DeleteAcl 删除 ACL (Java: AUTH_DELETE_ACL)
	DeleteAcl = 3008

	// GetAcl 获取 ACL (Java: AUTH_GET_ACL)
	GetAcl = 3009

	// ListAcl 列出 ACL (Java: AUTH_LIST_ACL)
	ListAcl = 3010
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
