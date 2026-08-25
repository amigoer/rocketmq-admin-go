package remoting

// Request codes, mirroring the Java RequestCode.java on master and 4.9.8.
const (
	// Message basics

	PullMessage             = 11
	QueryMessage            = 12
	UpdateConsumerOffset    = 15 // Java: UPDATE_CONSUMER_OFFSET
	UpdateAndCreateTopic    = 17
	GetAllTopicConfig       = 21
	UpdateBrokerConfig      = 25
	GetBrokerConfig         = 26
	GetBrokerRuntimeInfo    = 28
	SearchOffsetByTimestamp = 29
	GetMaxOffset            = 30
	GetMinOffset            = 31
	ViewMessageById         = 33

	// Legacy ACL (RocketMQ 4.x plain_acl.yml)

	UpdateAndCreateAclConfig     = 50 // Java: UPDATE_AND_CREATE_ACL_CONFIG
	DeleteAclConfig              = 51 // Java: DELETE_ACL_CONFIG
	GetBrokerClusterAclInfo      = 52 // Java: GET_BROKER_CLUSTER_ACL_INFO
	UpdateGlobalWhiteAddrsConfig = 53 // Java: UPDATE_GLOBAL_WHITE_ADDRS_CONFIG

	// KV configuration

	PutKVConfig    = 100
	GetKVConfig    = 101
	DeleteKVConfig = 102

	// NameServer

	GetRouteInfoByTopic  = 105
	GetBrokerClusterInfo = 106

	// Consumer, subscription group and topic management

	UpdateAndCreateSubscriptionGroup = 200
	GetAllSubscriptionGroupConfig    = 201
	GetTopicStatsInfo                = 202
	GetConsumerConnectionList        = 203
	GetProducerConnectionList        = 204
	WipeWritePermOfBroker            = 205 // Java: WIPE_WRITE_PERM_OF_BROKER
	GetAllTopicListFromNamesrv       = 206
	DeleteSubscriptionGroup          = 207
	GetConsumeStats                  = 208
	DeleteTopicInBroker              = 215
	DeleteTopicInNamesrv             = 216
	GetKVListByNamespace             = 219

	// ResetConsumerOffset drives the admin -> broker -> consumer reset. Java
	// names 220 RESET_CONSUMER_CLIENT_OFFSET, which is only the broker ->
	// consumer leg; admin tools need 222, kept under this name for compatibility.
	ResetConsumerOffset = 222

	GetConsumerStatusFromClient     = 221 // broker -> consumer direction
	InvokeBrokerToGetConsumerStatus = 223 // Java: INVOKE_BROKER_TO_GET_CONSUMER_STATUS
	GetTopicsByCluster              = 224

	// 300 range

	QueryTopicConsumeByWho   = 300
	QueryConsumeTimeSpan     = 303 // Java: QUERY_CONSUME_TIME_SPAN
	CleanExpiredConsumeQueue = 306 // Java: CLEAN_EXPIRED_CONSUMEQUEUE
	GetConsumerRunningInfo   = 307
	ConsumeMessageDirectly   = 309
	CloneGroupOffset         = 314 // Java: CLONE_GROUP_OFFSET
	ViewBrokerStatsData      = 315 // Java: VIEW_BROKER_STATS_DATA
	CleanUnusedTopic         = 316 // Java: CLEAN_UNUSED_TOPIC
	GetBrokerConsumeStats    = 317 // Java: GET_BROKER_CONSUME_STATS
	UpdateNamesrvConfig      = 318
	GetNamesrvConfig         = 319
	QueryConsumeQueue        = 321 // Java: QUERY_CONSUME_QUEUE
	ResumeCheckHalfMessage   = 323 // Java: RESUME_CHECK_HALF_MESSAGE
	AddWritePermOfBroker     = 327 // Java: ADD_WRITE_PERM_OF_BROKER
	GetProducerInfo          = 328 // Java: GET_ALL_PRODUCER_INFO
	DeleteExpiredCommitLog   = 329 // Java: DELETE_EXPIRED_COMMITLOG

	// 340 range

	QueryTopicsByConsumer       = 343
	QuerySubscription           = 345 // Java: QUERY_SUBSCRIPTION_BY_CONSUMER
	GetTopicConfig              = 351 // Java: GET_TOPIC_CONFIG
	GetSubscriptionGroupConfig  = 352 // Java: GET_SUBSCRIPTIONGROUP_CONFIG
	UpdateAndGetGroupForbidden  = 353 // Java: UPDATE_AND_GET_GROUP_FORBIDDEN
	CheckRocksdbCqWriteProgress = 354 // Java: CHECK_ROCKSDB_CQ_WRITE_PROGRESS
	ExportRocksDBConfigToJson   = 355 // Java: EXPORT_ROCKSDB_CONFIG_TO_JSON

	// Message request mode

	SetMessageRequestMode = 401 // Java: SET_MESSAGE_REQUEST_MODE

	// Static topics

	CreateStaticTopic = 513 // Java: UPDATE_AND_CREATE_STATIC_TOPIC

	// Broker container and HA (RocketMQ 5.x)

	AddBrokerToContainer      = 902 // Java: ADD_BROKER
	RemoveBrokerFromContainer = 903 // Java: REMOVE_BROKER
	GetBrokerHAStatus         = 907 // Java: GET_BROKER_HA_STATUS
	ResetMasterFlushOffset    = 908 // Java: RESET_MASTER_FLUSH_OFFSET

	// Controller (RocketMQ 5.x)

	ControllerElectMaster     = 1002 // Java: CONTROLLER_ELECT_MASTER
	ControllerGetMetadataInfo = 1005 // Java: CONTROLLER_GET_METADATA_INFO
	GetInSyncStateData        = 1006 // Java: CONTROLLER_GET_SYNC_STATE_DATA
	GetBrokerEpochCache       = 1007 // Java: GET_BROKER_EPOCH_CACHE
	ControllerUpdateConfig    = 1009 // Java: UPDATE_CONTROLLER_CONFIG
	ControllerGetConfig       = 1010 // Java: GET_CONTROLLER_CONFIG
	CleanControllerBrokerData = 1011 // Java: CLEAN_BROKER_DATA

	// Cold data flow control

	UpdateColdDataFlowCtrGroupConfig = 2001 // Java: UPDATE_COLD_DATA_FLOW_CTR_CONFIG
	RemoveColdDataFlowCtrGroupConfig = 2002 // Java: REMOVE_COLD_DATA_FLOW_CTR_CONFIG
	GetColdDataFlowCtrInfo           = 2003 // Java: GET_COLD_DATA_FLOW_CTR_INFO
	SetCommitLogReadAheadMode        = 2004 // Java: SET_COMMITLOG_READ_MODE

	// ACL and user management (RocketMQ 5.x)

	CreateUser = 3001 // Java: AUTH_CREATE_USER
	UpdateUser = 3002 // Java: AUTH_UPDATE_USER
	DeleteUser = 3003 // Java: AUTH_DELETE_USER
	GetUser    = 3004 // Java: AUTH_GET_USER
	ListUser   = 3005 // Java: AUTH_LIST_USER
	CreateAcl  = 3006 // Java: AUTH_CREATE_ACL
	UpdateAcl  = 3007 // Java: AUTH_UPDATE_ACL
	DeleteAcl  = 3008 // Java: AUTH_DELETE_ACL
	GetAcl     = 3009 // Java: AUTH_GET_ACL
	ListAcl    = 3010 // Java: AUTH_LIST_ACL
)

// Response codes.
const (
	Success                 = 0
	SystemError             = 1
	SystemBusy              = 2
	RequestCodeNotSupported = 3
	TopicNotExist           = 17
	SubscriptionNotExist    = 21
	ConsumerNotOnline       = 206
)
