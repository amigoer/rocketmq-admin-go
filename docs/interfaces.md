<![CDATA[# RocketMQ Admin 接口对照表

本文档列出了需要对接的 RocketMQ 运维管理接口，基于 Java 版本 `MQAdminExt` 接口整理。

## 目录

1. [生命周期管理](#1-生命周期管理)
2. [Broker 管理](#2-broker-管理)
3. [Topic 管理](#3-topic-管理)
4. [消费者组管理](#4-消费者组管理)
5. [生产者管理](#5-生产者管理)
6. [集群管理](#6-集群管理)
7. [消息操作](#7-消息操作)
8. [Offset 管理](#8-offset-管理)
9. [KV 配置管理](#9-kv-配置管理)
10. [ACL 权限管理](#10-acl-权限管理)
11. [Controller 管理](#11-controller-管理)
12. [高级功能](#12-高级功能)

---

## 1. 生命周期管理

| 序号 | Java 方法名  | Go 方法名（规划） | 说明           | 优先级 |
| ---- | ------------ | ----------------- | -------------- | ------ |
| 1    | `start()`    | `Start()`         | 启动管理客户端 | P0     |
| 2    | `shutdown()` | `Close()`         | 关闭管理客户端 | P0     |

---

## 2. Broker 管理

| 序号 | Java 方法名                   | Go 方法名（规划）             | 说明                   | 优先级 |
| ---- | ----------------------------- | ----------------------------- | ---------------------- | ------ |
| 1    | `addBrokerToContainer()`      | `AddBrokerToContainer()`      | 添加 Broker 到容器     | P2     |
| 2    | `removeBrokerFromContainer()` | `RemoveBrokerFromContainer()` | 从容器移除 Broker      | P2     |
| 3    | `updateBrokerConfig()`        | `UpdateBrokerConfig()`        | 更新 Broker 配置       | P1     |
| 4    | `getBrokerConfig()`           | `GetBrokerConfig()`           | 获取 Broker 配置       | P1     |
| 5    | `fetchBrokerRuntimeStats()`   | `FetchBrokerRuntimeStats()`   | 获取 Broker 运行时统计 | P0     |
| 6    | `wipeWritePermOfBroker()`     | `WipeWritePermOfBroker()`     | 清除 Broker 写权限     | P1     |
| 7    | `addWritePermOfBroker()`      | `AddWritePermOfBroker()`      | 添加 Broker 写权限     | P1     |
| 8    | `viewBrokerStatsData()`       | `ViewBrokerStatsData()`       | 查看 Broker 统计数据   | P1     |
| 9    | `getBrokerHAStatus()`         | `GetBrokerHAStatus()`         | 获取 Broker HA 状态    | P1     |
| 10   | `getBrokerEpochCache()`       | `GetBrokerEpochCache()`       | 获取 Broker Epoch 缓存 | P2     |
| 11   | `getBrokerLiteInfo()`         | `GetBrokerLiteInfo()`         | 获取 Broker 简要信息   | P2     |
| 12   | `resetMasterFlushOffset()`    | `ResetMasterFlushOffset()`    | 重置 Master 刷盘偏移   | P2     |

---

## 3. Topic 管理

| 序号 | Java 方法名                        | Go 方法名（规划）                  | 说明                        | 优先级 |
| ---- | ---------------------------------- | ---------------------------------- | --------------------------- | ------ |
| 1    | `createAndUpdateTopicConfig()`     | `CreateAndUpdateTopicConfig()`     | 创建/更新 Topic 配置        | P0     |
| 2    | `createAndUpdateTopicConfigList()` | `CreateAndUpdateTopicConfigList()` | 批量创建/更新 Topic 配置    | P1     |
| 3    | `examineTopicStats()`              | `ExamineTopicStats()`              | 查询 Topic 统计信息         | P0     |
| 4    | `examineTopicStatsConcurrent()`    | `ExamineTopicStatsConcurrent()`    | 并发查询 Topic 统计         | P2     |
| 5    | `fetchAllTopicList()`              | `FetchAllTopicList()`              | 获取所有 Topic 列表         | P0     |
| 6    | `fetchTopicsByCluster()`           | `FetchTopicsByCluster()`           | 按集群获取 Topic 列表       | P0     |
| 7    | `examineTopicRouteInfo()`          | `ExamineTopicRouteInfo()`          | 查询 Topic 路由信息         | P0     |
| 8    | `deleteTopic()`                    | `DeleteTopic()`                    | 删除 Topic                  | P0     |
| 9    | `deleteTopicInBroker()`            | `DeleteTopicInBroker()`            | 在 Broker 中删除 Topic      | P1     |
| 10   | `deleteTopicInBrokerConcurrent()`  | `DeleteTopicInBrokerConcurrent()`  | 并发删除 Topic              | P2     |
| 11   | `deleteTopicInNameServer()`        | `DeleteTopicInNameServer()`        | 从 NameServer 删除 Topic    | P1     |
| 12   | `examineTopicConfig()`             | `ExamineTopicConfig()`             | 查询 Topic 配置             | P1     |
| 13   | `createStaticTopic()`              | `CreateStaticTopic()`              | 创建静态 Topic              | P2     |
| 14   | `queryTopicConsumeByWho()`         | `QueryTopicConsumeByWho()`         | 查询 Topic 被哪些消费者消费 | P1     |
| 15   | `getClusterList()`                 | `GetClusterList()`                 | 获取 Topic 所在集群列表     | P1     |
| 16   | `getTopicClusterList()`            | `GetTopicClusterList()`            | 获取 Topic 集群列表         | P1     |
| 17   | `getAllTopicConfig()`              | `GetAllTopicConfig()`              | 获取所有 Topic 配置         | P1     |
| 18   | `getUserTopicConfig()`             | `GetUserTopicConfig()`             | 获取用户 Topic 配置         | P2     |
| 19   | `getParentTopicInfo()`             | `GetParentTopicInfo()`             | 获取父 Topic 信息           | P2     |
| 20   | `getLiteTopicInfo()`               | `GetLiteTopicInfo()`               | 获取轻量 Topic 信息         | P2     |

---

## 4. 消费者组管理

| 序号 | Java 方法名                                    | Go 方法名（规划）                              | 说明                   | 优先级 |
| ---- | ---------------------------------------------- | ---------------------------------------------- | ---------------------- | ------ |
| 1    | `createAndUpdateSubscriptionGroupConfig()`     | `CreateAndUpdateSubscriptionGroupConfig()`     | 创建/更新订阅组配置    | P0     |
| 2    | `createAndUpdateSubscriptionGroupConfigList()` | `CreateAndUpdateSubscriptionGroupConfigList()` | 批量创建/更新订阅组    | P1     |
| 3    | `examineSubscriptionGroupConfig()`             | `ExamineSubscriptionGroupConfig()`             | 查询订阅组配置         | P0     |
| 4    | `deleteSubscriptionGroup()`                    | `DeleteSubscriptionGroup()`                    | 删除订阅组             | P0     |
| 5    | `examineConsumeStats()`                        | `ExamineConsumeStats()`                        | 查询消费统计           | P0     |
| 6    | `examineConsumeStatsConcurrent()`              | `ExamineConsumeStatsConcurrent()`              | 并发查询消费统计       | P2     |
| 7    | `examineConsumerConnectionInfo()`              | `ExamineConsumerConnectionInfo()`              | 查询消费者连接信息     | P0     |
| 8    | `getConsumerRunningInfo()`                     | `GetConsumerRunningInfo()`                     | 获取消费者运行时信息   | P1     |
| 9    | `queryTopicsByConsumer()`                      | `QueryTopicsByConsumer()`                      | 查询消费者订阅的 Topic | P1     |
| 10   | `queryTopicsByConsumerConcurrent()`            | `QueryTopicsByConsumerConcurrent()`            | 并发查询消费者 Topic   | P2     |
| 11   | `querySubscription()`                          | `QuerySubscription()`                          | 查询订阅信息           | P1     |
| 12   | `queryConsumeTimeSpan()`                       | `QueryConsumeTimeSpan()`                       | 查询消费时间跨度       | P1     |
| 13   | `queryConsumeTimeSpanConcurrent()`             | `QueryConsumeTimeSpanConcurrent()`             | 并发查询消费时间跨度   | P2     |
| 14   | `getConsumeStatus()`                           | `GetConsumeStatus()`                           | 获取消费状态           | P1     |
| 15   | `fetchConsumeStatsInBroker()`                  | `FetchConsumeStatsInBroker()`                  | 获取 Broker 消费统计   | P1     |
| 16   | `getAllSubscriptionGroup()`                    | `GetAllSubscriptionGroup()`                    | 获取所有订阅组         | P1     |
| 17   | `getUserSubscriptionGroup()`                   | `GetUserSubscriptionGroup()`                   | 获取用户订阅组         | P2     |
| 18   | `updateAndGetGroupReadForbidden()`             | `UpdateAndGetGroupReadForbidden()`             | 更新组读取禁止状态     | P2     |
| 19   | `cloneGroupOffset()`                           | `CloneGroupOffset()`                           | 克隆消费组偏移         | P2     |
| 20   | `getLiteGroupInfo()`                           | `GetLiteGroupInfo()`                           | 获取轻量组信息         | P2     |
| 21   | `getLiteClientInfo()`                          | `GetLiteClientInfo()`                          | 获取轻量客户端信息     | P2     |
| 22   | `triggerLiteDispatch()`                        | `TriggerLiteDispatch()`                        | 触发轻量分发           | P2     |

---

## 5. 生产者管理

| 序号 | Java 方法名                       | Go 方法名（规划）                 | 说明               | 优先级 |
| ---- | --------------------------------- | --------------------------------- | ------------------ | ------ |
| 1    | `examineProducerConnectionInfo()` | `ExamineProducerConnectionInfo()` | 查询生产者连接信息 | P1     |
| 2    | `getAllProducerInfo()`            | `GetAllProducerInfo()`            | 获取所有生产者信息 | P1     |

---

## 6. 集群管理

| 序号 | Java 方法名                  | Go 方法名（规划）            | 说明                     | 优先级 |
| ---- | ---------------------------- | ---------------------------- | ------------------------ | ------ |
| 1    | `examineBrokerClusterInfo()` | `ExamineBrokerClusterInfo()` | 查询集群信息             | P0     |
| 2    | `getNameServerAddressList()` | `GetNameServerAddressList()` | 获取 NameServer 地址列表 | P0     |
| 3    | `updateNameServerConfig()`   | `UpdateNameServerConfig()`   | 更新 NameServer 配置     | P1     |
| 4    | `getNameServerConfig()`      | `GetNameServerConfig()`      | 获取 NameServer 配置     | P1     |
| 5    | `getInSyncStateData()`       | `GetInSyncStateData()`       | 获取同步状态数据         | P2     |

---

## 7. 消息操作

| 序号 | Java 方法名                      | Go 方法名（规划）                | 说明             | 优先级 |
| ---- | -------------------------------- | -------------------------------- | ---------------- | ------ |
| 1    | `queryMessage()`                 | `QueryMessage()`                 | 查询消息         | P1     |
| 2    | `messageTrackDetail()`           | `MessageTrackDetail()`           | 消息轨迹详情     | P1     |
| 3    | `messageTrackDetailConcurrent()` | `MessageTrackDetailConcurrent()` | 并发查询消息轨迹 | P2     |
| 4    | `consumeMessageDirectly()`       | `ConsumeMessageDirectly()`       | 直接消费消息     | P2     |
| 5    | `resumeCheckHalfMessage()`       | `ResumeCheckHalfMessage()`       | 恢复检查半消息   | P2     |
| 6    | `setMessageRequestMode()`        | `SetMessageRequestMode()`        | 设置消息请求模式 | P2     |

---

## 8. Offset 管理

| 序号 | Java 方法名                   | Go 方法名（规划）             | 说明                     | 优先级 |
| ---- | ----------------------------- | ----------------------------- | ------------------------ | ------ |
| 1    | `resetOffsetByTimestampOld()` | `ResetOffsetByTimestampOld()` | 按时间戳重置偏移（旧版） | P1     |
| 2    | `resetOffsetByTimestamp()`    | `ResetOffsetByTimestamp()`    | 按时间戳重置偏移         | P0     |
| 3    | `resetOffsetNew()`            | `ResetOffsetNew()`            | 重置偏移（新版）         | P1     |
| 4    | `resetOffsetNewConcurrent()`  | `ResetOffsetNewConcurrent()`  | 并发重置偏移             | P2     |
| 5    | `updateConsumeOffset()`       | `UpdateConsumeOffset()`       | 更新消费偏移             | P1     |
| 6    | `resetOffsetByQueueId()`      | `ResetOffsetByQueueId()`      | 按队列 ID 重置偏移       | P1     |
| 7    | `searchOffset()`              | `SearchOffset()`              | 搜索偏移（已废弃）       | P3     |

---

## 9. KV 配置管理

| 序号 | Java 方法名                 | Go 方法名（规划）           | 说明                   | 优先级 |
| ---- | --------------------------- | --------------------------- | ---------------------- | ------ |
| 1    | `putKVConfig()`             | `PutKVConfig()`             | 存储 KV 配置           | P2     |
| 2    | `getKVConfig()`             | `GetKVConfig()`             | 获取 KV 配置           | P2     |
| 3    | `getKVListByNamespace()`    | `GetKVListByNamespace()`    | 按命名空间获取 KV 列表 | P2     |
| 4    | `createAndUpdateKvConfig()` | `CreateAndUpdateKvConfig()` | 创建/更新 KV 配置      | P2     |
| 5    | `deleteKvConfig()`          | `DeleteKvConfig()`          | 删除 KV 配置           | P2     |
| 6    | `createOrUpdateOrderConf()` | `CreateOrUpdateOrderConf()` | 创建/更新顺序配置      | P2     |

---

## 10. ACL 权限管理

| 序号 | Java 方法名    | Go 方法名（规划） | 说明          | 优先级 |
| ---- | -------------- | ----------------- | ------------- | ------ |
| 1    | `createUser()` | `CreateUser()`    | 创建用户      | P1     |
| 2    | `updateUser()` | `UpdateUser()`    | 更新用户      | P1     |
| 3    | `deleteUser()` | `DeleteUser()`    | 删除用户      | P1     |
| 4    | `getUser()`    | `GetUser()`       | 获取用户信息  | P1     |
| 5    | `listUser()`   | `ListUser()`      | 列出用户      | P1     |
| 6    | `createAcl()`  | `CreateAcl()`     | 创建 ACL 规则 | P1     |
| 7    | `updateAcl()`  | `UpdateAcl()`     | 更新 ACL 规则 | P1     |
| 8    | `deleteAcl()`  | `DeleteAcl()`     | 删除 ACL 规则 | P1     |
| 9    | `getAcl()`     | `GetAcl()`        | 获取 ACL 规则 | P1     |
| 10   | `listAcl()`    | `ListAcl()`       | 列出 ACL 规则 | P1     |

---

## 11. Controller 管理

| 序号 | Java 方法名                   | Go 方法名（规划）             | 说明                        | 优先级 |
| ---- | ----------------------------- | ----------------------------- | --------------------------- | ------ |
| 1    | `getControllerMetaData()`     | `GetControllerMetaData()`     | 获取 Controller 元数据      | P2     |
| 2    | `getControllerConfig()`       | `GetControllerConfig()`       | 获取 Controller 配置        | P2     |
| 3    | `updateControllerConfig()`    | `UpdateControllerConfig()`    | 更新 Controller 配置        | P2     |
| 4    | `electMaster()`               | `ElectMaster()`               | 选举 Master                 | P2     |
| 5    | `cleanControllerBrokerData()` | `CleanControllerBrokerData()` | 清理 Controller Broker 数据 | P2     |

---

## 12. 高级功能

| 序号 | Java 方法名                          | Go 方法名（规划）                    | 说明                    | 优先级 |
| ---- | ------------------------------------ | ------------------------------------ | ----------------------- | ------ |
| 1    | `queryConsumeQueue()`                | `QueryConsumeQueue()`                | 查询消费队列            | P2     |
| 2    | `cleanExpiredConsumerQueue()`        | `CleanExpiredConsumerQueue()`        | 清理过期消费队列        | P2     |
| 3    | `cleanExpiredConsumerQueueByAddr()`  | `CleanExpiredConsumerQueueByAddr()`  | 按地址清理过期队列      | P2     |
| 4    | `deleteExpiredCommitLog()`           | `DeleteExpiredCommitLog()`           | 删除过期 CommitLog      | P2     |
| 5    | `deleteExpiredCommitLogByAddr()`     | `DeleteExpiredCommitLogByAddr()`     | 按地址删除过期日志      | P2     |
| 6    | `cleanUnusedTopic()`                 | `CleanUnusedTopic()`                 | 清理未使用 Topic        | P2     |
| 7    | `cleanUnusedTopicByAddr()`           | `CleanUnusedTopicByAddr()`           | 按地址清理未使用 Topic  | P2     |
| 8    | `updateColdDataFlowCtrGroupConfig()` | `UpdateColdDataFlowCtrGroupConfig()` | 更新冷数据流控配置      | P3     |
| 9    | `removeColdDataFlowCtrGroupConfig()` | `RemoveColdDataFlowCtrGroupConfig()` | 移除冷数据流控配置      | P3     |
| 10   | `getColdDataFlowCtrInfo()`           | `GetColdDataFlowCtrInfo()`           | 获取冷数据流控信息      | P3     |
| 11   | `setCommitLogReadAheadMode()`        | `SetCommitLogReadAheadMode()`        | 设置 CommitLog 预读模式 | P3     |
| 12   | `exportRocksDBConfigToJson()`        | `ExportRocksDBConfigToJson()`        | 导出 RocksDB 配置       | P3     |
| 13   | `checkRocksdbCqWriteProgress()`      | `CheckRocksdbCqWriteProgress()`      | 检查 RocksDB 写入进度   | P3     |
| 14   | `switchTimerEngine()`                | `SwitchTimerEngine()`                | 切换定时器引擎          | P3     |
| 15   | `exportPopRecords()`                 | `ExportPopRecords()`                 | 导出 Pop 记录           | P3     |

---

## 优先级说明

- **P0** - 核心功能，第一阶段必须实现
- **P1** - 常用功能，第二阶段实现
- **P2** - 进阶功能，第三阶段实现
- **P3** - 高级/边缘功能，按需实现

## 接口统计

| 分类            | 接口数量 | P0     | P1     | P2     | P3    |
| --------------- | -------- | ------ | ------ | ------ | ----- |
| 生命周期管理    | 2        | 2      | 0      | 0      | 0     |
| Broker 管理     | 12       | 1      | 5      | 6      | 0     |
| Topic 管理      | 20       | 5      | 9      | 6      | 0     |
| 消费者组管理    | 22       | 5      | 7      | 10     | 0     |
| 生产者管理      | 2        | 0      | 2      | 0      | 0     |
| 集群管理        | 5        | 2      | 2      | 1      | 0     |
| 消息操作        | 6        | 0      | 2      | 4      | 0     |
| Offset 管理     | 7        | 1      | 4      | 1      | 1     |
| KV 配置管理     | 6        | 0      | 0      | 6      | 0     |
| ACL 权限管理    | 10       | 0      | 10     | 0      | 0     |
| Controller 管理 | 5        | 0      | 0      | 5      | 0     |
| 高级功能        | 15       | 0      | 0      | 7      | 8     |
| **总计**        | **112**  | **16** | **41** | **46** | **9** |

---

## 版本更新记录

| 版本 | 日期       | 说明                   |
| ---- | ---------- | ---------------------- |
| v0.1 | 2024-XX-XX | 初始版本，完成 P0 接口 |
| v0.2 | TBD        | 完成 P1 接口           |
| v0.3 | TBD        | 完成 P2 接口           |
| v1.0 | TBD        | 全部接口完成，正式发布 |
]]>
