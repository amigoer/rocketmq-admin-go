# Mapping Java `MQAdminExt` onto this API

`admin.Client` exposes **106 methods**, covering the admin surface of Java's
`MQAdminExt`. Almost all of them map mechanically, so this page lists only what
does not.

The authoritative, always-current method list is on
[pkg.go.dev](https://pkg.go.dev/github.com/amigoer/rocketmq-admin-go#Client).

## The rule

Java `camelCase()` becomes Go `PascalCase()`:

| Java                        | Go                          |
| --------------------------- | --------------------------- |
| `getBrokerConfig()`         | `GetBrokerConfig()`         |
| `examineTopicRouteInfo()`   | `ExamineTopicRouteInfo()`   |
| `wipeWritePermOfBroker()`   | `WipeWritePermOfBroker()`   |

**92 of the 106** follow this exactly. One wrinkle: Java writes `Kv`, Go writes
`KV` — `createAndUpdateKvConfig()` becomes `CreateAndUpdateKVConfig()`.

## Renamed

| Java                                        | Go                          | Why                                     |
| ------------------------------------------- | --------------------------- | --------------------------------------- |
| `shutdown()`                                | `Close()`                   | Go convention (`io.Closer`)             |
| `createAndUpdateTopicConfig()`              | `CreateTopic()`             | shorter, and it also updates            |
| `createAndUpdateSubscriptionGroupConfig()`  | `CreateSubscriptionGroup()` | shorter, and it also updates            |
| `examineConsumeStats(group, topic)`         | `ExamineConsumeStatsByTopic()` | Go has no method overloading         |

## Legacy 4.x ACL

RocketMQ 4.x configures permissions through `plain_acl.yml` rather than the 5.x
RBAC model, so these have no `MQAdminExt` counterpart in the 5.x sense. They are
identified by request code instead:

| Go                               | Request code                          |
| -------------------------------- | ------------------------------------- |
| `UpdatePlainAccessConfig()`      | 50 `UPDATE_AND_CREATE_ACL_CONFIG`     |
| `DeletePlainAccessConfig()`      | 51 `DELETE_ACL_CONFIG`                |
| `GetBrokerClusterAclInfo()`      | 52 `GET_BROKER_CLUSTER_ACL_INFO`      |
| `UpdateGlobalWhiteAddrsConfig()` | 53 `UPDATE_GLOBAL_WHITE_ADDRS_CONFIG` |

The 5.x user and ACL calls (`CreateUser`, `CreateAcl`, …) map by the rule above.

## Go-only

These have no direct `MQAdminExt` counterpart:

| Go                                            | What it is                                             |
| --------------------------------------------- | ------------------------------------------------------ |
| `IsStarted()` / `IsClosed()`                  | lifecycle predicates                                   |
| `PullMessage()`                               | raw pull from one queue at one offset                  |
| `QueryMessageByTime()`                        | time-range browse, built on `SearchOffset` + `PullMessage` |
| `SetCommitLogReadAheadModeInCluster()`        | applies the per-Broker call across a whole cluster      |
| `UpdateColdDataFlowCtrGroupConfigInCluster()` | applies the per-Broker call across a whole cluster      |

## A note on the `*Concurrent` methods

`ExamineConsumeStatsConcurrent`, `QueryConsumeTimeSpanConcurrent`,
`QueryTopicsByConsumerConcurrent` and `ExamineTopicStatsConcurrent` mirror names
from the Java side, but none of them fans out — each delegates to its
non-concurrent counterpart. Their doc comments say so.

`ExamineConsumeStatsConcurrent` is the one that carries an extra argument: it
routes to `ExamineConsumeStatsByTopic` when `topic` is non-empty, and to
`ExamineConsumeStats` otherwise.
