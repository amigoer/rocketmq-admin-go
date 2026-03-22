<div align="center">
  <img src="docs/logo.png" width="512" alt="RocketMQ Admin Go Logo">
  <h1>🚀 RocketMQ Admin Go</h1>
  <p><strong>专为 Go 语言打造的 Apache RocketMQ 运维管理客户端</strong></p>
  <p>全功能复刻 Java 版 <code>MQAdminExt</code> 能力，与官方 <code>rocketmq-client-go</code> 无缝集成。</p>

  <p>
    <a href="https://pkg.go.dev/github.com/amigoer/rocketmq-admin-go">
      <img src="https://pkg.go.dev/badge/github.com/amigoer/rocketmq-admin-go.svg" alt="Go Reference">
    </a>
    <a href="https://goreportcard.com/report/github.com/amigoer/rocketmq-admin-go">
      <img src="https://goreportcard.com/badge/github.com/amigoer/rocketmq-admin-go" alt="Go Report Card">
    </a>
    <a href="LICENSE">
      <img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License">
    </a>
    <img src="https://img.shields.io/badge/RocketMQ-4.x%20%2F%205.x-brightgreen" alt="RocketMQ Version">
  </p>

  <p>
    官方的 <a href="https://github.com/apache/rocketmq-client-go">rocketmq-client-go</a> 专注于消息的<strong>生产</strong>与<strong>消费</strong>，但在运维管理方面缺乏原生支持。<br>
    本项目作为其<strong>增强包</strong>，提供完整的运维管理能力，并支持<strong>配置共享</strong>。
  </p>

</div>

## ✨ 核心特性

| 模块           | 功能亮点                                                         | 完成度 |
| :------------- | :--------------------------------------------------------------- | :----: |
| **配置共享**   | 与 rocketmq-client-go 无缝集成，**配置一次、两边使用**           |   ✅    |
| **基础运维**   | 集群状态监控、Broker 运行时信息、NameServer 配置管理             |   ✅    |
| **Topic 管理** | 创建/删除 Topic、路由查询、静态 Topic、Topic 权限控制            |   ✅    |
| **消费者管理** | 订阅组管理、消费进度监控、在线客户端查询、**重置消费位点**       |   ✅    |
| **消息操作**   | 消息轨迹查询、**消息直接消费**、死信队列处理、半消息恢复         |   ✅    |
| **权限安全**   | 完整的 ACL 用户管理、白名单/黑名单规则控制                       |   ✅    |
| **高级功能**   | KV 配置、Controller 模式管理 (5.x)、**冷数据流控**、RocksDB 调优 |   ✅    |



## 🛠️ 安装

```bash
go get github.com/amigoer/rocketmq-admin-go@latest
```

> 要求 Go 1.21 或更高版本。



## 🚀 快速开始

### 方式一：统一配置（推荐）

**配置一次，同时使用运维接口和消息收发接口：**

```go
package main

import (
    "context"
    "fmt"
    
    admin "github.com/amigoer/rocketmq-admin-go"
    "github.com/apache/rocketmq-client-go/v2/primitive"
    "github.com/apache/rocketmq-client-go/v2/producer"
)

func main() {
    // ========== 配置只写一次 ==========
    config := admin.NewConfig("localhost:9876").
        WithCredentials("admin", "password")

    // ========== 运维操作 ==========
    adminClient, _ := config.NewAdminClient()
    adminClient.Start()
    defer adminClient.Close()

    // 查询集群信息
    clusterInfo, _ := adminClient.ExamineBrokerClusterInfo(context.Background())
    fmt.Printf("集群: %+v\n", clusterInfo)

    // ========== 生产消息 ==========
    p, _ := config.NewProducer(producer.WithRetry(2))
    p.Start()
    defer p.Shutdown()

    res, _ := p.SendSync(context.Background(), &primitive.Message{
        Topic: "test-topic",
        Body:  []byte("Hello RocketMQ!"),
    })
    fmt.Printf("发送成功: %s\n", res.MsgID)
}
```

### 方式二：仅使用运维接口

如果不需要消息收发，可以直接创建 Admin 客户端：

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    admin "github.com/amigoer/rocketmq-admin-go"
)

func main() {
    // 创建 Admin 客户端
    client, err := admin.NewClient(
        admin.WithNameServers([]string{"127.0.0.1:9876"}),
        admin.WithTimeout(5 * time.Second),
    )
    if err != nil {
        log.Fatalf("初始化失败: %v", err)
    }
    defer client.Close()

    if err := client.Start(); err != nil {
        log.Fatalf("启动失败: %v", err)
    }

    // 查询集群信息
    clusterInfo, err := client.ExamineBrokerClusterInfo(context.Background())
    if err != nil {
        log.Fatalf("查询异常: %v", err)
    }

    fmt.Println("🚀 RocketMQ 集群概览:")
    for clusterName, brokerNames := range clusterInfo.ClusterAddrTable {
        fmt.Printf("Cluster: %s\n", clusterName)
        for _, brokerName := range brokerNames {
            brokerData := clusterInfo.BrokerAddrTable[brokerName]
            fmt.Printf("  └─ Broker: %s (Master: %s)\n", brokerName, brokerData.BrokerAddrs["0"])
        }
    }
}
```

更多示例请参考 [examples](./examples) 目录。



## 🔌 与 rocketmq-client-go 集成

本项目设计为 `rocketmq-client-go` 的**增强包**，通过统一配置工厂实现配置共享：

```go
// 统一配置
config := admin.NewConfig("localhost:9876").
    WithCredentials("accessKey", "secretKey").
    WithTimeout(10 * time.Second)

// 创建 Admin 运维客户端
adminClient, _ := config.NewAdminClient()

// 创建 Producer
producer, _ := config.NewProducer()

// 创建 Push Consumer
pushConsumer, _ := config.NewPushConsumer(consumer.WithGroupName("my-group"))

// 创建 Pull Consumer
pullConsumer, _ := config.NewPullConsumer()
```

**工厂方法：**

| 方法                       | 返回类型                | 说明            |
| -------------------------- | ----------------------- | --------------- |
| `NewAdminClient()`         | `*admin.Client`         | 运维管理客户端  |
| `NewProducer(opts...)`     | `rocketmq.Producer`     | 消息生产者      |
| `NewPushConsumer(opts...)` | `rocketmq.PushConsumer` | Push 模式消费者 |
| `NewPullConsumer(opts...)` | `rocketmq.PullConsumer` | Pull 模式消费者 |



## 🏗️ 架构概览

```mermaid
graph TD
    User["用户应用 / 运维平台"] -->|"API 调用"| Config["统一配置 Config"]
    
    Config --> AdminClient["Admin 客户端"]
    Config --> Producer["Producer (client-go)"]
    Config --> Consumer["Consumer (client-go)"]
    
    subgraph SDK ["RocketMQ Admin Go SDK"]
        AdminClient
        Remoting["通信协议层"]
        Codec["序列化/反序列化"]
    end

    AdminClient --> Remoting
    Remoting --> Codec
    
    subgraph Cluster ["RocketMQ Cluster"]
        NS["NameServer"]
        BrokerMaster["Broker Master"]
        BrokerSlave["Broker Slave"]
        Controller["Controller (5.x)"]
    end

    Remoting -->|"TCP 长连接"| NS
    Remoting -->|"TCP 长连接"| BrokerMaster
    Producer -->|"消息发送"| BrokerMaster
    Consumer -->|"消息消费"| BrokerMaster
```



## 📚 技术文档

- [接口对照表](./docs/interfaces.md): 详细列出了所有支持的 Admin 接口及其实现状态。
- [协议实现解析](./docs/rocketmq_protocol.md): 深入解析 RocketMQ Remoting 协议的纯 Go 实现原理。



## 🤝 贡献与支持

欢迎提交 [Issue](https://github.com/amigoer/rocketmq-admin-go/issues) 或 [Pull Request](https://github.com/amigoer/rocketmq-admin-go/pulls) 改进本项目。

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 提交 Pull Request



## 📄 许可证

本项目采用 [Apache-2.0](./LICENSE) 许可证。

Copyright (c) 2026 Amigoer
