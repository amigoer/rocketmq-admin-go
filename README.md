<div align="center">
  <img src="docs/logo.png" width="128" height="128" alt="RocketMQ Admin Go Logo">
  <h1>🚀 RocketMQ Admin Go</h1>
  <p><strong>专为 Go 语言打造的 Apache RocketMQ 运维管理客户端</strong></p>
  <p>全功能复刻 Java 版 <code>MQAdminExt</code> 能力，轻量、高效、零依赖。</p>

  <p>
    <a href="https://pkg.go.dev/github.com/codermast/rocketmq-admin-go">
      <img src="https://pkg.go.dev/badge/github.com/codermast/rocketmq-admin-go.svg" alt="Go Reference">
    </a>
    <a href="https://goreportcard.com/report/github.com/codermast/rocketmq-admin-go">
      <img src="https://goreportcard.com/badge/github.com/codermast/rocketmq-admin-go" alt="Go Report Card">
    </a>
    <a href="LICENSE">
      <img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License">
    </a>
    <img src="https://img.shields.io/badge/RocketMQ-4.x%20%2F%205.x-brightgreen" alt="RocketMQ Version">
  </p>
</div>

---

## 📖 简介

**RocketMQ Admin Go** 是一个纯 Go 语言实现的 RocketMQ 运维管理 SDK。

官方的 [rocketmq-client-go](https://github.com/apache/rocketmq-client-go) 专注于消息的**生产**与**消费**，但在运维管理（创建 Topic、查询集群状态、重置消费位点等）方面缺乏原生支持。由此，本项目应运而生。

✅ **100% 接口覆盖**：完整实现 Java 版 `MQAdminExt` 的所有运维接口（P0-P3）。

✅ **原生协议支持**：直接基于 RocketMQ Remoting 协议，无中间层，不仅兼容性好，而且性能极佳。

✅ **开箱即用**：提供清晰的 API 和丰富的示例，几行代码即可管理百万级消息集群。

---

## ✨ 核心特性

| 模块           | 功能亮点                                                         | 完成度 |
| :------------- | :--------------------------------------------------------------- | :----: |
| **基础运维**   | 集群状态监控、Broker 运行时信息、NameServer 配置管理             |   ✅    |
| **Topic 管理** | 创建/删除 Topic、路由查询、静态 Topic、Topic 权限控制            |   ✅    |
| **消费者管理** | 订阅组管理、消费进度监控、在线客户端查询、**重置消费位点**       |   ✅    |
| **消息操作**   | 消息轨迹查询、**消息直接消费**、死信队列处理、半消息恢复         |   ✅    |
| **权限安全**   | 完整的 ACL 用户管理、白名单/黑名单规则控制                       |   ✅    |
| **高级功能**   | KV 配置、Controller 模式管理 (5.x)、**冷数据流控**、RocksDB 调优 |   ✅    |

---

## 🛠️ 安装

```bash
go get github.com/codermast/rocketmq-admin-go
```

> 要求 Go 1.21 或更高版本。

---

## 🚀 快速开始

以下示例展示如何连接到 RocketMQ 集群并查询集群信息：

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    admin "github.com/codermast/rocketmq-admin-go"
)

func main() {
    // 1. 创建 Admin 客户端
    client, err := admin.NewClient(
        admin.WithNameServers([]string{"127.0.0.1:9876"}), // NameServer 地址
        admin.WithTimeout(5 * time.Second),                 // 请求超时时间
    )
    if err != nil {
        log.Fatalf("初始化失败: %v", err)
    }
    defer client.Close()

    // 2. 启动客户端
    if err := client.Start(); err != nil {
        log.Fatalf("启动失败: %v", err)
    }

    // 3. 执行运维操作：查询集群信息
    clusterInfo, err := client.ExamineBrokerClusterInfo(context.Background())
    if err != nil {
        log.Fatalf("查询异常: %v", err)
    }

    // 4. 输出结果
    fmt.Println("🚀 RocketMQ 集群概览:")
    for clusterName, brokerNames := range clusterInfo.ClusterAddrTable {
        fmt.Printf("Cluster: %s\n", clusterName)
        for _, brokerName := range brokerNames {
            brokerData := clusterInfo.BrokerAddrTable[brokerName]
            fmt.Printf("  └─ Broker: %s (Master: %s)\n", brokerName, brokerData.BrokerAddrs[0])
        }
    }
}
```

更多示例请参考 [examples](./examples) 目录。

---

## 🏗️ 架构概览

```mermaid
graph TD
    User["用户应用 / 运维平台"] -->|"API 调用"| AdminClient
    
    subgraph SDK ["RocketMQ Admin Go SDK"]
        AdminClient["Admin 客户端"]
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
    Remoting -->|"TCP 长连接"| Controller
```

---

## 🧩 接口能力矩阵

本项目完整实现了 Java 版 `MQAdminExt` 的 112 个原生接口，覆盖 RocketMQ 所有运维场景：

| 功能模块           | 核心能力说明                              | 接口数量 |   状态   |
| :----------------- | :---------------------------------------- | :------: | :------: |
| **Topic 管理**     | 增删改查、路由查询、配置管理、静态 Topic  |    20    |    ✅     |
| **消费者管理**     | 订阅组管理、消费进度监控、连接查询        |    22    |    ✅     |
| **Broker 管理**    | 运行时状态、配置动态更新、写权限控制      |    12    |    ✅     |
| **消息与 Offset**  | 消息轨迹、位点重置、死信处理、直接消费    |    13    |    ✅     |
| **集群与基础**     | 集群拓扑、NameServer 配置、生命周期管理   |    7     |    ✅     |
| **权限安全 (ACL)** | 用户管理、白名单控制、权限规则配置        |    10    |    ✅     |
| **高级运维**       | KV配置、Controller模式、流控、RocksDB调优 |    26    |    ✅     |
| **生产者管理**     | 生产者连接信息查询                        |    2     |    ✅     |
| **总计**           | **全功能集合**                            | **112**  | **100%** |

> 详细接口列表请参考 [docs/interfaces.md](./docs/interfaces.md)

---

## 🤝 贡献与支持

欢迎提交 [Issue](https://github.com/codermast/rocketmq-admin-go/issues) 或 [Pull Request](https://github.com/codermast/rocketmq-admin-go/pulls) 改进本项目。

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 提交 Pull Request

---

## 📄 许可证

本项目采用 [Apache-2.0](./LICENSE) 许可证。

Copyright (c) 2026 CoderMast