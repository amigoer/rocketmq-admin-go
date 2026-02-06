# RocketMQ Admin Go

[![Go Reference](https://pkg.go.dev/badge/github.com/codermast/rocketmq-admin-go.svg)](https://pkg.go.dev/github.com/codermast/rocketmq-admin-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/codermast/rocketmq-admin-go)](https://goreportcard.com/report/github.com/codermast/rocketmq-admin-go)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

一个使用 Go 语言实现的 Apache RocketMQ **运维管理**客户端库。

> ⚠️ **注意**：本库**仅提供运维管理接口**，不包含消息生产/消费功能。如需消息功能，请使用官方 [rocketmq-client-go](https://github.com/apache/rocketmq-client-go)。

## 项目背景

Apache RocketMQ 官方 Go 客户端 ([rocketmq-client-go](https://github.com/apache/rocketmq-client-go)) 专注于消息的生产和消费，而运维管理接口（如 Topic 管理、消费者组管理、Broker 监控、ACL 权限管理等）只有 Java 版本的实现（`MQAdminExt`）。

本项目**专注于运维管理场景**：
- 提供完整的 RocketMQ 运维管理 API（对标 Java 版 `MQAdminExt`）
- 支持构建运维监控平台、管理控制台、自动化运维脚本等
- 对接 RocketMQ 原生通信协议，保证兼容性

## 功能特性

### 集群管理
- 查询集群信息和状态
- 获取/更新 NameServer 配置
- 管理 Controller（RocketMQ 5.x）

### Broker 管理
- 查询 Broker 运行时状态
- 获取/更新 Broker 配置
- Broker 容器管理（添加/移除）
- HA 状态监控
- 写权限管理

### Topic 管理
- 创建/更新/删除 Topic
- 查询 Topic 列表和路由信息
- 查询 Topic 统计数据
- 管理静态 Topic
- Topic 权限控制

### 消费者组管理
- 创建/更新/删除订阅组
- 查询消费者连接信息
- 查询消费统计和进度
- 重置消费位点
- 消费者运行时信息

### 生产者管理
- 查询生产者连接信息
- 获取所有生产者信息

### 消息操作
- 消息轨迹查询
- 消息直接消费
- 消息重试（半消息恢复）

### ACL 权限管理
- 用户管理（创建/更新/删除/查询）
- ACL 规则管理（创建/更新/删除/查询）

### 高级功能
- ConsumeQueue 查询
- 过期消息清理
- 冷数据流控
- RocksDB 配置导出
- 定时器引擎切换

## 安装

```bash
go get github.com/codermast/rocketmq-admin-go
```

## 快速开始

```go
package main

import (
    "context"
    "fmt"
    "log"

    admin "github.com/codermast/rocketmq-admin-go"
)

func main() {
    // 创建管理客户端
    client, err := admin.NewAdminClient(
        admin.WithNameServers([]string{"127.0.0.1:9876"}),
    )
    if err != nil {
        log.Fatalf("创建客户端失败: %v", err)
    }
    defer client.Close()

    // 查询集群信息
    clusterInfo, err := client.ExamineBrokerClusterInfo(context.Background())
    if err != nil {
        log.Fatalf("查询集群信息失败: %v", err)
    }
    fmt.Printf("集群信息: %+v\n", clusterInfo)

    // 获取所有 Topic 列表
    topicList, err := client.FetchAllTopicList(context.Background())
    if err != nil {
        log.Fatalf("获取 Topic 列表失败: %v", err)
    }
    fmt.Printf("Topic 数量: %d\n", len(topicList.Topics))
}
```

## 项目结构

```
rocketmq-admin-go/
├── README.md                   # 项目说明文档
├── LICENSE                     # 开源许可证
├── go.mod                      # Go 模块定义
├── go.sum                      # 依赖校验
├── admin.go                    # 主入口，AdminClient 定义
├── options.go                  # 客户端配置选项
├── errors.go                   # 错误定义
│
├── protocol/                   # 通信协议层
│   ├── remoting/               # 远程通信基础设施
│   │   ├── client.go           # TCP 客户端
│   │   ├── codec.go            # 协议编解码
│   │   └── command.go          # 远程命令定义
│   ├── header/                 # 请求/响应头定义
│   └── body/                   # 请求/响应体定义
│
├── admin/                      # 管理接口实现
│   ├── cluster.go              # 集群管理
│   ├── broker.go               # Broker 管理
│   ├── topic.go                # Topic 管理
│   ├── consumer.go             # 消费者管理
│   ├── producer.go             # 生产者管理
│   ├── message.go              # 消息操作
│   ├── acl.go                  # ACL 权限管理
│   └── controller.go           # Controller 管理
│
├── model/                      # 数据模型
│   ├── cluster.go              # 集群相关模型
│   ├── broker.go               # Broker 相关模型
│   ├── topic.go                # Topic 相关模型
│   ├── consumer.go             # 消费者相关模型
│   ├── message.go              # 消息相关模型
│   └── acl.go                  # ACL 相关模型
│
├── internal/                   # 内部工具包
│   ├── utils/                  # 通用工具
│   └── constants/              # 常量定义
│
├── examples/                   # 使用示例
│   ├── cluster/                # 集群管理示例
│   ├── topic/                  # Topic 管理示例
│   └── consumer/               # 消费者管理示例
│
└── docs/                       # 文档
    ├── api.md                  # API 文档
    └── interfaces.md           # 接口对照表
```

## 兼容性

- **RocketMQ 版本**: 4.x / 5.x
- **Go 版本**: 1.21+

## 开发计划

详见 [ROADMAP.md](./docs/ROADMAP.md)

## 贡献指南

欢迎提交 Issue 和 Pull Request！

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m '添加某个特性'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建 Pull Request

## 许可证

本项目采用 Apache 2.0 许可证 - 详见 [LICENSE](LICENSE) 文件

## 相关项目

- [Apache RocketMQ](https://github.com/apache/rocketmq) - RocketMQ 服务端
- [rocketmq-client-go](https://github.com/apache/rocketmq-client-go) - 官方 Go 客户端（生产消费）
- [rocketmq-dashboard](https://github.com/apache/rocketmq-dashboard) - RocketMQ 控制台

## 联系方式

- 作者: CoderMast
- GitHub: [@codermast](https://github.com/codermast)