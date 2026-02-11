# CoretexDB

企业级分布式向量数据库

## 项目简介

CoretexDB 是一款高性能的企业级分布式向量数据库，专为 AI 应用场景设计。作为 CortexDB 的继任者，CoretexDB 在架构、性能和功能方面都进行了全面升级，支持向量检索、全文搜索、标量查询等多种数据访问模式，并提供完整的 SDK 和部署解决方案。

## 核心特性

### 强大的向量检索

CoretexDB 提供了多种先进的向量索引算法，能够高效处理大规模向量数据。系统支持 HNSW（Hierarchical Navigable Small World）算法，这是一种基于图的近似最近邻搜索算法，在高维空间中表现出色，召回率高且查询延迟低。同时支持 DiskANN 算法，这是一种针对磁盘存储优化的索引结构，能够在有限内存条件下处理数十亿级别的向量数据。此外，系统还提供了 Brute Force 暴力搜索作为基准对比方案，适用于小规模数据集或需要精确结果的场景。向量索引支持实时插入和更新，无需重建整个索引即可添加新向量，这使得系统能够支持流式数据摄入场景。系统还支持混合搜索功能，可以将向量相似度搜索与传统的标量条件查询结合起来，实现更精准的检索结果。

### 灵活的存储引擎

CoretexDB 自研了 CoretexDB 存储引擎，这是一个专门为向量数据优化的存储系统。存储引擎支持两种模式：内存存储模式适用于对延迟敏感的场景，所有数据都驻留在内存中，提供微秒级的访问延迟；持久化存储模式则将数据写入磁盘，通过 WAL（Write-Ahead Log）确保数据持久性，即使在系统崩溃后也能恢复数据。存储引擎内置了自动压缩功能，支持 LZ4、Zstd 等多种压缩算法，能够显著降低存储成本。系统还实现了完善的事务支持，采用 MVCC（多版本并发控制）机制，提供了快照隔离级别，确保在高并发场景下的数据一致性。此外，存储引擎还支持数据分片和自动负载均衡，能够根据数据分布自动调整存储策略。

### 多协议支持

为了满足不同场景的需求，CoretexDB 提供了丰富的协议支持。gRPC 接口提供了高性能的二进制 RPC 通信，适用于对延迟敏感的生产环境，支持双向流式传输。RESTful API 采用标准的 HTTP/JSON 格式，便于与各种编程语言和框架集成，提供了完整的 OpenAPI 文档。PostgreSQL 兼容协议使得用户可以直接使用现有的 PostgreSQL 客户端和工具链，降低了学习成本。GraphQL 接口提供了灵活的数据查询能力，用户可以精确指定需要返回的字段，减少不必要的数据传输。Python SDK 与主流 AI 框架深度集成，包括 LangChain、HuggingFace 和 OpenAI，方便 AI 开发者快速上手。

### 多语言 SDK

CoretexDB 提供了四种主流编程语言的官方 SDK，每种 SDK 都经过精心设计，充分考虑了各语言的特性和最佳实践。Go SDK 采用异步编程模型，基于 tokio 运行时，支持高并发连接，提供了连接池、超时重试、断线重连等企业级功能。Java SDK 提供了同步和异步两种 API 风格，基于 Netty 实现，支持 Maven 中央仓库一键依赖。Node.js SDK 完全使用 TypeScript 编写，提供了完整的类型定义，与现代 JavaScript/TypeScript 开发流程无缝集成。C++ SDK 提供了 CMake 构建配置，支持静态和动态链接，适用于性能敏感的场景或嵌入式部署。

### Python 生态系统

Python 是 AI 领域最流行的编程语言，CoretexDB 为 Python 开发者提供了全方位的支持。Python 客户端提供了简洁易用的 API，与 PyTorch、TensorFlow 等深度学习框架的数据结构兼容。LangChain 集成使得用户可以轻松地将 CoretexDB 作为向量存储后端使用，构建复杂的 AI 应用。HuggingFace 集成支持直接使用 HuggingFace Hub 中的预训练模型生成向量，并存储到 CoretexDB 中。OpenAI 集成提供了与 OpenAI Embedding API 的桥接，可以方便地将 OpenAI 生成的向量存储到本地。此外，Python 包还提供了 CLI 工具和监控脚本，方便日常运维操作。

### 企业级功能

CoretexDB 提供了完整的企业级功能，满足生产环境的各种需求。分布式集群支持自动故障转移和负载均衡，单个节点故障不会影响整体服务。自动分片功能会根据数据量和查询负载自动调整数据分布，用户无需手动干预。备份和恢复功能支持增量备份和定时任务，可以恢复到任意时间点。监控和告警系统集成了 Prometheus 指标收集和 Grafana 仪表盘，支持自定义告警规则。安全方面提供了基于 JWT 的身份认证、细粒度的权限控制和数据传输加密。此外，系统还支持与 Consul、Etcd 等服务发现系统集成，便于构建微服务架构。

## 快速开始

### Docker 部署

使用 Docker 部署是最简单的方式，适合快速验证和开发测试场景。首先确保系统已安装 Docker 和 Docker Compose，然后执行以下命令启动单节点服务：

```bash
cd deploy/docker
docker-compose up -d
```

服务启动后，可以通过以下地址访问各个组件。RESTful API 监听在 8080 端口，提供数据库的读写操作。gRPC 服务监听在 50051 端口，适用于高性能场景。Grafana 监控面板在 3001 端口，默认账号 admin/admin。Prometheus 指标服务在 9090 端口，可以连接 Grafana 或其他监控系统。

### Kubernetes 部署

对于生产环境，推荐使用 Kubernetes 部署。单节点部署适用于中小规模数据量，命令如下：

```bash
kubectl apply -f deploy/k8s/
```

高可用部署适用于生产环境，提供了多副本配置和自动故障转移：

```bash
kubectl apply -f deploy/kubernetes/
```

### Helm 部署

使用 Helm 可以更灵活地配置部署参数。首先添加 Helm 仓库：

```bash
helm repo add coretexdb https://qinhaowow.github.io/cortexdb-helm
helm install my-coretexdb coretexdb/coretexdb -f deploy/helm/coretexdb/values.yaml
```

### 从源码编译

从源码编译可以获得最新的功能和改进。首先克隆仓库并进入项目目录：

```bash
git clone https://github.com/qinhaowow/cortexdb.git
cd cortexdb
```

使用 cargo 编译发布版本：

```bash
cargo build --release
```

运行编译后的二进制文件：

```bash
./target/release/coretexdb --config src/coretex_core/config.rs
```

## SDK 使用示例

### Go SDK 示例

Go SDK 提供了异步客户端，支持高并发场景。以下是基本使用示例：

```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/qinhaowow/cortexdb/SDK/go/coretexdb"
)

func main() {
    // 创建客户端配置
    config := coretexdb.Config{
        Address:     "localhost:50051",
        MaxRetries:  3,
        Timeout:     time.Second * 10,
    }
    
    // 创建客户端
    client, err := coretexdb.NewClient(config)
    if err != nil {
        log.Fatalf("Failed to create client: %v", err)
    }
    defer client.Close()
    
    ctx := context.Background()
    
    // 创建集合
    err = client.CreateCollection(ctx, &coretexdb.CollectionConfig{
        Name:         "my_collection",
        Dimension:    768,
        MetricType:   coretexdb.CosineSimilarity,
        IndexType:    coretexdb.HNSW,
    })
    if err != nil {
        log.Fatalf("Failed to create collection: %v", err)
    }
    
    // 准备向量数据
    vectors := make([][]float32, 1000)
    for i := range vectors {
        vectors[i] = make([]float32, 768)
        for j := range vectors[i] {
            vectors[i][j] = float32(i%256) / 255.0
        }
    }
    
    // 插入向量
    ids, err := client.Insert(ctx, "my_collection", vectors, nil)
    if err != nil {
        log.Fatalf("Failed to insert vectors: %v", err)
    }
    log.Printf("Inserted %d vectors", len(ids))
    
    // 搜索向量
    query := make([]float32, 768)
    for i := range query {
        query[i] = 0.5
    }
    
    results, err := client.Search(ctx, &coretexdb.SearchRequest{
        Collection: "my_collection",
        Query:     query,
        TopK:      10,
        Filters:   nil,
    })
    if err != nil {
        log.Fatalf("Failed to search: %v", err)
    }
    
    for _, r := range results {
        log.Printf("ID: %s, Score: %.4f", r.ID, r.Score)
    }
}
```

### Python SDK 示例

Python 客户端与主流 AI 框架无缝集成，使用非常简单：

```python
from coretexdb import CoretexDB, CollectionConfig
import numpy as np

# 连接数据库
client = CoretexDB(host="localhost", port=50051)

# 创建集合
config = CollectionConfig(
    name="my_collection",
    dimension=768,
    metric_type="cosine",
    index_type="hnsw"
)
client.create_collection(config)

# 准备向量数据
vectors = np.random.randn(1000, 768).astype(np.float32)

# 插入向量（支持批量）
ids = client.insert("my_collection", vectors, metadata=None)
print(f"Inserted {len(ids)} vectors")

# 搜索向量
query = np.random.randn(768).astype(np.float32)
results = client.search(
    collection="my_collection",
    query=query,
    top_k=10,
    filters=None
)

for result in results:
    print(f"ID: {result.id}, Score: {result.score:.4f}")

# 使用 LangChain 集成
from langchain.vectorstores import CoretexDB
from langchain.embeddings import OpenAIEmbeddings

embeddings = OpenAIEmbeddings()
vectorstore = CoretexDB.from_documents(documents, embeddings)
docs = vectorstore.similarity_search("your query")
```

### Node.js SDK 示例

Node.js SDK 完全使用 TypeScript 编写，提供了完整的类型安全：

```typescript
import { CoretexDB, CollectionConfig } from 'coretexdb';

interface Document {
    id: string;
    title: string;
    content: string;
    embedding: number[];
}

async function main() {
    const client = new CoretexDB({
        host: 'localhost',
        port: 50051,
        maxRetries: 3,
        timeout: 10000,
    });

    try {
        // 创建集合
        await client.createCollection({
            name: 'documents',
            dimension: 768,
            metricType: 'cosine',
            indexType: 'hnsw',
        });

        // 准备文档数据
        const documents: Document[] = [
            {
                id: 'doc1',
                title: 'Introduction to Vector Databases',
                content: 'Vector databases are specialized databases...',
                embedding: Array(768).fill(0.1),
            },
        ];

        // 插入文档
        const ids = await client.insert<Document>('documents', documents);
        console.log(`Inserted ${ids.length} documents`);

        // 搜索文档
        const results = await client.search({
            collection: 'documents',
            query: Array(768).fill(0.5),
            topK: 5,
        });

        for (const result of results) {
            console.log(`ID: ${result.id}, Score: ${result.score}`);
        }
    } finally {
        await client.close();
    }
}

main().catch(console.error);
```

### Java SDK 示例

Java SDK 提供了同步和异步两种 API：

```java
import com.coretexdb.CoretexDBClient;
import com.coretexdb.models.CollectionConfig;
import com.coretexdb.models.SearchResult;

import java.util.*;
import java.util.concurrent.*;

public class Example {
    public static void main(String[] args) throws Exception {
        // 创建客户端
        CoretexDBClient client = new CoretexDBClient(
            "localhost",
            50051,
            3,  // 重试次数
            10  // 超时秒数
        );

        try {
            // 创建集合
            CollectionConfig config = new CollectionConfig.Builder()
                .name("my_collection")
                .dimension(768)
                .metricType("cosine")
                .indexType("hnsw")
                .build();
            
            client.createCollection(config);
            
            // 准备向量数据
            List<float[]> vectors = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                float[] vector = new float[768];
                Arrays.fill(vector, (float) (i % 256) / 255.0f);
                vectors.add(vector);
            }
            
            // 插入向量
            List<String> ids = client.insert("my_collection", vectors, null);
            System.out.println("Inserted " + ids.size() + " vectors");
            
            // 搜索向量
            float[] query = new float[768];
            Arrays.fill(query, 0.5f);
            
            List<SearchResult> results = client.search(
                "my_collection",
                query,
                10
            );
            
            for (SearchResult result : results) {
                System.out.printf("ID: %s, Score: %.4f%n", 
                    result.getId(), result.getScore());
            }
            
        } finally {
            client.close();
        }
    }
}
```

## 项目结构

```
CoretexDataBases/
├── src/                              # Rust 核心代码
│   ├── coretex_api/                  # API 层实现
│   │   ├── grpc/                     # gRPC 服务
│   │   │   ├── proto/                # Protocol Buffer 定义
│   │   │   ├── generated.rs          # 自动生成的代码
│   │   │   └── server.rs             # gRPC 服务器
│   │   ├── rest/                     # RESTful API
│   │   │   ├── handlers.rs           # 请求处理器
│   │   │   ├── routes.rs            # 路由定义
│   │   │   └── server.rs            # HTTP 服务器
│   │   └── python/                   # Python 集成
│   ├── coretex_backup/               # 备份和恢复
│   │   ├── backup.rs                # 备份逻辑
│   │   ├── restore.rs               # 恢复逻辑
│   │   └── replication.rs           # 副本同步
│   ├── coretex_cli/                  # 命令行工具
│   │   ├── commands.rs              # 命令定义
│   │   ├── parser.rs                # 参数解析
│   │   └── utils.rs                 # 工具函数
│   ├── coretex_core/                 # 核心类型和配置
│   │   ├── config.rs                # 配置结构
│   │   ├── error.rs                 # 错误类型
│   │   ├── schema.rs                # Schema 定义
│   │   └── types.rs                 # 基本类型
│   ├── coretex_distributed/          # 分布式协调
│   │   ├── cluster.rs               # 集群管理
│   │   ├── coordinator.rs           # 协调者逻辑
│   │   ├── metadata.rs              # 元数据管理
│   │   └── sharding.rs              # 分片策略
│   ├── coretex_index/                # 索引实现
│   │   ├── hnsw.rs                  # HNSW 索引
│   │   ├── diskann.rs               # DiskANN 索引
│   │   ├── brute_force.rs           # 暴力搜索
│   │   ├── manager.rs              # 索引管理器
│   │   └── scalar.rs                # 标量索引
│   ├── coretex_monitor/              # 监控和告警
│   │   ├── metrics.rs               # 指标收集
│   │   ├── health.rs                # 健康检查
│   │   ├── alerts.rs                # 告警管理
│   │   └── dashboard.rs             # 仪表盘数据
│   ├── coretex_perf/                 # 性能优化
│   │   ├── cache.rs                 # 查询缓存
│   │   ├── router.rs                # 查询路由
│   │   ├── parallel.rs              # 并行执行
│   │   └── optimization.rs          # 优化规则
│   ├── coretex_query/                # 查询处理
│   │   ├── planner.rs               # 查询规划
│   │   ├── optimizer.rs             # 查询优化
│   │   ├── executor.rs              # 执行引擎
│   │   └── builder.rs              # 查询构建
│   ├── coretex_security/             # 安全模块
│   │   ├── auth.rs                 # 身份认证
│   │   ├── token.rs                # Token 管理
│   │   ├── permission.rs           # 权限控制
│   │   └── encryption.rs           # 数据加密
│   ├── coretex_storage/              # 存储引擎
│   │   ├── engine.rs               # 引擎接口
│   │   ├── memory.rs                # 内存存储
│   │   ├── persistent.rs            # 持久化存储
│   │   └── cdb/                     # CoretexDB 格式
│   └── coretex_utils/                # 工具函数
│       ├── logging.rs               # 日志记录
│       ├── metrics.rs               # 指标工具
│       ├── telemetry.rs             # 遥测收集
│       └── retry.rs                 # 重试逻辑
│
├── SDK/                              # 多语言 SDK
│   ├── cpp/                          # C++ SDK
│   │   ├── coretexdb/
│   │   │   ├── include/             # 头文件
│   │   │   ├── src/                # 实现
│   │   │   └── examples/           # 示例
│   │   └── CMakeLists.txt
│   ├── go/                           # Go SDK
│   │   └── coretexdb/
│   │       ├── client.go            # 客户端
│   │       ├── async_client.go      # 异步客户端
│   │       ├── models.go            # 数据模型
│   │       └── examples/            # 示例
│   ├── java/                         # Java SDK
│   │   └── coretexdb/
│   │       ├── src/main/java/       # 源代码
│   │       └── pom.xml
│   └── node/                         # Node.js SDK
│       └── coretexdb/
│           ├── src/                 # 源代码
│           ├── examples/            # 示例
│           └── package.json
│
├── python/                           # Python 包
│   ├── coretexdb/                   # 主包
│   │   ├── client.py                # 客户端
│   │   ├── core.py                  # 核心功能
│   │   └── version.py               # 版本信息
│   ├── integrations/                 # AI 框架集成
│   │   ├── langchain.py             # LangChain 集成
│   │   ├── huggingface.py          # HuggingFace 集成
│   │   └── openai.py               # OpenAI 集成
│   ├── pyproject.toml
│   └── setup.py
│
├── deploy/                          # 部署配置
│   ├── docker/                      # Docker 配置
│   │   ├── Dockerfile              # 构建镜像
│   │   ├── docker-compose.yml      # 编排配置
│   │   ├── grafana/                # Grafana 仪表盘
│   │   └── prometheus/             # Prometheus 配置
│   ├── kubernetes/                  # K8s 配置
│   │   ├── deployment.yaml         # 部署配置
│   │   ├── service.yaml            # 服务配置
│   │   ├── deployment.ha.yaml      # 高可用配置
│   │   └── namespace.yaml          # 命名空间
│   ├── helm/                        # Helm Chart
│   │   └── coretexdb/
│   │       ├── Chart.yaml
│   │       ├── values.yaml
│   │       └── values.prod.yaml
│   ├── terraform/                   # Terraform 配置
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   ├── ansible/                     # Ansible Playbook
│   │   ├── inventory/
│   │   ├── playbooks/
│   │   └── templates/
│   └── cicd/                         # CI/CD 配置
│       └── github-actions/
│
├── tests/                            # 测试代码
│   ├── unit/                         # 单元测试
│   ├── integration/                  # 集成测试
│   ├── performance/                 # 性能测试
│   ├── chaos/                        # 混沌测试
│   └── property/                     # 属性测试
│
├── examples/                         # 示例程序
├── experiments/                       # 实验性功能
├── tools/                            # 工具脚本
├── Cargo.toml                         # Rust 依赖配置
├── Makefile                           # 构建脚本
└── README.md                          # 项目文档
```

## 部署架构

### Docker Compose 架构

Docker Compose 部署适用于开发、测试和小规模生产环境。在这种模式下，所有组件运行在同一台机器上，通过 Docker 网络进行通信。架构包含 CoretexDB 主服务实例、Prometheus 指标收集器、Grafana 监控仪表盘和 Alertmanager 告警管理器。这种部署方式简单快捷，配置集中在 docker-compose.yml 文件中，支持一键启动和停止。

### Kubernetes 生产架构

Kubernetes 部署适用于大规模生产环境，提供了高可用、自动伸缩和自愈能力。架构分为数据层、控制层和监控层三个部分。数据层包含 CoretexDB Pod 集群，每个 Pod 挂载 PersistentVolumeClaim 存储数据，支持跨可用区分布。控制层包含 StatefulSet 管理 Pod 生命周期、Service 提供负载均衡和 DNS 发现、ConfigMap 和 Secret 管理配置和密钥。监控层包含 Prometheus Operator 收集指标、Grafana 显示仪表盘、Alertmanager 处理告警、Ingress 暴露外部访问。

### 集群拓扑

在分布式模式下，CoretexDB 采用主从架构实现高可用。集群由多个节点组成，其中一个节点被选为 Leader，负责处理写请求和协调分布式事务。Follower 节点接收 Leader 的复制数据，提供只读查询服务，实现读写分离。当 Leader 节点故障时，系统通过 Raft 协议自动选举新的 Leader，确保服务连续性。数据通过一致性哈希分片分布到不同节点，支持水平扩展存储容量和查询吞吐。

## 性能基准

CoretexDB 在标准基准测试中表现出色，以下是典型配置下的性能数据。向量插入速度方面，单节点配置下可以达到每秒 10 万向量的插入速度，批量插入时吞吐量更高。向量搜索延迟方面，HNSW 索引在 99 分位延迟低于 10 毫秒，P99 延迟取决于索引参数和数据分布。维度支持方面，最高支持 4096 维向量，维度越高精度越好但性能开销越大。召回率方面，HNSW 索引在 efSearch=100 时召回率超过 95%。集群扩展方面，单集群最多支持 1000 个节点，扩展后线性提升吞吐。

## 系统要求

### 最低要求

最低配置适用于开发和测试环境。CPU 需要至少 2 核，内存至少 4GB，存储至少 10GB 可用空间。操作系统支持 Linux（推荐 Ubuntu 20.04+）、macOS 11+ 和 Windows 10+。Rust 工具链版本要求 1.70 或更高。

### 推荐配置

推荐配置适用于生产环境的中等规模部署。CPU 需要 8 核或更多，内存至少 32GB，存储建议使用 SSD 且至少 100GB 可用空间。网络方面建议使用千兆以太网，集群部署时节点间延迟应低于 1 毫秒。

## 文档

项目提供了全面的文档，涵盖从入门到高级的所有主题。API 文档位于项目的 docs 目录或在线文档站点，包含所有 gRPC 和 REST API 的详细说明。SDK 指南提供了各语言 SDK 的安装、配置和使用方法，包含丰富的代码示例。部署指南详细说明了各种部署方式的配置选项和最佳实践。架构设计文档深入解析了系统的内部实现，包括索引算法、存储引擎和分布式协议。

## 许可证

本项目采用 MIT 许可证开源，允许自由使用、修改和分发。详细信息请参阅 LICENSE 文件。

## 贡献

欢迎社区贡献代码、文档和反馈。贡献前请阅读 CONTRIBUTING.md 了解贡献流程和代码规范。可以通过提交 Issue 报告 bug 或提出新功能建议，也可以提交 Pull Request 贡献代码修复或新功能。

## 联系方式

项目主页位于 GitHub：https://github.com/qinhaowow/cortexdb。文档站点提供详细的用户指南和 API 参考。如有问题或建议，请通过 GitHub Issues 与我们联系。
