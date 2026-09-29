# V1.0 完整栈 Outbox 积压调查

日期：2026-09-29（Asia/Shanghai）。只调查完整 Docker Compose + PostgreSQL + Redis + Kafka + 两台 IM 实例的直聊 Outbox。本文保留修改前 Baseline，并将后续每项优化与相同负载的真实复测分开记录。此前 [BENCHMARK.md](BENCHMARK.md) 的非容器 Windows pprof 不能解释本次积压。

## 修改前可重复 Baseline

启动 `docker compose --env-file .env -f deployments/compose.yaml --profile multi up --build -d`。压测客户端运行在同一 Windows 10 主机，通过 `127.0.0.1:8080` 连 app1；app2 仍运行并消费独立 Kafka Group。Docker Desktop 29.8.1 / Compose 5.5.1 / WSL2；24 逻辑 CPU、宿主物理内存约 31.82 GiB，Docker 内存配额 15.52 GiB。Go 1.27.1。使用未修改业务代码的提交 `83ecb752285daac7d81d9fb832429e75e7ddbd83`，与此前 500 档完全相同的参数：**100 WebSocket 连接、100 用户、目标 500 条/秒、发送窗口 10 秒、3 秒排空**，每条文本 `load test message`；接收方首次送达即 ACK。注册/登录和建连不计入发送窗口。

命令：`go build -o loadtest.exe ./cmd/loadtest`，然后 `./loadtest.exe -base http://127.0.0.1:8080 -connections 100 -users 100 -rate 500 -duration 10s -output baseline.json`。`scripts/observe-compose.go` 每秒读真实 PostgreSQL、两实例 `/metrics` 和 Kafka 两组 committed/end offset；`scripts/observe-docker.ps1` 在负载期间取 `docker stats`。`scripts/analyze-observe.ps1` 按 `started_at` 对齐时间线。监测采样本身也消耗少量本机资源，优化后必须采用同样采样方式。

| 指标 | 这次实际结果 |
| --- | ---: |
| 发送 | 4999 条 / 10.0018 秒，约 499.8 条/秒 |
| 首次送达 | 发送窗口内 1780 条，**177.97 条/秒**；3 秒排空后累计 3770 条 |
| P50 / P95 / P99 | 5753.5 / 6906.5 / 7174.2 ms（累计首次送达样本） |
| 重复帧 / 工具失败 / 意外断连 | 526 / 0 / 0 |
| app1 服务 CPU / RSS / goroutine | 31.53% / 峰值 93.7 MiB / 峰值 245（`/metrics`，仅 app1） |
| app2 CPU 秒增长 / RSS / goroutine | 1.26 CPU 秒 / 峰值 31.5 MiB / 峰值 43（约 11 秒采样间隔） |
| Kafka 两组 lag | 负载期间逐秒样本均为 0；61 个样本没有采集错误 |

原始文件：[负载 JSON](benchmarks/results/fullstack-baseline-500-load.json)、[逐秒 PostgreSQL/Kafka/app 指标 JSONL](benchmarks/results/fullstack-baseline-500-observe.jsonl)、[逐次容器 Stats](benchmarks/results/fullstack-baseline-500-docker.psv)。Windows 回环和单节点 Broker 结果不能外推到生产环境。

### 负载期间时间线

`t` 相对发送窗口起点；Outbox 为当时 PostgreSQL 中 `published_at IS NULL` 行数。Kafka lag 是采样时各分区 log end offset 减消费组 committed offset 后求和；每组分别观测，不用事后 lag 代替负载期间数值。

| t 秒 | Message 待发布 | Status 待发布 | Kafka end offset 总和 | app1/app2 lag |
| ---: | ---: | ---: | ---: | ---: |
| -0.1 | 0 | 0 | 12408 | 0 / 0 |
| 0.9 | 389 | 14 | 12521 | 0 / 0 |
| 2.9 | 1225 | 11 | 12848 | 0 / 0 |
| 4.9 | 2062 | 11 | 13177 | 0 / 0 |
| 6.9 | 2898 | 231 | 13502 | 0 / 0 |
| 8.9 | 3735 | 1078 | 13829 | 0 / 0 |
| 9.9 | **4153** | 996 | 13992 | 0 / 0 |
| 12.9 | 3956 | **2740** | 14486 | 0 / 0 |
| 15.9 | 3707 | 2488 | 14989 | 0 / 0 |

从负载起点前一个样本到结束后第一个样本（约 11 秒），两实例 Kafka 发布成功计数分别增加 876 和 868，消费成功计数各增加 1744；Topic end offset 总和增加 1749。生产 4999 条消息远快于消息 Outbox 的发布，Kafka 消费组采样 lag 一直为 0。**积压首先发生在 PostgreSQL Outbox → Kafka 发布之间，而不是 Kafka → 消费者之间。** Status Outbox 在 ACK 流量到来后也积压。停止发送后两种 Outbox 最终排空，但本次逐秒观测只持续到约 +30 秒，当时仍有 2538/1317 条；不能伪造精确排空时长。

PostgreSQL 负载窗口内 25 个连接以下、活跃连接采样最高 5；相邻约 11 秒样本间数据库提交事务 +16935、回滚 +123。`docker stats` 的 5 个负载窗口样本中 PostgreSQL CPU 平均 52.18%、最高 76.67%，Kafka 平均 36.14%、最高 90.64%，app1 平均 33.67%、最高 44.23%，app2 平均 11.36%、最高 12.57%。Docker CPU 100% 约对应一个逻辑 CPU；这些不是精确区间峰值。app1/app2 所有成功数据库操作的指标均值约 1.670/0.656 ms，仅为混合 SQL 的平均数，不是 Outbox SQL 的专属延迟。

## 代码与 SQL 初查（待 pprof 交叉验证）

- `RunPublisher` 和 `RunStatusPublisher` 各只有单条循环：`Begin → SELECT ... LIMIT 1 FOR UPDATE SKIP LOCKED → kafka.Writer.WriteMessages(单条) → UPDATE → Commit`。事务与行锁覆盖同步 Broker 确认；每条事件独立 DB 往返。空队列轮询等待 200 ms，出错等待 2 秒。
- Producer 在每个进程中复用，`RequiredAcks=RequireAll`，`BatchTimeout=20 ms`；当前每次 `WriteMessages` 只传一条并同步等待。没有证据支持关闭 `RequireAll`，也不应为分数降低确认级别。
- 消息与状态 Outbox 各有 `(created_at,message_id) WHERE published_at IS NULL` 部分索引。空队列时真 PostgreSQL `EXPLAIN (ANALYZE, BUFFERS)` 分别显示 `Index Scan`，执行时间 0.028 / 0.037 ms、shared hit=1；这是**空队列计划**，还需在有 backlog 时复核。未加载 `pg_stat_statements`，因此没有按 SQL 归因的历史 p95。

当前证据支持发布端处理速率不足；具体是同步 Kafka 等待、长事务、单条往返中的哪一段，仍需完整栈 pprof 和有积压时的 SQL 观察后确定。此时不改可靠性路径。
