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

## 代码、SQL 与完整栈 pprof

- `RunPublisher` 和 `RunStatusPublisher` 各只有单条循环：`Begin → SELECT ... LIMIT 1 FOR UPDATE SKIP LOCKED → kafka.Writer.WriteMessages(单条) → UPDATE → Commit`。事务与行锁覆盖同步 Broker 确认；每条事件独立 DB 往返。空队列轮询等待 200 ms，出错等待 2 秒。
- Producer 在每个进程中复用，`RequiredAcks=RequireAll`，`BatchTimeout=20 ms`；当前每次 `WriteMessages` 只传一条并同步等待。没有证据支持关闭 `RequireAll`，也不应为分数降低确认级别。
- 消息与状态 Outbox 各有 `(created_at,message_id) WHERE published_at IS NULL` 部分索引。空队列时真 PostgreSQL `EXPLAIN (ANALYZE, BUFFERS)` 分别显示 `Index Scan`，执行时间 0.028 / 0.037 ms、shared hit=1。另一次同配置 500/s 诊断负载中，当待发布消息/状态为 1947/8 时，索引仍命中，执行时间 0.119/0.054 ms。采样瞬间 `pg_stat_activity` 为 18 idle、5 active WALWrite、1 active WalSync、1 active CPU、1 idle in transaction。参见[原始 SQL 与状态](benchmarks/results/fullstack-diagnostic-postgres.txt)。未加载 `pg_stat_statements`，因此没有按 SQL 归因的历史 p95。

完整栈 pprof 通过 `PPROF_ADDR=127.0.0.1:6060` 在两个真实容器内采集；Block/Mutex 采样仅在本次剖析启动，常规压测关闭。15 秒、500/s 剖析负载从 20:15:46.522 至 20:16:04.670；100 条连接于 20:15:46.640 建好，两实例 CPU profile 从 20:15:46.681 至 20:15:55.012，**在发送窗口内**。CPU、heap、goroutine、block、mutex 均为本次完整 Compose，而非此前 Windows 非容器剖析。这个剖析负载时长不同于 Baseline，不能直接比较吞吐。两实例 CPU 样本 app1 1.70 CPU 秒、app2 0.71 CPU 秒；app1 中 `linux.Syscall6` 0.56 秒、`runtime.futex` 0.32 秒，`pgx.Conn.Query` 累计 0.30 秒，Outbox 发布调用路径 CPU 约 0.02 秒。Heap 样本 app1 in-use 约 13.7 MiB；goroutine 快照 app1 约 242、app2 约 42，以 WS read/write goroutine 为主。JSON 编码和应用锁均不是显著 CPU 热点。

为避免 Block profile 中自进程启动以来的累计等待误导，另一次相同 500/s 诊断负载在发送中采集相隔 4.24 秒的 Block 和 Mutex 快照，计算 pprof diff。`RunPublisher → PublishNext → Kafka.Publish → kafka.Writer.WriteMessages → runtime.selectgo` 累计阻塞 **3.56 秒**；相同状态发布路径阻塞 **3.74 秒**。两个各自串行的发布循环在此窗口几乎都在等待同步 Producer。Mutex profile diff 总量只有约 **16.13 ms**，发布路径占比不足 1 ms。Goroutine 快照确有每类 Outbox 各一条发布 goroutine。诊断负载 4998 条、发送窗口首次送达约 202.49/s、P95 6867.9 ms、工具失败 0；参见[诊断负载](benchmarks/results/fullstack-diagnostic-500-load.json)、[采样时刻](benchmarks/results/fullstack-diagnostic-times.txt)与[原始 pprof 和采样说明](benchmarks/profiles/fullstack-before-batch/README.md)。最初误采于发送窗口结束后的 `*-fullstack-before.pprof` 未用作本次结论，也没有归档进 Git。

**结论：主要瓶颈是每条 Outbox 事件单独同步 Kafka `WriteMessages`，两个实例各自每种 Outbox 只有一条串行发布循环；事务同时横跨 Broker 确认，造成逐条数据库往返和长事务。** Kafka 消费 lag 在负载期间逐秒为 0，Outbox 索引扫描亚毫秒且 PostgreSQL CPU 未饱和，故不先调 Consumer、索引或连接池。下一步仅把一次锁定、写 Kafka、标记已发布扩展为小批次；仍使用 `RequireAll`，仍在 Broker 确认后更新 Outbox，保留至少一次投递与崩溃后可能重复的语义。

## 最小优化与可重复复测

优化提交 `6eaffa8`：Message 和 Status Outbox 各次最多取 32 行，保留 `FOR UPDATE SKIP LOCKED` 和原有部分索引；Kafka Writer 仍复用、`RequiredAcks=RequireAll`、`BatchTimeout=20ms`、同步等待，只是一次 `WriteMessages` 传入整批；Broker 成功后用一条 `UPDATE ... WHERE message_id = ANY($1::bigint[])` 标记整批，检查受影响行数再提交。错误则回滚整批，事件继续待发布。未改变 Topic、分区键、协议、ACK、消费者或其他业务路径。代价：Broker 已接受而 DB 提交失败时可能重复整批事件；DB 事务持锁的最长时间仍覆盖一次 Broker 确认。真 PostgreSQL `TestOutboxBatchRollbackAndMark` 验证 Message/Status 两类 2 行批次错误后均保留、成功后均标记；客户端和消费者必须继续按消息 ID 幂等处理。

与 Baseline 同为 100 连接/100 用户、目标 500/s、10 秒发送 + 3 秒排空；两个 app 与全部基础设施真实运行，无 pprof 采样，使用相同逐秒观测方法。第一轮 Docker Stats 子进程因 PowerShell 执行策略未启动，故保留该轮负载/PG/Kafka/app 数据但不拿它比较容器资源；第二轮修正子进程启动并完整采集。三个测试开始前 Outbox 均为 0。相同配置前后结果如下：

| 指标 | 修改前 Baseline | 优化后第 1 轮 | 优化后第 2 轮 |
| --- | ---: | ---: | ---: |
| 实际发送 / 秒 | 4999 / 10.0018 = 499.8 | 4997 / 10.0005 = 499.7 | 4998 / 10.0005 = 499.8 |
| 窗口首次送达 / 秒 | **177.97**（1780 条） | **497.78**（4978 条） | **497.77**（4978 条） |
| 3 秒排空后累计首次送达 | 3770 | 4997 | 4998 |
| P50 / P95 / P99 ms | 5753.5 / 6906.5 / 7174.2 | 39.0 / 56.0 / 70.0 | 42.0 / 58.0 / 87.0 |
| 逐秒采样 Message Outbox 峰值 | 4153 | 22 | 26 |
| 逐秒采样 Status Outbox 峰值（发送窗） | 1078；发送后 2740 | 37 | 31 |
| 逐秒采样 Kafka lag 峰值 app1/app2 | 0 / 0 | 3 / 3 | 12 / 7 |
| 两实例 Kafka 发布成功计数增量 | 1744 | 9994 | 9996 |
| PG 提交事务增量 | 16935 | 34574 | 33700 |
| PG 活跃连接采样峰值 | 5 | 7 | 8 |
| app1/app2 CPU 秒增长 | 3.61 / 1.26 | 6.56 / 3.40 | 6.08 / 3.21 |
| app1/app2 RSS 采样峰值 MiB | 93.7 / 31.5 | 42.9 / 28.0 | 46.1 / 34.9 |
| 工具失败 / 意外断连 | 0 / 0 | 0 / 0 | 0 / 0 |

这些 CPU/RSS 是不同运行的采样，基线已经处理大量历史负载，**不能将 RSS 较低归因于批量优化**。CPU 增长反映实际处理更多事件；第二轮 `docker stats` 负载窗口 5 个样本的均值/峰值：app1 52.49/63.57%、app2 28.40/33.47%、PostgreSQL 59.36/69.06%、Kafka 89.85/238.70%。均非精确持续峰值。第二轮全部 61 个 PostgreSQL/Kafka/app 逐秒样本无采集错误。其时间序列如下：

| t 秒 | Message 待发布 | Status 待发布 | Kafka lag app1/app2 |
| ---: | ---: | ---: | ---: |
| -0.6 | 0 | 0 | 0 / 0 |
| 0.4 | 20 | 31 | 12 / 7 |
| 2.4 | 26 | 24 | 4 / 0 |
| 4.4 | 13 | 14 | 0 / 0 |
| 6.4 | 24 | 23 | 2 / 0 |
| 8.4 | 18 | 23 | 0 / 0 |
| 9.4 | 18 | 22 | 0 / 0 |
| 10.4 | 0 | 0 | 0 / 0 |

直接按第二轮发送窗口查询真实 PostgreSQL：4998 条消息均 `delivered`，各自 Message/Status Outbox 均已发布，`delivery_attempts>0` 为 0；全库两类待发布为 0。原始文件：[第 1 轮负载](benchmarks/results/fullstack-after-batch-500-load.json)、[第 1 轮逐秒样本](benchmarks/results/fullstack-after-batch-500-observe.jsonl)、[第 2 轮负载](benchmarks/results/fullstack-after-batch-500-repeat-load.json)、[第 2 轮逐秒样本](benchmarks/results/fullstack-after-batch-500-repeat-observe.jsonl)、[第 2 轮 Docker Stats](benchmarks/results/fullstack-after-batch-500-repeat-docker.psv)。

### 保持 500/s 的 30 秒稳定性检查

这是额外稳定性检查，**不与 10 秒 Baseline 混作一组同条件因果对比**。100 连接/100 用户、目标 500/s、30 秒发送 + 3 秒排空：发送 14970 条，发送窗口首次送达 14946 条，**498.20 条/秒**；P50/P95/P99 41.0/58.0/83.0 ms，工具失败/意外断连/重复帧均为 0。61 个逐秒样本错误数 0，负载窗 Message/Status Outbox 峰值 24/27，Kafka lag 每组峰值 37/39，均为短时而非无界增长。t=0.5、5.5、10.5、15.5、20.5、25.5、30.5 秒时，Message 待发布为 19、13、18、14、15、13、0；Status 为 16、13、23、13、14、15、0。两组 lag 在 30.5 秒为 0。直接查真实 PostgreSQL：14970 条全部 delivered、两类 Outbox 均已发布，重试 `delivery_attempts>0` 为 0。发送结束后 app1/app2 连接 Gauge 0，goroutine 40/39。负载期 15 个 Docker Stats 样本均值/峰值 CPU：app1 63.91/75.10%、app2 33.65/41.85%、PostgreSQL 70.32/88.20%、Kafka 133.35/483.12%。这只证明此单机完整 Compose 负载模型下 30 秒稳定，不能外推到生产或长期容量。[30 秒负载](benchmarks/results/fullstack-after-batch-500-30s-load.json)、[逐秒样本](benchmarks/results/fullstack-after-batch-500-30s-observe.jsonl)、[Docker Stats](benchmarks/results/fullstack-after-batch-500-30s-docker.psv)。

## 回归与限制

- `go test -count=1 ./...`、`go vet ./...`、`go test -race -count=1 ./...` 在不设置外部集成环境变量时全部通过。真实 PostgreSQL 批量/单条 Outbox 测试在暂停 app 发布者后通过；完整 `./tests` 在真实双实例环境通过（20.78 秒）；真 Kafka 跨实例、离线重连、幂等的定向 race 测试通过。
- 一次同时设置所有真实服务变量、并让运行中的 app 与 repository 集成测试共享库的 `go test ./...` **失败**：app 抢先发布了测试专用 Outbox 行；同次 `TestOfflineReconnectAndLostACK` 还出现重复 `message.new` 在 ACK 回执之前到达。后者单独和完整 `./tests` 重跑均通过。测试隔离和重复帧容忍仍是测试工具限制，不能删除这次失败或声称所有组合任意同时运行都稳定。
- 真 Kafka 停机时 `TestRealKafkaOutageDurableFallback` 通过（6.66 秒）：消息 42506 数据库补发并 ACK，两类 Outbox 均待发布；Broker 重启后两类 Outbox 和全库 backlog 均为 0。恢复后立即运行严格 Kafka 测试 **失败一次**（`delivery_attempts=1`）；Broker 日志显示两组约 30 秒重平衡后稳定，重跑 0.50 秒通过。真 Redis 停机时严格跨 Kafka/ACK 测试通过（2.56 秒），已恢复 healthy。
- 没有测试 750/1000 msg/s：本轮目标是定位并消除 500/s 的显著瓶颈。30 秒稳定不等于长期运行稳定或最大容量。Kafka 单节点、无跨主机网络；在线客户端重复事件去重仍由客户端承担。Prometheus 默认只抓 app1。其他边界见 [FINAL_VALIDATION.md](FINAL_VALIDATION.md)。
