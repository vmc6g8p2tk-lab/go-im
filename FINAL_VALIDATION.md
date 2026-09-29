# Go IM V1.0 最终真实环境验收

日期：2026-09-29（Asia/Shanghai）。本次从此前 Docker Engine 不可用的断点继续，在**实际 Docker Desktop Engine** 上执行。结论：**暂不封版，不打 `v1.0.0` tag，不把现有压测数字写成稳态最大吞吐。** 现有功能可在本机 Compose 中演示，但高负载延迟、Outbox 积压和 Redis 路由的功能边界需要明确处理。原始负载 JSON 见 [BENCHMARK.md](BENCHMARK.md)，故障过程见 [FAILURE_TEST.md](FAILURE_TEST.md)。

## 验收状态分类

### 【真实验证通过】

Docker Engine/Compose 完整启动、真 Redis Presence Key/TTL/心跳/自然过期与停机降级、真 Kafka Broker/Topic/两组消费与重启恢复、双实例跨 Kafka 单聊和 ACK、Prometheus 抓取 app1、Grafana 数据源查询、全套 race detector、完整栈低消息率 5000 与 10000 连接均已按下文条件实测。

### 【代码已实现但未完全验证】

群聊真 Kafka 大群扇出、10000 连接长时间或高消息率稳定性、完整栈消息最大稳态吞吐、在线客户端收到 Kafka 重复事件后的去重、多实例监控聚合及真实 PostgreSQL 容器宕机恢复均未完成对应验收。500 条/秒目标已经导致明显积压，不能把窗口内 208.09 条/秒称作最大稳定吞吐。

### 【未实现】

Redis 定向投递选路、生产级 TLS/WSS、JWT 主动吊销、PostgreSQL/Kafka/Redis 高可用集群、跨主机分布式部署、群逐成员 ACK 均不在当前实现内。Redis Presence Key 存在不等于实现 Redis Routing。

### 【环境限制】

本轮只在单台 Windows 主机的 Docker Desktop/WSL2 中运行，Kafka 单 Broker 单副本。首次 10000 连接尝试被 Windows 临时端口占用打断，端口回收后重试通过；未在独立压测机或跨主机网络复验。完整栈 CPU/RSS 指标仅采样 app1，不能代表整栈资源。项目尚无 GitHub remote 或实际模块路径。

## 环境与启动证据

- `docker version`：Client / Server 均为 29.8.1，`desktop-linux` Engine 已连接；`docker compose version`：v5.5.1；`docker info`：Linux/WSL2、24 CPU、Docker 可用内存 15.52 GiB。宿主机 Windows 10 Pro 22H2、i7-13700KF、物理内存约 31.82 GiB。
- 实际执行 `docker compose --env-file .env -f deployments/compose.yaml --profile multi up --build -d` 成功。`app`、`app2`、PostgreSQL 17、Redis 8、Kafka 4.3.1、Prometheus 3.15.0、Grafana 13.2.2 均运行；PostgreSQL/Redis/Kafka 报 healthy；迁移和 Topic 初始化容器退出码 0。PostgreSQL 实际有 27 条迁移记录。
- 两台 IM 的 `/healthz`、`app1 /metrics`、Prometheus `/-/ready`、Grafana `/api/health` 均 HTTP 200。容器内 `redis-cli PING` 得到 `PONG`。Kafka Topic `im.message.created.v1` 为 8 分区、单副本；两组 `im-delivery-app1` 和 `im-delivery-app2` 实际加入并消费，事后 offset/lag 检查为 0。

## 真实功能验收

| 项目 | 实际结果 | 判定 |
| --- | --- | --- |
| 注册、登录、bcrypt、JWT、双向 WebSocket、落库、历史、幂等、ACK、离线补发 | 本机真实 PostgreSQL 端到端与全套单元/集成测试已有通过记录；本次完整 Compose 上又运行跨实例集成测试，创建新用户和消息并直接查 PostgreSQL。 | 通过已测路径 |
| 双实例跨 Kafka 消息与 ACK | `TestRealKafkaCrossInstanceDeliveryAndStatus`：A 连 app1、B 连 app2；A 获得 `server_ack`，B 获得 `message.new` 并 ACK，A 收到 `message.status=delivered`；库中状态 `delivered`、消息与状态 Outbox 已发布、`delivery_attempts=0`。Broker 恢复且消费组稳定后再次通过（0.48 秒）。 | 通过稳定状态路径 |
| Redis 真服务与 Presence | 真 Redis `PING=PONG`；75 秒长连接测试时 `im:route:3/4` 含 `app1`，TTL 与时间分数随心跳更新；Redis 停止时两台 IM 发出 Presence 警告，但跨 Kafka 消息与 ACK 测试仍通过；Redis 重启恢复 healthy。 | Presence 写入/续期及降级通过；**Redis 未参与当前投递选路** |
| Kafka Broker、重复事件 | 真 Topic、两组 Consumer Group 和分区 offset 已核查。向真 Broker 注入已投递消息 ID 的重复事件后，两实例 `im_kafka_consume_total{result="ok"}` 各增加 1；数据库该消息及 outbox 仍各 1 行、状态 delivered。 | 消费与数据库逻辑幂等通过；在线客户端重复帧仍需去重 |
| Prometheus/Grafana | Prometheus Targets API 显示 `http://app:8080/metrics` 为 `up`、无抓取错误；Grafana 数据源指向真实 Prometheus，`Go IM Overview` Dashboard UID `go-im-overview` 有 10 面板；通过 Grafana 数据源代理实查 `im_websocket_connections` 得到成功结果。 | 实际服务与查询通过；未截取浏览器面板图片；默认只监控 app1 |
| Race Detector / 静态检查 | Go 1.27.1 + MSYS2 GCC，在完整 Compose 环境设置两实例 URL 后，`CGO_ENABLED=1 go test -race -count=1 ./...` 退出码 0；另以 `-race -run TestRealKafkaCrossInstanceDeliveryAndStatus -v ./tests` 明确执行并通过（0.40 秒）；`go vet ./...`、`go mod verify` 通过。 | 已覆盖本次执行路径；没有长期并发安全保证 |

## 真实故障恢复

- 停止真 Redis 后，跨实例 Kafka 消息、接收方 ACK、发送方状态通知仍通过；重启 Redis 后服务 healthy。这验证聊天不依赖 Presence 可用性，不能声称 Redis 故障期间仍能精确查询在线路由。
- 停止真 Kafka Broker 后，新增 `TestRealKafkaOutageDurableFallback` 实际通过：接收方通过 PostgreSQL 约 7 秒补发并 ACK，消息状态 delivered、`delivery_attempts=1`，消息/状态 Outbox 均为待发布。Broker 重启后同一消息两个 Outbox 均变为已发布，全库待发布数回到 0；Consumer Group 恢复。
- 重启 app2 后，立即运行严格跨 Kafka 测试**失败一次**：消费组重平衡期间消息走数据库补发，`delivery_attempts=1`，未满足测试要求的纯 Kafka 快速链路。Broker 日志显示旧成员约 30 秒后超时、消费组稳定；稳定后重跑通过（0.48 秒）。这是实际恢复延迟，不隐去失败。
- 另对 app2 执行 `stop -t 0`：真实 Redis `im:route:888` 在停止后仍含 `app2`，TTL 从 55/47 秒降至 5 秒，再到 `EXISTS=0`、`TTL=-2`；两条客户端连接按预期断开。app2 随后启动。此证据验证 TTL 自然清理，不代表离线状态瞬时准确。
- app2 再启动后 `/healthz` 返回 200，`im-delivery-app2` 的 8 个分区事后 lag 均为 0；严格跨 Kafka 消息与 ACK 回归再次通过（0.32 秒）。
- 既有 PostgreSQL TCP 代理断连与两进程强杀恢复测试见 [FAILURE_TEST.md](FAILURE_TEST.md)；本轮没有实际停止 PostgreSQL 容器，也没有验证数据库主从切换。

## 完整 Compose 性能与资源

客户端从 Windows 通过回环连 app1；app2 同时消费 Kafka 事件。CPU/RSS/goroutine 由 app1 `/metrics` 采样，**不包含**压测进程、app2、PostgreSQL、Redis、Kafka、Prometheus、Grafana。Docker Stats 在 500 条/秒档后显示 Kafka 约 973 MiB、PostgreSQL约 112 MiB；这些是单次事后快照，不是区间峰值。具体参数与原始 JSON 见 [BENCHMARK.md](BENCHMARK.md)。

| 负载 | 实测结果 | 限制 |
| --- | --- | --- |
| 100 条/秒目标、100 连接、15 秒 | 1499 发、1477 首次送达，窗口 81.40 条/秒，P95 3091 ms、P99 3197 ms，0 工具失败/断连。 | 已有秒级尾延迟；3 秒排空后仍少 22 条客户端首次送达。 |
| 500 条/秒目标、100 连接、10 秒 | 4990 发、3115 首次送达，窗口 208.09 条/秒，P95 6880 ms、P99 7064 ms，470 重复帧，0 工具失败/断连。 | 测试后消息/状态 Outbox 待发布 2736/1062，后续排空；没有稳态。 |
| 2000 连接、100 用户、5 条/秒、10 秒 | 2000/2000 建连，49/49 首次送达，0 断连，P95 214 ms，app1 峰值 RSS 102.8 MiB、4043 goroutine。 | 低速率连接测试；931 个额外设备帧。 |
| 5000 连接、100 用户、5 条/秒、10 秒 | 5000/5000 建连，49/49 首次送达，0 断连，P95 213 ms，app1 峰值 RSS 208.1 MiB、10044 goroutine。 | 低速率连接测试；2401 个额外设备帧。 |
| 首次 10000 连接尝试 | 第 9049 条由 Windows `connectex` 报本地 socket 地址/端口不可用，未进入发送窗口。 | 此前连续关闭 2000/5000 连接；不能归因于服务端上限。动态端口范围 49152–65535。退出后服务连接/队列 Gauge 均为 0。 |
| 端口回收后单独重试 10000 连接 | 10000/10000 建连，49/49 首次送达，0 工具失败/意外断连，P95 215 ms、P99 231 ms，app1 峰值 RSS 364.4 MiB、20042 goroutine。 | 仅 100 用户、5 条/秒、10 秒发送窗口；4851 个额外设备帧，不能证明 10000 连接在高吞吐下稳定。退出后连接/队列 Gauge 均为 0、Outbox 为空。 |

100 条/秒档已出现明显延迟，500 条/秒档出现积压，故按停止条件不继续 1000/2000/5000 条/秒档。这里测到的是特定发送窗口的客户端首次送达率，不是系统最大吞吐。此前非容器、无 Redis/Kafka 的 5000 连接回环结果不能与完整栈混写。

## 代码边界与封版判断

1. Redis `im:route:<user>` 是可过期在线实例提示。**当前生产投递路径未调用 `RedisStore.Instances`**；Kafka 采用每实例独立消费组广播事件，本地 Hub 决定是否投递。将此版本称为“Redis 定向路由”会误导，README 和学习记录应按代码实际描述。
2. 单节点 Kafka replication factor 1、单 PostgreSQL、单 Redis 只适合本机开发验证；没有跨主机、高可用与长期运行数据。Broker/实例重启可恢复，但消费组重平衡期间实时路径存在约 30 秒窗口。
3. 高频负载的 Outbox 单条发布和状态事件成本已经在真栈产生积压；在无改动、无重复测量前不能给出稳态容量或简历上的“高并发吞吐量”。
4. Prometheus 默认只抓 app1。10,000 连接首次尝试受本机端口资源限制，等待回收后低消息率重试通过；更长时间或高消息率尚未验证。群聊真 Kafka 大群、在线重复帧、长期泄漏、真实 PostgreSQL 容器宕机与跨主机网络未验收。
5. `go.mod` 仍为 `github.com/example/go-im`，仓库没有 remote；待用户确定 GitHub 名称后再替换路径。`.env`、二进制、日志和 pprof 保持不入 Git。**本轮不新增业务功能，也不推进第二阶段。**

因此 V1.0 可以作为**本机演示与学习里程碑**，但目前**不正式封版**。若要封版，需要先确定并记录目标门槛，再针对高负载积压、监控第二实例、Redis 路由定位以及 10,000 连接更长时间/更高消息率逐项补证；任何后续实现或性能改动都应由独立任务授权。
