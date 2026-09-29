# 第一阶段实际测试记录

日期：2026-09-27（Asia/Shanghai）。以下仅记录执行过的检查；没有吞吐、P95/P99、CPU 或内存的压测数据。

## 环境

- Windows x86-64；工作区内临时安装并校验 SHA256 的 Go 1.27.1 工具链。
- 本机无 Docker / Docker Compose。使用临时 PostgreSQL 18.3 Windows 二进制在 `127.0.0.1:15432` 运行真实数据库，应用在 `127.0.0.1:18080` 运行。两者仅用于本次验证，不是项目的交付依赖。
- 从项目 SQL 文件执行迁移，重复执行迁移命令也成功；`schema_migrations` 有 3 条记录。

## 已通过

| 检查 | 实际结果 |
| --- | --- |
| `go test -count=1 -v ./...` | 全部测试通过 |
| `go vet ./...` | 退出码 0 |
| Linux 交叉编译与模块校验 | `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./cmd/server` 成功；`go mod verify` 输出 `all modules verified` |
| 开发密钥脚本 | `scripts/init-env.ps1` 创建 `.env` 成功，Git 忽略该文件 |
| Compose YAML 语法 | YAML 解析通过，识别 `postgres`、`migrate`、`app` 三个服务；未验证 Docker Compose 语义或启动 |
| 密码与 JWT | bcrypt Hash/校验、签发/验证、伪造 token 拒绝测试通过；真实数据库中的密码是 bcrypt Hash |
| 两用户端到端 | 实际注册、登录、建立两条 WebSocket 连接，A→B 与 B→A 均收到相同数据库 ID 的消息 |
| PostgreSQL 落库 | 测试直接查询 `messages`，逐条确认 sender、recipient、body 和 ID；并验证历史查询及 `before_id` 游标 |
| 双向并发 | 双方各发送 20 条，共 40 条；每条连接收到 40 个唯一消息 ID；PostgreSQL 查询确认 40 条记录 |
| 连接释放 | `Hub.CloseAll` 后客户端连接关闭，连接从 Hub 删除；此单元测试连续 20 次通过 |
| Origin 限制 | 不在白名单中的浏览器 Origin 被拒绝 |

最后一次全量测试的端到端部分输出：

```text
=== RUN   TestTwoUsersWebSocketAndPersistence
--- PASS: TestTwoUsersWebSocketAndPersistence (0.38s)
=== RUN   TestConcurrentBidirectionalDelivery
--- PASS: TestConcurrentBidirectionalDelivery (0.33s)
PASS
ok  github.com/example/go-im/tests  1.097s
```

最后一次独立数据库核查结果为 `users=18`、`messages=170`、`schema_migrations=3`；用户/消息数包含此前重复运行的测试数据，不代表吞吐或生产负载。

## 发现并修复

连接释放测试首次失败：`auth.ok` 帧先于 Hub 注册发出，客户端一收到成功帧就触发关闭时，`CloseAll` 可能漏掉该连接。已调整为先注册 Hub，再发送 `auth.ok`；重跑 20 次连接释放测试与全套测试均通过。

## 未验证及原因

- `docker compose up --build`：本机没有 Docker CLI/Engine，因此 **Compose 启动未验证**。Compose 文件与 Dockerfile 已提供，需在有 Docker 的环境执行 README 中的启动和测试命令。
- `go test -race`：本机 Go 的 CGO 未启用且没有 GCC/Clang；命令返回 `go: -race requires cgo`。功能性并发测试与代码审查已完成，但不能把它们写成 race detector 通过。
- 任何性能数字：尚未执行压测，不能写入简历或项目说明。

由于 Docker Compose 未实际启动，第一阶段的**容器化开发环境验收仍待完成**；本地真实 PostgreSQL 和 Go 服务的核心链路已经验证。

## V1.0 迭代：发送幂等（2026-09-27）

在同一真实 PostgreSQL 18.3 实例上执行新增迁移 `004`、`005`，保留已有第一阶段数据；应用以新二进制启动。`go test -count=1 -v ./...` 全部通过，包含原有双向消息、40 条并发消息和连接释放测试。

- 同一 `client_message_id` 连续发送 3 次：返回同一服务器消息 ID，数据库仅 1 行。
- 8 条并发 WebSocket 连接用同一发送者与同一 ID 发送：返回同一服务器消息 ID，数据库仅 1 行。
- 同一 ID 更换正文：收到 `idempotency_conflict`，原消息未被覆盖。
- 单独启动测试服务、发送、强制终止进程、重启并重发：返回原服务器消息 ID，数据库仍仅 1 行；`TestIdempotencySurvivesServerRestart` 通过。

这些检查证明数据库逻辑记录的幂等行为；**尚未**证明消息帧可靠送达。旧协议不传客户端 ID 时保留原行为，仍不具备重发幂等性。

## V1.0 迭代：ACK、重试与离线补发（2026-09-27）

在同一真实 PostgreSQL 18.3 数据库执行迁移 `006`–`010`，启动新服务后执行真实 WebSocket 测试：

- `TestDeliveryAcknowledgmentAndDuplicateACK`：`server_ack` 后数据库仍为 `created`；发送者伪造接收 ACK 被拒；接收者 ACK 后状态与首次确认时间持久化；重复 ACK 不改时间。
- `TestOfflineReconnectAndLostACK`：接收者离线时消息保留；首次连接未 ACK 即断开，再连接收到同一消息 ID；确认后数据库只有一条且状态为 `delivered`。
- `TestDelayedACKCausesRetry`：故意延迟 ACK，约 5 秒后观察到同一消息 ID 再投递；数据库 `delivery_attempts >= 1`。
- `TestOfflineSyncAcrossBatches`：发送 120 条离线消息，接收者跨多个 32 条批次收齐并逐条 ACK；数据库确认 120 条均为 `delivered`。首版 100 条重试批量曾使 64 帧队列满并断开连接；将单批降至 32 条后测试通过。这是实际发现并修复的背压问题，不是性能基准。

上述证明单实例的数据库状态与重试行为；多实例、Redis/Kafka 故障和正式压测仍未验证。帧允许重复，客户端必须按 `message.id` 去重。

## V1.0 迭代：Redis 在线路由（2026-09-27）

`internal/presence` 使用 go-redis，单元测试在 `miniredis`（纯 Go Redis 协议模拟器）中验证多实例成员、30 秒过期过滤、移除与连接失败显式报错；WebSocket 测试验证认证连接写入路由，以及 Redis 模拟器关闭后本地 Hub 连接仍存在。`go test -count=1 ./internal/presence ./internal/websocket` 通过。

加入 Redis 后再次运行带真实 PostgreSQL 端到端环境的 `go test -count=1 ./...`、`go vet ./...` 和 `go mod verify`，均通过。Compose YAML 可解析为 4 个服务；这只验证语法，不代表 Docker Compose 已启动。

**真实 Redis Server 未验证**：当前 Windows 环境没有 Docker/Redis 服务。Compose 已加入 Redis 服务，但没有声称它已实际启动或验证跨实例路由。

## V1.0 迭代：Kafka outbox 与双实例恢复路径（2026-09-27）

对真实 PostgreSQL 18.3 执行迁移 `011`–`014`，SQL 触发器将消息与 outbox 行在同一事务保存。`TestOutboxAtomicPublishAndRetry` 用可注入的发布函数模拟 Broker 错误：错误返回后 `published_at` 保持空值，下一次成功发布后该字段落库；测试通过。此测试验证数据库事务边界，**不是实际 Kafka 发布**。

`TestCrossInstanceConsumerDelivery` 在同一测试进程内启动两个独立 HTTP/WebSocket 服务器与 Hub，共用真实 PostgreSQL 和 `miniredis`；将事件注入 Kafka 消费处理边界后，另一实例连接收到相同数据库消息 ID。重复注入会重复产生帧，但数据库仍只有一条逻辑消息并保持已确认状态；测试通过。**不是实际 Broker 或独立进程的消费测试**。

`TestTwoProcessesDatabaseFallbackAndReconnect` 启动两个真实服务操作系统进程，分别连接真实 PostgreSQL 与 `miniredis`。A 连进程 1，B 连进程 2；A 发消息后，B 由数据库重试循环收到并 ACK。随后强制结束进程 2，A 再发消息，B 重连进程 1 并同步同一条未确认消息；Redis 模拟器中的 B 路由出现进程 1，PostgreSQL 核查两条消息记录。测试通过。首次使用 5 秒读取超时失败：新消息的 `next_delivery_at` 初值是写入后 5 秒，轮询可能再等约 2 秒；改用 10 秒观察窗口后通过。这个测试**未启用 Kafka**，只验证数据库补发跨进程恢复路径。进程 2 的旧 Redis 路由需靠 30 秒心跳过期过滤，测试未等待该窗口。

新二进制运行在本地 `127.0.0.1:18080`、未设置 Redis/Kafka 的兼容模式下，`go test -count=1 ./...`、`go vet ./...`、`go mod verify` 均通过；此前第一阶段端到端测试仍通过。Compose YAML 静态解析识别 7 个服务，但本机无 Docker，因此 **真实 Kafka、真实 Redis、Compose 启动和 Kafka 多实例链路均未验证**。没有生成或宣称任何吞吐、延迟、CPU、内存数据。

## V1.0 迭代：慢消费者（2026-09-27）

`TestFullSendQueueClosesSlowConnectionOnce` 使用真实 WebSocket 连接，但测试服务故意不运行写协程，使 1 帧测试队列确定性填满；第二次投递关闭连接，重复投递后慢消费者断开计数仍为 1，客户端读到连接关闭。测试通过。生产连接队列仍固定 64 帧；尚未对真实慢网络、长时间连接数或内存上限做压测。

## V1.0 迭代：连接生命周期（2026-09-27）

`TestDuplicateLoginAndUnexpectedDisconnect` 使用真实 WebSocket 测试同一用户同时建两条连接，第一条直接关闭后 Hub 保留第二条，第二条关闭后 Hub 不再保留该用户。配合已有 `TestCloseAllReleasesAuthenticatedConnection` 覆盖服务主动关闭。Redis 脏路由仍按 30 秒时间窗过滤，未宣称瞬时离线精确性。

## V1.0 迭代：限流与基础安全（2026-09-27）

`TestTokenBucketRefillAndIsolation` 验证突发、按时间补充令牌和不同 key 隔离；`TestLimitIPIgnoresUntrustedForwardedFor` 验证同一直接来源改写转发头后仍收到 HTTP 429；`TestWebSocketRateLimitAndSenderSpoofRejected` 使用真实 PostgreSQL + WebSocket 验证伪造字段被拒、单连接超额返回 `rate_limited` 且仅保存一条消息；`TestHistoryAuthorization` 验证匿名查询 401、第三方 JWT 不能通过请求参数读取其他人已保存的会话。上述测试均通过。生产默认额度与多实例全局效果尚未压测。

## V1.0 迭代：基础群聊（2026-09-27 至 2026-09-28）

在同一真实 PostgreSQL 18.3 上执行迁移 `015`–`023`；`TestGroupMembershipFanoutHistoryAndIdempotency` 通过新 Go 服务的真实 HTTP/WebSocket 路径验证建群、三用户加入、群主移除成员、非群主移除被拒、非成员发消息及查历史被拒、两个在线成员收到同一群消息、重复 UUID 只保留一行、`group_message_outbox` 有且仅有一行、成员退群后不能查历史、群主不能直接退群。`TestCrossInstanceConsumerDelivery` 追加群事件消费边界注入，第二个本地 Hub 收到群消息；该测试共用真实 PostgreSQL 和模拟 Redis，**并非真实 Kafka Broker**。

`TestConcurrentGroupJoinRespectsHundredMemberCap` 使用真实 PostgreSQL 创建群主与 105 个候选用户，并发加入后恰好 99 人成功、6 人得到 `group_full`，连同群主正好 100 人；测试通过。这只验证成员上限，不是 100 人同时在线扇出压测。

2026-09-28 重测时本地临时 PostgreSQL 进程已停止，第一次运行报连接拒绝；重新启动原数据目录后 `TestCrossInstanceConsumerDelivery` 通过。该环境中断不计为群功能通过或性能结果。随后在新服务二进制、真实 PostgreSQL 环境运行 `go test -count=1 ./...`、`go vet ./...`、`go mod verify`，全部通过。真实 Kafka 群扇出及大量离线成员尚未做实测。

## V1.0 迭代：指标与面板配置（2026-09-29）

在本地真实 Go 进程与 PostgreSQL 上实际访问 `GET /metrics`，初次抓取显示 `im_websocket_connections 0`、`im_websocket_send_queue_frames 0`、`im_kafka_producer_up 0`、`im_kafka_consumer_up 0`、`go_goroutines 11`；Kafka 相关配置未启用，0 不代表已验证 Broker 故障。随后运行完整端到端测试，第二次实际抓取显示 `im_messages_total{kind="direct",result="created"} 168`、`im_delivery_confirmed_total 124`、`im_ack_latency_seconds_count 124`、`im_db_operation_seconds_count{result="ok"} 625`，连接和队列深度在测试结束后均为 0。这些只是本次混合功能测试的**累计计数**，不是 QPS、吞吐、延迟分位或容量基准。

`TestMetricsExpositionUsesActualObservations` 验证 Registry 导出的计数器、Gauge 和 Redis 操作直方图；新的服务二进制下 `go test -count=1 ./...`、`go vet ./...`、`go mod verify` 均通过。Compose、Prometheus、Grafana provisioning YAML 通过静态解析，Grafana Dashboard JSON 可解析且包含 10 个面板。`scripts/init-env.ps1` 对已有 `.env` 补充随机 Grafana 密码，再次运行未产生重复条目。当前无 Docker，因此 **Prometheus 抓取、Grafana 面板显示、容器启动和截图未验证**。

## V1.0 迭代：Docker 开发环境静态检查（2026-09-29）

对照 Apache Kafka 官方单节点 KRaft 配置，补充 Kafka 数据卷、固定开发集群 ID、日志目录与单节点内部 Topic 复制因子；Compose YAML 重新解析得到 9 个服务。`scripts/verify-compose.ps1` 提供有 Docker 时的配置校验、启动、健康探测和 Topic 核查入口，PowerShell 语法解析为 0 个错误。本机 `docker` 与 `docker-compose` 命令均不存在，**没有执行 `docker compose config`、镜像构建、容器启动或脚本运行**；这些仍需安装并启动 Docker Desktop 后验证。

## V1.0 迭代：跨实例送达状态与并发检查（2026-09-29）

在现有真实 PostgreSQL 18.3 实例上执行迁移 `024`–`027`。`TestDeliveryAcknowledgmentAndDuplicateACK` 验证首次 ACK 产生一条 `delivery_status_outbox` 行、重复 ACK 不增加行数；`TestDeliveryStatusOutboxPublishAndRetry` 模拟 Kafka 发布失败后确认 outbox 仍待处理，成功后确认标记已发布；`TestCrossInstanceConsumerDelivery` 以两个独立 Hub 和真实数据库、模拟消费边界验证 ACK 实例与发送者实例不同仍收到 `message.status`。`TestValidEventTargets` 验证三种事件类型及混合目标拒绝。完整 `go test -count=1 ./...` 与 `go vet ./...` 均通过。这里的发布与消费是注入函数和模拟边界，**真实 Kafka Broker 未验证**。

尝试 `go test -race ./internal/websocket ./internal/messaging`：默认因 CGO 关闭失败；显式设置 `CGO_ENABLED=1` 后因本机缺少 `gcc` C 编译器失败。没有得到 race detector 的通过结论；安装兼容 Go 的 C 编译器并确认 PATH 后需重跑 `go test -race ./...`。

## V1.0 迭代：压测、Profiling 与故障恢复（2026-09-29）

独立 `cmd/loadtest` 经过 10 连接冒烟测试，并以真实 HTTP 用户和 WebSocket 连接运行 100、500、1000、2000、5000 连接组。各组建立的连接数均达到请求数，发送与首次送达数一致，0 个工具计数失败与意外断连；完整参数、P50/P95/P99、服务端 CPU/RSS/goroutine 和原始 JSON 见 [BENCHMARK.md](BENCHMARK.md) 与 `benchmarks/results/`。这些结果是本机回环、未启用 Kafka/Redis 的固定速率测试，不是最大吞吐。另运行 100 连接、70 秒长连接测试，349/349 首次送达，0 意外断连；关闭连接后 `/metrics` 为连接 0、队列 0、goroutine 11。

在 500 连接、300 条/秒负载期间实际采集 10 秒 CPU Profile、Heap 与 Goroutine Profile；CPU 样本主要落在 Windows `WSASend`。尝试共享写缓冲池与 1 KiB 读写缓冲后，5000 连接同负载单次比较峰值 RSS 从 246.6 MiB 变为 234.3 MiB，P95/P99 从 0.731/1.108 ms 变为 1.043/2.674 ms；因此只记录内存与延迟取舍，不声称总体提速或显著性结论。

新增 `TestDatabaseConnectionOutageAndRecovery`：以 TCP 代理断开真实服务与 PostgreSQL 的网络连接，实际观察 `internal_error`、库中 0 行；恢复连接并复用 UUID 后，接收者收到消息、库中 1 行。目标测试通过。真实 PostgreSQL 进程宕机和主从恢复未验证。`TestRealKafkaCrossInstanceDeliveryAndStatus` 已编写，但当前无 Docker/真实 Broker，缺少 `INTEGRATION_SECOND_BASE_URL` 时明确跳过；**不能记为真实 Kafka 通过**。

在最终 1 KiB 缓冲版本和真实 PostgreSQL 上重跑 `go test -count=1 ./...`，包含原有与新故障测试，全部通过；接下来仍需在 Docker 环境执行真实 Kafka/Redis/Compose、安装 `gcc` 后执行 race detector。

## V1.0 最终验收补充：Race Detector（2026-09-29）

本机安装 MSYS2 UCRT GCC 16.2.0。Go 1.27.1 测试进程直接启动 GCC 时缺少其运行库搜索路径，`runtime/cgo` 在编译阶段以状态 2 退出；仓库外的临时 `gcc-wrapper.cmd` 在子进程内补齐 MSYS2 UCRT `bin` 路径后，执行 `CGO_ENABLED=1 go test -race -count=1 ./...` 成功。测试环境同时设置真实 PostgreSQL 的 `DATABASE_URL`、运行中的本地 Go 服务 `INTEGRATION_BASE_URL`、测试服务二进制和测试用 JWT 密钥，覆盖 `tests` 包的真实端到端路径。所有包通过，`tests` 包耗时 26.511 秒；没有 data race 报告。`go vet ./...` 同次检查退出码 0。真实 Kafka 双实例测试仍因缺少第二个运行中的实例而跳过，不能由本次 race PASS 推断其已验证。

本机随后安装 Docker Desktop 当前用户版与 Microsoft WSL 2.7.14.0；`docker compose --env-file .env -f deployments/compose.yaml --profile multi config --quiet` 退出码 0，列出 9 个服务。`docker info` 明确报 Docker Engine 命名管道不存在；Windows 功能启用后仍待用户重启并在 Docker Desktop GUI 接受协议。因此 `docker compose up/ps/logs`、真实 Redis/Kafka、Prometheus/Grafana 均尚未运行。本段取代上文各阶段“本机无 Docker CLI”的历史环境描述。

实际执行 `docker compose --env-file .env -f deployments/compose.yaml --profile multi up --build -d` 和同配置的 `ps`，两者均以状态 1 退出，错误为 `failed to connect to the docker API at npipe:////./pipe/docker_engine`，命名管道不存在；没有任何容器被这两条命令启动。这是环境启动失败，不能算 Compose 验收通过。
