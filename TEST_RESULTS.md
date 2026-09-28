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
