# Go Distributed IM System

技术栈：**Go · WebSocket · PostgreSQL · Redis · Kafka · Prometheus · Grafana · Docker Compose**。

核心能力：Reliable Messaging、Idempotency、ACK、Offline Delivery、Multi-instance Kafka Delivery、Transactional Outbox、Backpressure、Rate Limiting、Metrics、Load Testing、Failure Recovery。Redis 记录可过期的在线实例提示，当前投递路径由每实例 Kafka 消费组广播后交给本地 Hub，并未使用 Redis 定向选路。

本项目按阶段构建可解释、可验证的即时通信系统。注册、登录、JWT、单聊、群聊、消息持久化、幂等、ACK 与离线补发已在真实 Go 服务和 PostgreSQL 上测试。Docker Compose 的两台 IM、Redis、Kafka、Prometheus、Grafana 已实际启动；跨 Kafka 消息与 ACK、Redis/Kafka 故障恢复、10000 低消息率连接均有真实测试记录。最初完整栈 500 msg/s 目标负载出现秒级尾延迟与 Outbox 积压；经容器内 pprof、SQL 与逐秒 Kafka/数据库指标定位后，批量 Outbox 发布在相同 10 秒负载下两次约 497.8/s 首次送达、P95 56/58 ms，另以同速率 30 秒检查约 498.2/s、P95 58 ms，Outbox 无持续增长。**V1.0 可以在本机已测范围封版**，不宣称生产能力或最大稳态吞吐。`go test -race -count=1 ./...` 与真实 Kafka 双实例定向 race 测试通过。原始压测见 [BENCHMARK.md](BENCHMARK.md)，完整调查与失败记录见 [PERFORMANCE_INVESTIGATION.md](PERFORMANCE_INVESTIGATION.md)，最终边界见 [FINAL_VALIDATION.md](FINAL_VALIDATION.md)。[架构设计](ARCHITECTURE.md)、[学习记录](LEARNING.md)、[故障测试](FAILURE_TEST.md)与 [50 道面试题](INTERVIEW.md)记录实现依据和边界。

## 技术栈与架构

Go、HTTP REST、WebSocket、PostgreSQL、Redis、Kafka、Docker Compose、Prometheus、Grafana。数据库保存消息与 outbox 作为可靠状态；Redis 保存可过期的在线实例提示；Kafka 用于跨实例事件通知。下图对应已在本机 Compose 中启动的组件；Prometheus 当前只抓 app1。

```mermaid
flowchart LR
    Client[客户端] -->|HTTP + WebSocket| A[IM 实例 1]
    Client -->|HTTP + WebSocket| B[IM 实例 2]
    A <--> PG[(PostgreSQL 消息与 outbox)]
    B <--> PG
    A <--> R[(Redis 在线提示)]
    B <--> R
    A <--> K[(Kafka 事件 Topic)]
    B <--> K
    P[Prometheus] -->|抓取 /metrics| A
    P --> G[Grafana 面板]
```

## 环境与启动

- Go 1.26+（本项目使用 Go Modules）
- Docker 与 Docker Compose

在仓库根目录运行：

```powershell
pwsh -File scripts/init-env.ps1
pwsh -File scripts/verify-compose.ps1 -Multi
```

`init-env.ps1` 生成随机开发密钥；已有 `.env` 时只补缺失的 Grafana 密码。该文件已被 Git 忽略。PostgreSQL 数据保存在 Compose volume 中。迁移容器先执行 `migrations/*.sql`，成功后应用才启动；不要手动修改生产表结构。停止服务用 `docker compose --env-file .env -f deployments/compose.yaml down`，此命令保留数据卷。

若 5432/8080 已被占用，需调整 Compose 端口映射；若改动数据库密码但复用旧 volume，旧数据库密码不会自动变化。测试环境可先明确备份后清理 volume 再初始化。

`verify-compose.ps1` 会检查 Compose 配置、启动服务、等待 HTTP/指标/Prometheus/Grafana 就绪并核查 Kafka Topic；加 `-Multi` 会启用第二个 IM 实例。脚本不会删除数据卷。2026-09-29 已通过实际 `docker compose --env-file .env -f deployments/compose.yaml --profile multi up --build -d` 启动完整环境并核查服务、Topic、Redis、监控和跨实例链路；执行细节见 [FINAL_VALIDATION.md](FINAL_VALIDATION.md)。

## 接口快速体验

```powershell
$alice = Invoke-RestMethod -Method Post http://localhost:8080/v1/auth/register -ContentType 'application/json' -Body '{"username":"alice","password":"password123"}'
$bob = Invoke-RestMethod -Method Post http://localhost:8080/v1/auth/register -ContentType 'application/json' -Body '{"username":"bob","password":"password123"}'
$login = Invoke-RestMethod -Method Post http://localhost:8080/v1/auth/login -ContentType 'application/json' -Body '{"username":"alice","password":"password123"}'
Invoke-RestMethod -Headers @{Authorization="Bearer $($login.token)"} "http://localhost:8080/v1/messages?peer_id=$($bob.user.id)"
```

WebSocket 地址是 `ws://localhost:8080/v1/ws`。升级后首帧发送 `{"type":"auth","token":"登录返回的 JWT"}`，收到 `auth.ok` 才发送 `{"type":"message.send","recipient_id":2,"client_message_id":"0c0dcce8-7a12-4f9d-8827-0527722092b1","body":"你好"}`。客户端对同一条逻辑消息的重试必须复用相同 UUID。完整协议和错误格式见 [ARCHITECTURE.md](ARCHITECTURE.md)。本地可用 `wscat` 或下面的端到端测试自动连接两个用户。

传入 `client_message_id` 的发送者收到 `server_ack`，它只表示消息已落库；接收者收到 `message.new` 后发送 `{"type":"delivery.ack","message_id":7}`，数据库状态才进入 `delivered`。旧客户端不传 ID 时保留第一阶段的发送者 `message.new` 回显。离线或 ACK 丢失会使同一消息帧重现，客户端需按服务器消息 ID 去重。重试批量与边界见架构文档。

基础群聊通过 `POST /v1/groups` 建群，`POST /v1/groups/{id}/join` 加入；WebSocket 发送 `{"type":"group.send","group_id":3,"client_message_id":"UUID","body":"大家好"}`。群消息只入库一行，在线成员收到 `group.message.new`；群历史和成员操作见 [ARCHITECTURE.md](ARCHITECTURE.md)。V1.0 限制每群最多 100 人，离线群消息通过历史 API 查询，暂无逐成员群 ACK。

## 监控

Go 服务在 `GET /metrics` 暴露 Prometheus 指标；本地 Compose 配置 Prometheus `http://localhost:9090` 和 Grafana `http://localhost:3000`。Grafana 管理员密码由 `.env` 中的 `GRAFANA_ADMIN_PASSWORD` 提供，面板配置在 [im-overview.json](deployments/grafana/dashboards/im-overview.json)。当前抓取配置监控默认 `app` 实例；启用 `multi` profile 时，应在 [prometheus.yml](deployments/prometheus.yml) 增加 `app2:8080` 目标。Grafana 截图应在真实容器启动并产生测试流量后保存至 `docs/screenshots/`；仓库现在没有监控截图或预填面板数据。`/metrics` 应只在受信网络开放。

`PPROF_ADDR=127.0.0.1:6060` 可选开启独立回环端口的 CPU、Heap、Goroutine Profile。不要将此端口转发到公网；实际采样与热点见 [BENCHMARK.md](BENCHMARK.md)。

## 测试与数据库核查

```powershell
go test ./...
$env:INTEGRATION_BASE_URL='http://localhost:8080'
$env:DATABASE_URL='postgres://im:<.env 中的 POSTGRES_PASSWORD>@localhost:5432/im?sslmode=disable'
go test -v ./tests
docker compose --env-file .env -f deployments/compose.yaml exec postgres psql -U im -d im -c 'SELECT id, sender_id, recipient_id, body, created_at FROM messages ORDER BY id DESC LIMIT 10;'
```

`tests/e2e_test.go` 使用真实 HTTP、两个 WebSocket 连接和 PostgreSQL，验证双向消息、落库及历史查询。`INTEGRATION_SERVER_BINARY` 与 `INTEGRATION_JWT_SECRET` 还可启用双进程故障恢复测试。未设置所需环境变量时测试会明确跳过，不可将跳过写成通过。实际执行记录见 [TEST_RESULTS.md](TEST_RESULTS.md)。

真实 Kafka 双实例链路可在 Compose 启动后设置 `INTEGRATION_SECOND_BASE_URL=http://localhost:8081`，并提供两个服务共用的 `DATABASE_URL` 与 `INTEGRATION_BASE_URL`，执行 `go test -run TestRealKafkaCrossInstanceDeliveryAndStatus -v ./tests`。此测试已在稳定的真实 Broker 上通过；app2 重启后立即运行时曾因消费组重平衡而走数据库补发，详情见 [FAILURE_TEST.md](FAILURE_TEST.md)。

独立压测工具：

```powershell
go build -o loadtest.exe ./cmd/loadtest
./loadtest.exe -base http://127.0.0.1:8080 -connections 100 -users 10 -rate 50 -duration 15s -output result.json
```

详细负载模型、原始结果、pprof 和优化前后实测见 [BENCHMARK.md](BENCHMARK.md)。压测会创建用户与数据库消息，请使用测试数据库。

## 目录

```text
cmd/server             入口、依赖装配、退出
cmd/loadtest           独立 WebSocket 压测工具
internal/auth          密码散列、JWT
internal/config        环境配置
internal/handler       HTTP 接口
internal/middleware    Bearer 鉴权
internal/ratelimit     进程内令牌桶
internal/migration     迁移执行器
internal/messaging     Kafka outbox 发布与消费
internal/metrics       Prometheus 指标与数据库查询追踪
internal/model         数据结构
internal/presence      Redis 在线实例提示与 TTL
internal/repository    PostgreSQL SQL
internal/service       业务规则
internal/websocket     Hub、连接读写循环
migrations             已版本化的 SQL
deployments            Dockerfile、Compose
benchmarks/results     本机真实测试的原始 JSON 汇总
scripts                开发密钥初始化
tests                  真实服务端到端测试
```

## 已知边界

- Hub 只管理本实例连接；Kafka 每实例独立消费组传播消息事件，消费者向本地 Hub 投递。PostgreSQL 中的未确认消息可由在线重试循环和重连同步补发。真实跨 Kafka 消息与 ACK 路径已通过，但 Redis Presence 当前不参与定向投递。
- 跨实例送达状态使用 PostgreSQL `delivery_status_outbox` 和 Kafka 事件通知发送者；真实 Broker 稳定状态路径和 Broker 停止后的数据库补发均已验证。
- 已实现送达 ACK 和重试，但客户端仍需去重；旧客户端不传 ID 时重发仍可能重复写库。已读 ACK 尚未实现。
- JWT 在到期前不可主动吊销；生产环境需要 TLS/WSS、密钥管理与限流。
- `go.mod` 暂用 `github.com/example/go-im`，发布到你的 GitHub 仓库前应替换为实际仓库路径并更新 Go import。
- 历史查询分页按数据库消息 ID，而非全局严格时间顺序。

Kafka/outbox 代码可用 `KAFKA_BROKERS` 启用；未设置时沿用单实例即时投递。`docker compose --profile multi --env-file .env -f deployments/compose.yaml up --build -d` 配置第二个服务实例（8081），已在本机 Docker Desktop 实际运行。每实例独立消费组使 Kafka 读取成本随实例数增加。群离线消息目前通过历史 API 查询，尚无逐成员 ACK。完整栈的性能边界和封版阻碍见 [FINAL_VALIDATION.md](FINAL_VALIDATION.md)。
