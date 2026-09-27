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
