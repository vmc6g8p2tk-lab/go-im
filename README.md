# Go 即时通信系统 · 第一阶段

本项目按阶段构建可解释、可验证的即时通信系统。当前只实现注册、登录、JWT、单实例 WebSocket 单聊、PostgreSQL 落库与历史查询。**尚无并发性能结论。** [架构设计](ARCHITECTURE.md)解释目标与后续边界，[学习记录](LEARNING.md)解释关键实现及面试问答。

## 环境与启动

- Go 1.26+（本项目使用 Go Modules）
- Docker 与 Docker Compose

在仓库根目录运行：

```powershell
pwsh -File scripts/init-env.ps1
docker compose --env-file .env -f deployments/compose.yaml up --build -d
docker compose --env-file .env -f deployments/compose.yaml ps
Invoke-RestMethod http://localhost:8080/healthz
```

`init-env.ps1` 只在 `.env` 不存在时生成随机开发密钥，文件已被 Git 忽略。PostgreSQL 数据保存在 Compose volume 中。迁移容器先执行 `migrations/*.sql`，成功后应用才启动；不要手动修改生产表结构。停止服务用 `docker compose --env-file .env -f deployments/compose.yaml down`，此命令保留数据卷。

若 5432/8080 已被占用，需调整 Compose 端口映射；若改动数据库密码但复用旧 volume，旧数据库密码不会自动变化。测试环境可先明确备份后清理 volume 再初始化。

## 接口快速体验

```powershell
$alice = Invoke-RestMethod -Method Post http://localhost:8080/v1/auth/register -ContentType 'application/json' -Body '{"username":"alice","password":"password123"}'
$bob = Invoke-RestMethod -Method Post http://localhost:8080/v1/auth/register -ContentType 'application/json' -Body '{"username":"bob","password":"password123"}'
$login = Invoke-RestMethod -Method Post http://localhost:8080/v1/auth/login -ContentType 'application/json' -Body '{"username":"alice","password":"password123"}'
Invoke-RestMethod -Headers @{Authorization="Bearer $($login.token)"} "http://localhost:8080/v1/messages?peer_id=$($bob.user.id)"
```

WebSocket 地址是 `ws://localhost:8080/v1/ws`。升级后首帧发送 `{"type":"auth","token":"登录返回的 JWT"}`，收到 `auth.ok` 才发送 `{"type":"message.send","recipient_id":2,"body":"你好"}`。两个用户都保持连接时，双方会收到同一 `message.new`，其中的 `message.id` 是已落库的 ID。完整协议和错误格式见 [ARCHITECTURE.md](ARCHITECTURE.md)。本地可用 `wscat` 或下面的端到端测试自动连接两个用户。

## 测试与数据库核查

```powershell
go test ./...
$env:INTEGRATION_BASE_URL='http://localhost:8080'
$env:DATABASE_URL='postgres://im:<.env 中的 POSTGRES_PASSWORD>@localhost:5432/im?sslmode=disable'
go test -v ./tests
docker compose --env-file .env -f deployments/compose.yaml exec postgres psql -U im -d im -c 'SELECT id, sender_id, recipient_id, body, created_at FROM messages ORDER BY id DESC LIMIT 10;'
```

`tests/e2e_test.go` 使用真实 HTTP、两个 WebSocket 连接和 PostgreSQL，验证双向消息、落库及历史查询。未设置两个测试环境变量时它会明确跳过，不可将跳过写成通过。实际执行记录见 [TEST_RESULTS.md](TEST_RESULTS.md)。

## 目录

```text
cmd/server             入口、依赖装配、退出
internal/auth          密码散列、JWT
internal/config        环境配置
internal/handler       HTTP 接口
internal/middleware    Bearer 鉴权
internal/migration     迁移执行器
internal/model         数据结构
internal/repository    PostgreSQL SQL
internal/service       业务规则
internal/websocket     Hub、连接读写循环
migrations             已版本化的 SQL
deployments            Dockerfile、Compose
scripts                开发密钥初始化
tests                  真实服务端到端测试
```

## 已知边界

- 单实例 Hub 只能推送本进程内在线用户；离线用户要查询历史。
- 实时帧无 ACK/重试/幂等；发送者断线时可能不知道写库是否成功，重发可能重复。
- JWT 在到期前不可主动吊销；生产环境需要 TLS/WSS、密钥管理与限流。
- `go.mod` 暂用 `github.com/example/go-im`，发布到你的 GitHub 仓库前应替换为实际仓库路径并更新 Go import。
- 历史查询分页按数据库消息 ID，而非全局严格时间顺序。

下一阶段的 Redis、Kafka、群聊和监控仅在架构文档中设计，代码没有提前引入。
