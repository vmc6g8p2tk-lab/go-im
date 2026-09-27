# Go 即时通信系统架构

## 1. 系统目标

这是一个供校招/实习技术面试展示、可逐阶段扩展并可真实压测的即时通信项目。第一阶段先验证从身份认证、WebSocket 收发到 PostgreSQL 持久化及历史查询的完整链路。所有性能结论必须附真实环境、方法和原始测试结果；目前不声明任何吞吐量或延迟指标。

## 2. 第一阶段架构

```text
HTTP 客户端 ── 注册/登录/历史查询 ──┐
                                  ├─ Go API ── PostgreSQL
WebSocket 客户端 ── 认证/单聊 ──────┘    │
                                    └─ 进程内连接 Hub
```

单实例 Go 服务承担 HTTP、JWT 验证、WebSocket 连接管理和消息写入。只有数据库写入成功后才向在线连接推送消息；同一用户可有多个连接。此阶段的在线推送只覆盖本实例，离线用户通过历史 API 获取持久化消息。

## 3. 后续完整架构（目标，尚未实现）

```text
客户端 → 负载均衡 → 多个 Go 网关/业务实例 → PostgreSQL
                              │       │
                              │       ├─ Redis：在线路由、限流、热点数据
                              └─ Kafka：跨实例投递和异步事件
Prometheus ← 指标端点；Grafana ← Prometheus
```

后续要先定义投递语义、幂等键、ACK 与重试状态，再引入 Kafka。不能仅靠加入队列宣称“可靠消息”或“高并发”。

## 4. 组件职责与目录

| 组件 | 职责 |
| --- | --- |
| `cmd/server` | 进程入口、配置加载、依赖装配、优雅退出 |
| `internal/config` | 环境变量校验 |
| `internal/model` | 领域数据结构 |
| `internal/repository` | 参数化 SQL 与数据库访问 |
| `internal/service` | 注册、登录、消息业务规则 |
| `internal/handler` | HTTP 参数解析、状态码和 JSON 响应 |
| `internal/middleware` | HTTP Bearer JWT 鉴权 |
| `internal/websocket` | 连接认证、读写循环、进程内 Hub |
| `internal/auth` | 密码散列与 JWT 签发/验证 |
| `migrations` | 纳入 Git 的有序 SQL 迁移 |
| `deployments` | Docker Compose 与容器镜像 |
| `tests` | 从外部调用真实服务的集成验证 |

Redis、Kafka、监控的代码暂不加入，避免让未使用组件污染第一阶段链路。

## 5. 数据流

1. 注册：规范化用户名 → 校验输入 → bcrypt 散列密码 → 参数化 SQL 插入用户。
2. 登录：按用户名读取散列 → bcrypt 验证 → 签发限时 HS256 JWT。
3. 建连：客户端连接 `/v1/ws`，在限定时间内发送 `auth` 帧；服务端验证 JWT 后把连接加入 Hub。
4. 发消息：服务端校验目标用户和正文 → 写入 PostgreSQL → 生成包含数据库 ID 的 `message.new` → 投递至发送者及目标用户在本实例的在线连接。
5. 查历史：Bearer JWT 认证 → 按双方用户 ID、消息 ID 游标查询 → 返回按时间正序的记录。

成功写库与实时投递之间仍有故障窗口。第一阶段不保证实时帧必达，历史记录是当前阶段的恢复入口。

## 6. PostgreSQL 表设计

### `users`

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `id` | `BIGINT GENERATED ALWAYS AS IDENTITY` | 主键 |
| `username` | `VARCHAR(32)` | 非空、唯一；应用层转小写，仅允许 ASCII 字母、数字、下划线 |
| `password_hash` | `TEXT` | 非空，仅存 bcrypt 散列 |
| `created_at` | `TIMESTAMPTZ` | 非空，默认 `now()` |

### `messages`

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `id` | `BIGINT GENERATED ALWAYS AS IDENTITY` | 主键，也是历史游标 |
| `sender_id` | `BIGINT` | 非空，外键 `users(id)` |
| `recipient_id` | `BIGINT` | 非空，外键 `users(id)` |
| `body` | `TEXT` | 非空，长度由应用和数据库约束 |
| `created_at` | `TIMESTAMPTZ` | 非空，默认 `now()` |

双向历史查询使用 `(sender_id, recipient_id, id DESC)` 索引；当前 SQL 合并两个方向并按 `id DESC` 截取，再正序返回。生产规模需要依据 `EXPLAIN ANALYZE` 和真实数据分布评估会话键、分区等方案。`id` 的顺序是数据库分配顺序，不承诺严格的客户端发送时间顺序。

## 7. Redis Key 设计（后续阶段；本阶段不连接 Redis）

| Key | 类型 | 值/用途 | TTL |
| --- | --- | --- | --- |
| `im:presence:{user_id}` | Hash | `instance_id → 最近心跳时间`，多设备/多实例在线路由 | 心跳更新，短 TTL |
| `im:rate:user:{user_id}` | String/计数器 | 每用户发送限流 | 窗口 TTL |
| `im:session:revoked:{jti}` | String | 需立即撤销的 token 标记 | 至 JWT 到期 |

Key 不存消息唯一真相。在线状态依赖心跳与过期机制，不能把 Redis Key 存在等同于“用户一定能收到消息”。上线前需明确隐私边界、原子操作和故障降级方式。

## 8. WebSocket 消息协议

端点：`GET /v1/ws`，成功升级后仅收发 UTF-8 JSON 文本帧。单帧最大 8 KiB；鉴权超时 5 秒。JWT 放在首帧，避免 URL 查询串泄漏。生产环境必须使用 WSS。

```json
{"type":"auth","token":"<JWT>"}
{"type":"auth.ok","user_id":1}
{"type":"message.send","recipient_id":2,"body":"你好"}
{"type":"message.new","message":{"id":7,"sender_id":1,"recipient_id":2,"body":"你好","created_at":"2026-09-27T08:00:00Z"}}
{"type":"error","code":"invalid_message","message":"..."}
```

`message.new` 同时回显给发送者并发送给接收者的本实例连接。发送者以数据库 `id` 对账。连接写操作由单独的 writer goroutine 串行执行，待发队列有界；慢连接被关闭以保护进程内存。服务端定期 Ping，客户端须回应 Pong。第一阶段不定义 ACK、自动重试或端到端去重；客户端重发可能产生重复记录。

## 9. HTTP API 设计

统一使用 `application/json`；错误格式为 `{"error":{"code":"...","message":"..."}}`。

| 方法与路径 | 请求 | 成功响应 | 说明 |
| --- | --- | --- | --- |
| `POST /v1/auth/register` | `{"username":"alice","password":"..."}` | `201 {"user":{"id":1,"username":"alice"}}` | 用户名冲突返回 409 |
| `POST /v1/auth/login` | 同上 | `200 {"token":"...","expires_at":"...","user":{...}}` | 错误凭据返回 401 |
| `GET /v1/messages?peer_id=2&limit=50&before_id=100` | Bearer JWT | `200 {"messages":[...]}` | `before_id` 可省；`limit` 默认 50、最多 100 |
| `GET /healthz` | 无 | `200 {"status":"ok"}` | 检查数据库连通性 |
| `GET /v1/ws` | WebSocket 首帧认证 | WebSocket 协议 | `Origin` 按配置白名单校验 |

## 10. 当前阶段的技术取舍

- 使用 Go 标准库 HTTP 服务、`pgx`、`gorilla/websocket`、`golang-jwt/jwt/v5` 和 bcrypt；避免第一阶段引入框架与中间件过多。
- 环境变量提供连接串、JWT 密钥、监听地址和允许的 WebSocket Origin。密钥不进入仓库；`.env.example` 仅给占位符。
- 使用 SQL 迁移文件和独立迁移命令。数据库约束与服务层校验同时存在。
- bcrypt 是方便验证的安全起点，但计算成本会影响登录吞吐；未来根据压测及安全要求评估 Argon2id。
- 单实例 Hub 不跨实例同步。第一阶段的“在线”仅表示当前进程内连接。
- Redis 虽完成 Key 设计，第一阶段 Compose 默认只启动必需的 PostgreSQL 和应用，降低验证链路复杂度。

## 11. 后续扩展方案

1. 定义会话 ID、客户端消息 ID、唯一约束和幂等行为；补充 ACK、重试、离线同步。
2. 引入 Redis 管理带过期的在线路由和限流，测试 Redis 故障行为。
3. 引入 Kafka 及 outbox/事务边界设计，明确数据库提交与事件发布的一致性。
4. 扩展群聊、成员权限和扇出策略；用真实数据测试热点群场景。
5. 先建立可复现的压测环境和原始数据，再添加 Prometheus/Grafana，记录吞吐、P95/P99、CPU、内存、错误率及测试配置。
6. 多实例部署和故障演练，验证断线恢复、重复投递、消息顺序与资源上限。
