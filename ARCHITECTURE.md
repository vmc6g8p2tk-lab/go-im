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
| `internal/presence` | Redis 在线实例路由与过期过滤（可选） |
| `migrations` | 纳入 Git 的有序 SQL 迁移 |
| `deployments` | Docker Compose 与容器镜像 |
| `tests` | 从外部调用真实服务的集成验证 |

Redis 在线路由已作为可选组件接入；Kafka 与监控仍未接入。消息的唯一真相仍在 PostgreSQL。

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
| `client_message_id` | `UUID` | 非空；`(sender_id, client_message_id)` 唯一，旧客户端由数据库生成 |
| `delivery_status` | `TEXT` | `created` / `delivered` / 预留 `read`；`delivered` 只由接收方 ACK 写入 |
| `delivered_at` | `TIMESTAMPTZ` | 接收方首次确认时间，重复 ACK 不修改 |
| `delivery_attempts` | `INTEGER` | 待确认消息的同步/重试领取次数 |
| `next_delivery_at` | `TIMESTAMPTZ` | 下次重试时间 |
| `created_at` | `TIMESTAMPTZ` | 非空，默认 `now()` |

双向历史查询使用 `(sender_id, recipient_id, id DESC)` 索引；当前 SQL 合并两个方向并按 `id DESC` 截取，再正序返回。生产规模需要依据 `EXPLAIN ANALYZE` 和真实数据分布评估会话键、分区等方案。`id` 的顺序是数据库分配顺序，不承诺严格的客户端发送时间顺序。

## 7. Redis Key 设计（在线路由已接入；跨实例消费尚未实现）

| Key | 类型 | 值/用途 | TTL |
| --- | --- | --- | --- |
| `im:route:{user_id}` | Sorted Set | member 为 `instance_id`，score 为最后心跳 Unix 毫秒；多个实例可同时在线 | 每 10 秒续期；30 秒后成员视为过期，Key TTL 60 秒 |
| `im:rate:user:{user_id}` | String/计数器 | 每用户发送限流 | 窗口 TTL |
| `im:session:revoked:{jti}` | String | 需立即撤销的 token 标记 | 至 JWT 到期 |

服务连接成功后及每 10 秒按本实例在线用户批量 `ZADD`/`EXPIRE`。查询路由时按时间分数过滤并移除过期成员；整个实例崩溃后最多约 30 秒存在脏路由，且不能把路由存在等同于“用户一定能收到消息”。当前 Redis 不可用时只记录警告，本地单实例聊天和 PostgreSQL 持久化继续；跨实例在线查询和后续远程投递会降级。Key 不存消息唯一真相，`im:rate` 与 `im:session:revoked` 仍是后续设计，尚未实现。

## 8. WebSocket 消息协议

端点：`GET /v1/ws`，成功升级后仅收发 UTF-8 JSON 文本帧。单帧最大 8 KiB；鉴权超时 5 秒。JWT 放在首帧，避免 URL 查询串泄漏。生产环境必须使用 WSS。

```json
{"type":"auth","token":"<JWT>"}
{"type":"auth.ok","user_id":1}
{"type":"message.send","recipient_id":2,"client_message_id":"0c0dcce8-7a12-4f9d-8827-0527722092b1","body":"你好"}
{"type":"message.new","message":{"id":7,"client_message_id":"0c0dcce8-7a12-4f9d-8827-0527722092b1","sender_id":1,"recipient_id":2,"body":"你好","created_at":"2026-09-27T08:00:00Z"}}
{"type":"server_ack","client_message_id":"0c0dcce8-7a12-4f9d-8827-0527722092b1","message_id":7,"status":"persisted","duplicate":false}
{"type":"delivery.ack","message_id":7}
{"type":"delivery.ack.ok","message_id":7,"status":"delivered"}
{"type":"message.status","message_id":7,"status":"delivered","delivered_at":"2026-09-27T08:00:01Z"}
{"type":"error","code":"invalid_message","message":"..."}
```

现代客户端发送后得到 `server_ack`，只表示 PostgreSQL 已保存；旧客户端不传 ID 时仍收到第一阶段的 `message.new` 回显。客户端重发相同 `client_message_id` 和内容时，服务端返回原数据库 ID，不再向接收方主动扇出；重用相同 ID 发送不同内容返回 `idempotency_conflict`。旧客户端不传 ID 时由数据库生成新 UUID，但其重发仍可能创建新消息。客户端应自己生成并持久保存 UUID，直到得到 `server_ack`。接收方可能收到重复 `message.new` 帧，必须以服务器消息 ID 去重并可重复 ACK。连接写操作由单独的 writer goroutine 串行执行，待发队列有界；慢连接被关闭以保护进程内存。服务端定期 Ping，客户端须回应 Pong。

### 已实现的发送幂等

数据库的 `(sender_id, client_message_id)` 唯一索引是跨 goroutine、跨进程、重启后仍有效的仲裁点。插入采用 `ON CONFLICT DO NOTHING`；冲突后读取原消息并比较接收方与正文。只在真正新建时向接收方主动扇出。内存去重无法跨服务重启，也无法阻止两个实例同时写入。这个约束保证**一条逻辑消息记录**，不等于实时帧恰好送达一次。

### 已实现的送达确认与离线补发

状态从 `created` 进入 `delivered` 的唯一条件是经过 JWT 身份验证的接收方发送 `delivery.ack`，服务端持久化 `delivered_at`。WebSocket 写成功、进入待发队列、接收方连接在线都**不等于**接收方确认收到。重复 ACK 返回相同结果且不改首次确认时间；其他用户不能代 ACK。发送者在线时收到 `message.status`，离线时可通过历史 API 查询状态。

未确认消息保留在 PostgreSQL。接收方连接时先领取最多 32 条，再在单个服务器级重试循环中每 2 秒检查本实例在线用户，按数据库 `next_delivery_at` 每 5 秒重试，每批最多 32 条。`FOR UPDATE SKIP LOCKED` 与更新重试时间在同一 SQL 中完成，避免并发扫描在同一重试窗口重复领取。客户端断线重连会再次领取未确认消息，因此这是**至少一次帧投递**，不是恰好一次；客户端按服务器消息 ID 去重。单条消息接收者确认后不再参与重试。发送队列仍固定为 64 帧，慢连接队列满时关闭，后续可从数据库恢复。

当前单服务器重试循环按在线用户串行查询，规模化前需用真实压测评估数据库扫描成本；多实例下还要验证跨实例领取与投递路由。没有客户端 ACK 的旧用户会持续保留 `created`，这是兼容期的明确限制。

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
- Redis 在线路由可通过 `REDIS_ADDR` 启用；Compose 现包含 Redis，但尚未在本机实际验证容器启动。

## 11. 后续扩展方案

1. 客户端消息 ID、ACK、重试和离线同步已经实现；继续补充会话 ID、跨实例投递及更完整的已读语义。
2. Redis 在线路由已实现；继续做真实 Redis 故障验证与限流。
3. 引入 Kafka 及 outbox/事务边界设计，明确数据库提交与事件发布的一致性。
4. 扩展群聊、成员权限和扇出策略；用真实数据测试热点群场景。
5. 先建立可复现的压测环境和原始数据，再添加 Prometheus/Grafana，记录吞吐、P95/P99、CPU、内存、错误率及测试配置。
6. 多实例部署和故障演练，验证断线恢复、重复投递、消息顺序与资源上限。
