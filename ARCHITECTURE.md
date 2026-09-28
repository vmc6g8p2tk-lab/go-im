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

## 3. V1.0 目标架构（部分已实现）

```text
客户端 → 负载均衡 → 多个 Go 网关/业务实例 → PostgreSQL
                              │       │
                              │       ├─ Redis：在线路由、限流、热点数据
                              └─ Kafka：跨实例投递和异步事件
Prometheus ← 指标端点；Grafana ← Prometheus
```

幂等键、ACK、数据库重试与 Kafka outbox 已进入代码。群聊、限流、指标和正式压测仍未完成。真实 Kafka 容器还未运行，不能仅凭代码和模拟测试宣称跨实例 Broker 链路已验证。

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
| `internal/messaging` | Kafka outbox 发布与每实例消费组 |
| `migrations` | 纳入 Git 的有序 SQL 迁移 |
| `deployments` | Docker Compose 与容器镜像 |
| `tests` | 从外部调用真实服务的集成验证 |

Redis 在线路由和 Kafka 发布/消费代码可选接入；监控仍未接入。消息的唯一真相仍在 PostgreSQL。

## 5. 数据流

1. 注册：规范化用户名 → 校验输入 → bcrypt 散列密码 → 参数化 SQL 插入用户。
2. 登录：按用户名读取散列 → bcrypt 验证 → 签发限时 HS256 JWT。
3. 建连：客户端连接 `/v1/ws`，在限定时间内发送 `auth` 帧；服务端验证 JWT 后把连接加入 Hub。
4. 发消息：服务端校验目标用户和正文 → PostgreSQL 同事务写消息与 outbox → 发送者收到 `server_ack` → 发布者把事件发给 Kafka → 每实例消费者仅投递本地连接；Kafka 未配置时保留单实例直接投递。
5. 查历史：Bearer JWT 认证 → 按双方用户 ID、消息 ID 游标查询 → 返回按时间正序的记录。

成功写库后若 Kafka 不可用，outbox 和未确认消息保留在数据库；在线重试与重连同步提供恢复入口。实时帧仍是至少一次语义。

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

## 7. Redis Key 设计

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
{"type":"group.send","group_id":3,"client_message_id":"19d0ef20-a829-43b7-917e-c08707e3e918","body":"大家好"}
{"type":"group.message.new","message":{"id":9,"group_id":3,"sender_id":1,"client_message_id":"19d0ef20-a829-43b7-917e-c08707e3e918","body":"大家好","created_at":"2026-09-27T08:00:02Z"}}
{"type":"error","code":"invalid_message","message":"..."}
```

现代客户端发送后得到 `server_ack`，只表示 PostgreSQL 已保存；旧客户端不传 ID 时仍收到第一阶段的 `message.new` 回显。客户端重发相同 `client_message_id` 和内容时，服务端返回原数据库 ID，不再向接收方主动扇出；重用相同 ID 发送不同内容返回 `idempotency_conflict`。旧客户端不传 ID 时由数据库生成新 UUID，但其重发仍可能创建新消息。客户端应自己生成并持久保存 UUID，直到得到 `server_ack`。接收方可能收到重复 `message.new` 帧，必须以服务器消息 ID 去重并可重复 ACK。连接写操作由单独的 writer goroutine 串行执行，待发队列每连接固定 64 帧；队列满关闭慢连接并只记录一次断开计数，未确认消息从数据库恢复。服务端定期 Ping，客户端须回应 Pong。同一用户可在多个连接登录，当前不踢出旧设备。

### 已实现的发送幂等

数据库的 `(sender_id, client_message_id)` 唯一索引是跨 goroutine、跨进程、重启后仍有效的仲裁点。插入采用 `ON CONFLICT DO NOTHING`；冲突后读取原消息并比较接收方与正文。只在真正新建时向接收方主动扇出。内存去重无法跨服务重启，也无法阻止两个实例同时写入。这个约束保证**一条逻辑消息记录**，不等于实时帧恰好送达一次。

### 已实现的送达确认与离线补发

状态从 `created` 进入 `delivered` 的唯一条件是经过 JWT 身份验证的接收方发送 `delivery.ack`，服务端持久化 `delivered_at`。WebSocket 写成功、进入待发队列、接收方连接在线都**不等于**接收方确认收到。重复 ACK 返回相同结果且不改首次确认时间；其他用户不能代 ACK。发送者在线时收到 `message.status`，离线时可通过历史 API 查询状态。

未确认消息保留在 PostgreSQL。接收方连接时先领取最多 32 条，再在单个服务器级重试循环中每 2 秒检查本实例在线用户，按数据库 `next_delivery_at` 每 5 秒重试，每批最多 32 条。`FOR UPDATE SKIP LOCKED` 与更新重试时间在同一 SQL 中完成，避免并发扫描在同一重试窗口重复领取。客户端断线重连会再次领取未确认消息，因此这是**至少一次帧投递**，不是恰好一次；客户端按服务器消息 ID 去重。单条消息接收者确认后不再参与重试。发送队列仍固定为 64 帧，慢连接队列满时关闭，后续可从数据库恢复。

当前单服务器重试循环按在线用户串行查询，规模化前需用真实压测评估数据库扫描成本；多实例下还要验证跨实例领取与投递路由。没有客户端 ACK 的旧用户会持续保留 `created`，这是兼容期的明确限制。

### Kafka 异步投递与事务 outbox（已实现代码，真实 Broker 未验证）

新消息插入时，PostgreSQL 触发器在同一事务写入 `message_outbox(message_id, recipient_id, created_at, published_at)`；回滚时两行一起回滚。每个实例的发布循环通过 `FOR UPDATE SKIP LOCKED` 领取未发布事件，Kafka 同步写入确认后才设置 `published_at` 并提交。失败则回滚，事件留待重试。进程若在 Kafka 确认后、数据库提交前崩溃，事件可能重复发布。Kafka 不提供整个 PostgreSQL + WebSocket 业务的 exactly once。

Topic `im.message.created.v1` 使用接收者 ID 作为 Partition Key。同一分区内部保留 Kafka 写入顺序；多发布者并发领取、数据库提交时序、客户端重连补发都会使端到端严格会话顺序不能保证。客户端按服务端消息 ID 排序展示和去重。每个实例有独立 Consumer Group `im-delivery-{instance_id}`，收到事件后再次从 PostgreSQL 读取消息，仅投递本地连接，处理成功后提交 offset。这样每个实例都能看到事件，但 Kafka 读取和数据库查询成本随实例数线性增长；大规模方案应考虑按路由转发或专用网关，且需要失效路由的恢复路径。

配置 `KAFKA_BROKERS` 后，新消息不再直接投递本地接收连接，而是由 Kafka 消费链路投递；发送方的 `server_ack` 仍在数据库写入后立刻返回。Kafka 不可用时消息与 outbox 留在 PostgreSQL；在线接收者可被现有数据库重试循环补发，离线接收者下次连接同步。Kafka 消费重放可能生成重复 WebSocket 帧，但不能新建第二条逻辑消息；接收方应按 `message.id` 去重、重复 ACK。跨实例 ACK 的 `message.status` 目前只通知 ACK 所在实例的发送方连接，其他实例的发送方需查历史状态，此缺口待修复。

### 基础群聊（最多 100 人）

`chat_groups` 保存群 ID、群主与名称；`group_members(group_id,user_id)` 是成员唯一约束，角色为 `owner/member`；`group_messages` 保存群 ID、发送者、客户端消息 UUID、正文与时间，`(sender_id,client_message_id)` 唯一索引保证群消息重试幂等；`group_message_outbox` 由数据库触发器随群消息同事务创建。加入群时锁住群行并统计成员，最多 100 人。非成员不能发送或查看群历史；群主不能直接退群，只有群主能移除普通成员。群主转让、群删除、群消息 ACK 和离线主动推送尚未实现，离线成员通过历史 API 查询。

WebSocket `group.send` 入库后返回 `server_ack`；Kafka 未配置时只向本实例在线成员发送 `group.message.new`。配置 Kafka 后，一条群事件由各实例独立消费组接收，各实例查询最多 100 个成员，仅投递本地连接。重复 Kafka 事件可能产生重复帧，客户端按群消息 ID 去重。当前群事件共用 `im.message.created.v1` Topic，Partition Key 为 `g:{group_id}`，直聊使用 `u:{recipient_id}`。真实 Kafka 群扇出尚未验证。

扇出规模取舍：100 人群可在每实例查询成员并对本地连接做有界队列投递；1 万人群不应每实例全量查询/消费，需按在线路由分片、批量扇出并分离在线与离线通知；10 万人群需要频道式拉取、消息游标或专用大群架构，不能为每成员即时写一条投递状态。当前数据库和消费组方案只为 100 人上限设计，未做大群性能声明。

## 9. HTTP API 设计

统一使用 `application/json`；错误格式为 `{"error":{"code":"...","message":"..."}}`。

| 方法与路径 | 请求 | 成功响应 | 说明 |
| --- | --- | --- | --- |
| `POST /v1/auth/register` | `{"username":"alice","password":"..."}` | `201 {"user":{"id":1,"username":"alice"}}` | 用户名冲突返回 409 |
| `POST /v1/auth/login` | 同上 | `200 {"token":"...","expires_at":"...","user":{...}}` | 错误凭据返回 401 |
| `GET /v1/messages?peer_id=2&limit=50&before_id=100` | Bearer JWT | `200 {"messages":[...]}` | `before_id` 可省；`limit` 默认 50、最多 100 |
| `GET /healthz` | 无 | `200 {"status":"ok"}` | 检查数据库连通性 |
| `GET /v1/ws` | WebSocket 首帧认证 | WebSocket 协议 | `Origin` 按配置白名单校验 |
| `POST /v1/groups` | Bearer JWT，`{"name":"..."}` | `201 {"group":...}` | 创建者自动成为群主 |
| `POST /v1/groups/{id}/join` | Bearer JWT | `200 {"status":"joined"}` | 开放加入，最多 100 人 |
| `DELETE /v1/groups/{id}/members/me` | Bearer JWT | `200 {"status":"left"}` | 群主不可直接退出 |
| `DELETE /v1/groups/{id}/members/{user_id}` | 群主 Bearer JWT | `200 {"status":"removed"}` | 移除普通成员 |
| `GET /v1/groups/{id}/members` | 成员 Bearer JWT | `200 {"members":[...]}` | 非成员 403 |
| `GET /v1/groups/{id}/messages?limit=50&before_id=100` | 成员 Bearer JWT | `200 {"messages":[...]}` | 按消息 ID 分页 |

## 10. 当前阶段的技术取舍

- 使用 Go 标准库 HTTP 服务、`pgx`、`gorilla/websocket`、`golang-jwt/jwt/v5` 和 bcrypt；避免第一阶段引入框架与中间件过多。
- 环境变量提供连接串、JWT 密钥、监听地址和允许的 WebSocket Origin。密钥不进入仓库；`.env.example` 仅给占位符。
- 使用 SQL 迁移文件和独立迁移命令。数据库约束与服务层校验同时存在。
- bcrypt 是方便验证的安全起点，但计算成本会影响登录吞吐；未来根据压测及安全要求评估 Argon2id。
- Hub 只管理本实例连接；Kafka 每实例独立消费组负责跨实例事件传播，数据库补发为恢复路径。真实 Kafka 与进程级跨实例链路尚未验证。
- Redis 在线路由可通过 `REDIS_ADDR` 启用；Compose 现包含 Redis，但尚未在本机实际验证容器启动。
- 注册与登录在进程内按直接连接 IP 令牌桶限流（每秒补 2 个、突发 100 个）；WebSocket 文本发送按连接及用户分别每秒补 100 个、突发 200 个。`X-Forwarded-For` 未经可信代理验证时不参与身份判定。此限制只在单进程生效，多实例累计配额会放大，需在网关或 Redis 实现全局限流。JSON 请求/帧与 WebSocket 帧大小均受限；身份始终取 JWT，不信任客户端提供的 `sender_id`。

## 11. 后续扩展方案

1. 客户端消息 ID、ACK、重试和离线同步已经实现；继续补充会话 ID、跨实例投递及更完整的已读语义。
2. Redis 在线路由已实现；继续做真实 Redis 故障验证与限流。
3. Kafka/outbox 代码已实现；在真实 Broker 上验证消费组、重复事件、重启、故障与顺序，再考虑定向分发以降低 O(实例数) 成本。
4. 扩展群聊、成员权限和扇出策略；用真实数据测试热点群场景。
5. 先建立可复现的压测环境和原始数据，再添加 Prometheus/Grafana，记录吞吐、P95/P99、CPU、内存、错误率及测试配置。
6. 多实例部署和故障演练，验证断线恢复、重复投递、消息顺序与资源上限。
