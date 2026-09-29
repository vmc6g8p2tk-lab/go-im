# 故障测试记录

测试时间：2026-09-29。下表只写实际执行过的路径。相关自动化测试在 `tests/` 和 `internal/repository/`。

| 故障 | 注入方式与实际结果 | 边界 |
| --- | --- | --- |
| PostgreSQL 连接中断 | `TestDatabaseConnectionOutageAndRecovery` 在真实 Go 服务与 PostgreSQL 之间放 TCP 代理，主动切断既有连接并拒绝新连接；发送者收到 `internal_error`，直接查询真实库确认该客户端 UUID 的消息为 0 行；恢复代理后复用同一 UUID 重发，接收者收到消息，数据库恰好 1 行。测试通过。 | 模拟网络中断；没有关闭 PostgreSQL 进程，没有测数据库主从切换。 |
| Kafka 发布失败 | `TestOutboxAtomicPublishAndRetry`、`TestDeliveryStatusOutboxPublishAndRetry` 注入发布函数错误，确认原始消息与状态 outbox 保留待发布；再次成功后标记 `published_at`。测试通过。 | 不是实际 Broker 故障、消费者重启或真实消息重平衡。 |
| Redis 连接失败 | `internal/presence` 测试关闭 `miniredis` 后操作返回错误；WebSocket 测试验证本地连接仍可存在。测试通过。 | 使用纯 Go 协议模拟器，真实 Redis Server 暂未验证。 |
| IM 实例退出 | `TestTwoProcessesDatabaseFallbackAndReconnect` 启动两个真实 OS 进程，强制结束接收者实例后，让接收者重连另一个实例；未确认消息由 PostgreSQL 同步，路由更新，库中消息记录核查通过。 | Kafka 未启用；旧 Redis 路由通过 TTL 过期的完整时间窗未等待验证。 |
| ACK 丢失、重复与断线重连 | `TestOfflineReconnectAndLostACK`、`TestDeliveryAcknowledgmentAndDuplicateACK`：断开接收连接而不 ACK，重连收到同一消息 ID；重复 ACK 不改变首次确认时间或新增状态 outbox。测试通过。 | 客户端须按消息 ID 去重，WebSocket 帧不承诺只出现一次。 |
| 慢消费者 | `TestFullSendQueueClosesSlowConnectionOnce` 填满有界队列，连接被关闭，慢消费者计数只增加一次。测试通过。 | 人工固定满队列，未模拟真实慢公网链路。 |
| 长连接心跳与资源释放 | 独立压测工具保持 100 条连接 70 秒，跨过两次 Ping 周期；349 条消息全部首次送达，0 意外断连。结束后实际抓取连接 Gauge 0、队列 Gauge 0、goroutine 11。 | 只覆盖 70 秒和本机回环，不是长期泄漏或半开公网连接测试。 |

所有上述结果都依赖测试使用的真实 PostgreSQL 和对应进程；真实 Kafka/Redis 容器故障、跨主机网络断开、长期 goroutine 泄漏、数据库实例重启与故障切换仍**未验证**。`go test -race` 因本机缺少 `gcc` 无法构建，因此也没有 race detector 通过结论。
