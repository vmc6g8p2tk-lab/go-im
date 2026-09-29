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

所有上述结果都依赖测试使用的真实 PostgreSQL 和对应进程；真实 Kafka/Redis 容器故障、跨主机网络断开、长期 goroutine 泄漏、数据库实例重启与故障切换仍**未验证**。2026-09-29 安装 MSYS2 UCRT GCC 后，使用仅位于本机工作目录的编译器包装脚本补齐 DLL 搜索路径，`go test -race -count=1 ./...` 全部通过；这只覆盖该次测试实际执行到的并发路径。

## 完整 Compose 栈的真实故障注入（2026-09-29，续）

上表保留先前单进程/模拟路径的历史记录；以下是本次实际运行 Docker Desktop Engine 后新增的真服务证据。

| 故障 / 操作 | 实际观察 | 边界 |
| --- | --- | --- |
| 停止 Redis 容器 | `docker compose stop redis` 后，app1/app2 日志出现 Presence 不可用/心跳失败；真 Kafka 跨实例消息和 ACK 集成测试仍通过（2.36 秒）。重启 Redis 后 healthy。停机前 75 秒长连接期间，真 Key `im:route:3/4` 成员和 TTL/时间分数随心跳续期。 | 证明本地 Hub/Kafka 投递可在 Presence 不可用时继续；没有证明 Redis 参与消息选路。停止容器会清除未持久化的 Key，不能当作 TTL 过期测试。 |
| 停止 Kafka Broker | `docker compose stop kafka` 后运行 `TestRealKafkaOutageDurableFallback` 通过（7.20 秒）：B 在 app2 收到 PostgreSQL 补发消息并 ACK，库中 `delivery_attempts=1`、状态 delivered；消息和状态 Outbox 均待发布。重启 Broker 后两个 Outbox 均已发布，全库待发布为 0。 | Broker 单节点，未模拟分区副本故障；故障期间是数据库补发，不是 Kafka 实时送达。 |
| 重启 app2 / Consumer Group 重平衡 | `docker compose restart app2` 后立即运行严格 Kafka 路径测试失败一次：库中 `delivery_attempts=1`，说明数据库补发先完成。Broker 日志中旧成员约 30 秒后超时，消费组稳定；再次运行严格测试通过（0.48 秒、`delivery_attempts=0`）。 | 故障恢复成功，但实时 Kafka 路径有重平衡窗口；必须保留首次失败。 |
| 向真 Kafka 重放已确认事件 | 控制台 Producer 对已 delivered 的消息 ID 1 再写入 Topic；app1/app2 的 `im_kafka_consume_total{result="ok"}` 分别 +1，PostgreSQL 仍只有 1 条消息、1 条对应 Outbox。 | 验证数据库逻辑幂等；当时无在线客户端，未测重放产生的重复 WebSocket 帧。 |
| 10000 WebSocket 首次连接尝试 | 依次先运行 2000 和 5000 连接组，再尝试 10000；第 9049 个由 Windows `connectex` 返回本地 socket 地址/端口不可用，未进入消息窗口。退出后 app1 `/metrics` 显示连接和队列 Gauge 均为 0、goroutine 40。 | 客户端端口限制与短时间 TIME_WAIT 可能影响；不能推断服务端 9048 连接上限。 |
| 端口回收后的 10000 连接重试 | 10000/10000 建连，49/49 消息首次送达，0 失败与意外断连；结束后 app1 连接和队列 Gauge 均为 0、goroutine 41。 | 100 用户、5 条/秒、10 秒低发送率窗口；不代表长时间或高吞吐容量。 |
| app2 异常停止与 Redis 路由 TTL | 向 app2 建立两条真实连接，用户 888 的 `im:route:888` 为 `app2`、TTL 55 秒；执行 `docker compose stop -t 0 app2` 后成员仍在、TTL 47 秒，之后 TTL 5 秒，最终 `EXISTS=0`、`TTL=-2`。客户端记录 2 次预期断连。app2 已重新启动。 | 只验证该 Key 的真实 Redis TTL 自然过期；没有在第 30 秒调用应用的 `Instances` 方法判断过滤结果。强停客户端故障负载原始 JSON 见 [结果](benchmarks/results/compose-app2-crash-client.json)。 |

真 PostgreSQL 容器停止/恢复、主从切换、跨主机分区、长期连接泄漏仍未执行。新增 Kafka Broker 故障测试需要运行时设置 `INTEGRATION_KAFKA_OUTAGE=1` 且手动先停止 Broker；绝不能在 Broker 正常时把它当作故障测试通过。完整总结见 [FINAL_VALIDATION.md](FINAL_VALIDATION.md)。

## Outbox 批量发布后的真服务回归（2026-09-29）

优化提交 `6eaffa8` 后，先在暂停两台 app 发布者的真实 PostgreSQL 上运行 `TestOutboxBatchRollbackAndMark`：消息与状态各 2 条，注入 Broker 错误后均保持待发布，再次成功后均标记已发布；原有单条 Outbox 回滚测试同时通过。真实 Kafka 停机后 `TestRealKafkaOutageDurableFallback` 通过（6.66 秒），消息 ID 42506 通过 PostgreSQL 补发并 ACK；直接查询该消息两类 Outbox 均为待发布。Kafka 重启健康后两类 Outbox 均已发布，全库待发布 0。

恢复后立即运行严格纯 Kafka 跨实例测试**失败一次**：`delivery_attempts=1`，数据库补发先于 Consumer Group 完成重平衡；Broker 日志随后显示两组约 30 秒后稳定、各分区 lag 0；重跑通过（0.50 秒，`delivery_attempts=0`）。这与此前实例重启时的恢复窗口一致，不能记为“瞬时恢复”。真实 Redis 停机期间再次执行跨 Kafka 消息和 ACK 测试通过（2.56 秒），Redis 重启后 `PING=PONG` 且 healthy。

完整环境变量一次性对 `go test ./...` 启用后，有运行中的 app 抢先发布 repository 集成测试自己的 Outbox 行，导致测试失败；同次离线重连测试收到重复 `message.new` 抢在 ACK 回执前。隔离暂停 app 后 repository 集成测试通过，完整端到端 `./tests` 独立重跑通过；详情见 [PERFORMANCE_INVESTIGATION.md](PERFORMANCE_INVESTIGATION.md)。这些失败记录保留，测试套件并行共享数据库的假设仍需在将来改进。
