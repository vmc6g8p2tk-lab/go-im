# Go IM V1.0 最终真实环境验收记录

记录日期：2026-09-29（Asia/Shanghai）。本文件按实际执行的证据区分状态；**V1.0 尚不能宣称完成全部真实环境验收或封版**。与历史记录对应的原始压测汇总见 [BENCHMARK.md](BENCHMARK.md)，端到端功能和故障测试见 [TEST_RESULTS.md](TEST_RESULTS.md)、[FAILURE_TEST.md](FAILURE_TEST.md)。

## 【真实验证通过】

| 能力 | 实际证据与范围 |
| --- | --- |
| 注册、登录、bcrypt、JWT、单聊、群聊、历史查询 | 本机真实 Go 服务 + PostgreSQL 18.3 的 HTTP/WebSocket 端到端测试通过；群聊成员权限及 100 人上限测试通过。 |
| 消息持久化、幂等、ACK、离线补发 | 测试直接查询真实 PostgreSQL；相同客户端 UUID 只产生一条逻辑消息，断线重连同步未确认消息，ACK 后状态持久化。 |
| Transactional Outbox | 真实 PostgreSQL 事务、触发器和待发布状态核查通过；发布失败及重试使用注入函数，不等于真实 Kafka Broker 验收。 |
| 双进程数据库补发 | 两个真实 OS 进程、真实 PostgreSQL；一个实例退出后重新连接另一个实例，接收者获得未确认消息。该路径未启用 Kafka。 |
| 限流、背压、资源释放 | 对应单元和真实 WebSocket 测试通过；100 连接、70 秒心跳测试后，`/metrics` 显示连接与发送队列 Gauge 均为 0。 |
| Race Detector | Windows 10 Pro 22H2 + Go 1.27.1 + MSYS2 UCRT GCC 16.2.0，`CGO_ENABLED=1 go test -race -count=1 ./...` 退出码 0；`tests` 包使用真实 PostgreSQL 与本地服务，耗时 26.511 秒。真实 Kafka 双实例测试因环境变量未设置而跳过。 |
| 静态检查 | `go vet ./...` 退出码 0；Git 仓库未跟踪 `.env`、可执行文件、pprof 或日志；未发现提交的实际 JWT 密钥、数据库密码或本机绝对路径。 |
| 本机回环 5000 WebSocket 连接 | 5000/5000 建连、499/499 首次送达、0 工具计数失败、0 非预期断连；50 条/秒目标速率、10 秒、50 用户、单个非容器 Go 服务和本机 PostgreSQL，Redis/Kafka 未启用。此结果不能外推到完整 Compose 栈。 |
| 现有 pprof 样本 | 在 500 连接、300 条/秒、20 秒负载期间采集 10 秒 CPU、Heap、Goroutine Profile；CPU 样本 `runtime.cgocall` 70.18% flat，累计栈指向 Windows `WSASend`；1011 个 goroutine 中约 1000 个为读写循环。 |

## 【代码已实现但未完全验证】

| 能力 | 当前状态与缺少的证据 |
| --- | --- |
| Redis 在线路由 | `miniredis` 模拟器测试通过；真实 Redis Key、Value、TTL、心跳续期、断线删除、实例强杀后的过期和真实 Redis 不可用场景均未执行。**真实 Redis：未通过验收（未运行）。** |
| Kafka 发布/消费 | 代码、Outbox 和模拟消费边界测试通过；真实 Topic、Producer、Consumer Group、Partition Key、Broker 重启、重复事件和消费失败未测试。**真实 Kafka Broker：未通过验收（未运行）。** |
| 双实例跨 Kafka 聊天与 ACK 状态传播 | 有专门的 `TestRealKafkaCrossInstanceDeliveryAndStatus`，但缺少两个已启动的 Kafka 实例；当前只能说明双进程 PostgreSQL 补发路径通过。**双实例跨 Kafka：未通过验收（未运行）。** |
| Docker Compose | 安装了 Docker Desktop CLI 29.8.1、Compose v5.5.1，9 服务的 `config --quiet` 退出码 0；实际执行 `up --build -d` 和 `ps` 均退出码 1，报 Engine 命名管道不存在。没有容器日志。**Compose：未通过验收。** |
| Prometheus / Grafana | Go `/metrics` 真实响应、面板 JSON 和抓取配置已检查；Prometheus 容器抓取、Grafana 数据源/面板和截图未实际运行。**两者真实容器验收：未通过。** |
| 故障恢复 | PostgreSQL TCP 代理断连、模拟 Redis 不可用、模拟 Kafka 发布失败、双进程强杀和客户端重连已测试；真实 PostgreSQL/Redis/Kafka 容器重启、Kafka lag、路由残留、Broker 恢复与消费组重平衡尚未测试。 |

## 【未实现】

- 生产环境 TLS/WSS、JWT 主动吊销和独立密钥托管未实现；当前配置面向本机开发环境。
- 群消息逐成员 ACK 未实现；离线群消息通过历史 API 查询。
- 跨主机和多区域部署没有实现或验收。不能把本机双进程测试描述为生产分布式集群。

## 【环境限制】

- Windows 10 Pro 22H2（build 19045）、Intel Core i7-13700KF（24 逻辑处理器）、物理内存约 31.82 GiB。检查时可用内存约 15.85 GiB、C: 剩余约 127.6 GiB。
- 已通过系统命令启用 `Microsoft-Windows-Subsystem-Linux` 和 `VirtualMachinePlatform`，并安装 Microsoft 签名的 WSL 2.7.14.0；Windows 仍报告重启待完成。**必须由用户手动重启，不能把已启用功能当成已运行的 WSL2。**重启后还需确认硬件虚拟化可用。
- 已安装 Docker Desktop 当前用户版本，但尚未启动 Engine；首次打开需要用户在 GUI 中阅读并接受 Docker Desktop 协议。CLI 可用不表示容器运行。
- 当前本机另有 PostgreSQL 18.3 测试进程监听 `127.0.0.1:15432`、Go 服务监听 `127.0.0.1:18080`；Compose 默认端口为 PostgreSQL 5432、应用 8080/8081，不冲突。
- 没有 GitHub 用户名或仓库 URL，`go.mod` 仍为 `github.com/example/go-im`；仓库尚无 remote。不能猜测实际模块路径，也未推送或创建 GitHub Release。

## 性能结论及禁止外推的边界

| 指标 | 本次确有的结果 | 完整 Redis/Kafka/Compose 环境 |
| --- | --- | --- |
| 最大已实测稳定连接组 | 5000/5000，50 条/秒，10 秒；不能称为连接上限 | 未测 |
| 10000 连接 | 未测 | 未测 |
| 稳定消息吞吐极限 | 未测；现有各连接组目标速率不同，pprof 负载的目标速率为 300 条/秒，不是吞吐极限测试 | 未测 |
| 5000 连接组首次送达延迟 | P50 0.524 ms、P95 0.731 ms、P99 1.108 ms（原始实现单次）；最终缓冲配置同负载单次 P95 1.043 ms、P99 2.674 ms | 未测 |
| 5000 连接组服务资源 | 原始实现 CPU 14.30%、峰值 RSS 246.6 MiB、峰值 goroutine 10012；仅计算 Go 服务，不含客户端和 PostgreSQL | 未测 |
| 优化前后 | 同负载单次比较：RSS 246.6 → 234.3 MiB，P95 0.731 → 1.043 ms，P99 1.108 → 2.674 ms；只观察到内存与延迟的取舍 | 未重复验证 |
| PostgreSQL / Redis / Kafka 状态 | PostgreSQL 真实落库核查通过；Redis 与 Kafka 真服务未运行，lag 未采集 | 未测 |

这些数值来自 Windows 本机回环压测，连接数、用户数、目标发送速率和时长见 [BENCHMARK.md](BENCHMARK.md)。压测客户端与数据库资源未计入服务 CPU/RSS。不能把 49.9 条/秒的 5000 连接组或 pprof 负载的 300 条/秒目标速率写成系统最大吞吐。

## 恢复验收所需的实际步骤

1. 用户手动重启 Windows。重启后运行 `wsl --version`，确认 WSL 版本；若 WSL2 初始化失败，检查 BIOS/UEFI 的硬件虚拟化设置。
2. 用户打开 Docker Desktop，在首次启动界面阅读并接受协议，等 Engine 就绪。运行 `docker info` 与 `docker compose version`；仅 CLI 版本输出不足以通过验收。
3. 在仓库根目录执行 `pwsh -File scripts/init-env.ps1`，再执行 `pwsh -File scripts/verify-compose.ps1 -Multi`。该脚本启动 PostgreSQL、Redis、Kafka、两台 IM 服务、Prometheus、Grafana，并检查 HTTP 健康与 Kafka Topic；随后保留 `docker compose --env-file .env -f deployments/compose.yaml --profile multi ps` 和关键 `logs` 的真实输出。
4. 按 README 设置 `DATABASE_URL`、`INTEGRATION_BASE_URL=http://127.0.0.1:8080`、`INTEGRATION_SECOND_BASE_URL=http://127.0.0.1:8081`，运行 `go test -run TestRealKafkaCrossInstanceDeliveryAndStatus -v ./tests`。同时以 `redis-cli` 查实际路由 Key/TTL，以 Kafka CLI 查 Topic 与 Consumer Group，以 PostgreSQL 查消息、ACK、Outbox。
5. 依次执行真实 Redis/Kafka/数据库重启及两台服务强杀恢复；记录日志、实际键值/TTL、lag、数据库最终状态和未预期断连。再在完整栈稳定后分级进行 1000/2000/5000/10000 连接测试和独立吞吐阶梯。每一级监视主机可用内存并在错误率、P99、lag 或资源明显恶化时停止。

所有步骤必须更新本文件和 [FAILURE_TEST.md](FAILURE_TEST.md) 的真实结果；目前没有 Docker `ps`、容器日志或监控截图可以附上。

## GitHub 发布门槛

- Git 已跟踪 100 个文件，当前检查没有跟踪 `.env`、日志、pprof 或可执行文件，未发现实际密钥、数据库密码、本机绝对路径或超过 1 MiB 的已跟踪文件。`.env.example` 只含明确的占位值。
- 用户提供 GitHub 用户名且仓库确定为 `github.com/<username>/go-im` 后，运行 `pwsh -File scripts/prepare-github-module.ps1 -GitHubUsername <username>`。该脚本只更新 `go.mod` 和 Go 源码导入路径；随后更新 README 中的占位说明，执行 `go mod tidy`、`go test ./...`、`go vet ./...`、`go test -race ./...`，检查 `git diff` 与远端 URL。
- 在真实 Compose/Redis/Kafka/监控、故障和分级性能验收完成以前，不打 `v1.0.0` tag，不把 README 的代码能力写成全部生产链路已验证。
