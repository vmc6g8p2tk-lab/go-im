# 修改前真实完整栈 pprof

2026-09-29（Asia/Shanghai），原始实现、两台 Docker Compose IM 容器、真实 PostgreSQL/Redis/Kafka。对 app1/app2 各采集 CPU、Heap、Goroutine、Block、Mutex；`profile-times.txt` 记录与 15 秒 500 msg/s 发送窗口的重叠，`profile-load.json` 保存这轮剖析负载结果。这个 15 秒剖析负载不能与 10 秒正式 Baseline 直接比较。

`*-fullstack-sync.pprof` 均在 100 条连接建好后、发送窗口仍进行时采集。CPU Profile 请求于 20:15:46.681、完成于 20:15:55.012；发送窗口开始于 20:15:46.522。Heap/Goroutine/Block/Mutex 快照于 20:15:46.902 前完成，属于较早的负载时刻，Block/Mutex 的原始采样累积了启动以来的等待。

为分析等待的**增量**，另一次相同 500 msg/s 诊断负载在 20:17:53.446 和 20:17:57.686 对 app1 采集 `block-start/end` 和 `mutex-start/end`，`goroutine-end` 是后一个时点快照。用 `go tool pprof -diff_base app1-block-start-fullstack-diff.pprof app1-block-end-fullstack-diff.pprof` 比较；Mutex 同理。详细结论、SQL 和吞吐时间线见仓库根目录 [PERFORMANCE_INVESTIGATION.md](../../../PERFORMANCE_INVESTIGATION.md)。文件均为 pprof 原始 protobuf，不包含服务环境变量或用户凭证；包含 Go 模块路径与函数符号。
