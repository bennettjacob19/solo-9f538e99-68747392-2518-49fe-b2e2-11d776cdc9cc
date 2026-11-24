# 温度探头校正服务

温度探头通过 TCP 长连接上报整数温度；独立**工作进程**加上校正偏移后，沿原连接
逐行回 `校正值,版号`。FastAPI 提供 `/docs` 管理接口，提交新偏移即触发**换版**：
平滑交接全部探头连接，半行不丢、停读不阻塞、新进程失败自动回退原版。

## 目录结构

```
app/
  main.py        FastAPI 应用与进程入口（supervisor / worker 两种模式）
  supervisor.py  工作进程监管、换版编排（fork+exec、fd 分批接力中转、容量预检、失败回退）
  worker.py      工作进程：TCP 接入、128B 应用层缓冲、逐行回执、状态快照/恢复
  netutil.py     Unix SEQPACKET 控制通道与 fd 传递（SCM_RIGHTS）
tests/           端到端集成测试与实测脚本（真实子进程 + 真实 TCP/HTTP）
Dockerfile, docker-compose.yml, requirements.txt
```

## 快速开始（docker compose）

```bash
docker compose up --build
# 管理接口（Swagger UI）: http://localhost:8000/docs
# 探头 TCP 长连接地址:     localhost:9000
```

本地直接运行（无需 docker）：

```bash
pip install -r requirements.txt
python -m app.main
# HTTP_PORT / TCP_PORT / INITIAL_OFFSET 可用环境变量覆盖
```

## 探头协议

- 长连接，上报 ASCII 整数，每行以 `\n` 结尾，**行体至多 32 字节**（超长行丢弃）。
- 服务端对每条完整行**恰好回执一次**：`<整数+偏移>,<版号>\n`，按上报顺序 FIFO。
- 非整数行丢弃；连接级错误只影响该连接，不影响工作进程与其他探头。

```bash
nc localhost 9000
# 输入: 25<回车>
# 回执: 25,1
```

## 管理接口（/docs）

| 方法 | 路径      | 说明                                   |
|------|-----------|----------------------------------------|
| POST | `/offset` | body `{"offset": 10}`，提交新偏移并换版 |
| GET  | `/status` | 当前工作进程 PID、版号、偏移、存活状态 |
| GET  | `/health` | 健康检查                               |

换版成功返回 `old_pid / pid / revision / offset / inherited_conns`；
新工作进程启动失败返回 `503`，旧进程继续按原版接单，PID/版号不变。

## 换版语义（关键设计）

工作进程与 supervisor 之间用 Unix `SOCK_SEQPACKET` + `SCM_RIGHTS` 传递 fd。
**快照与 fd 交接分离、fd 分批接力中转**，使监管进程换版期间额外占用的文件
描述符恒不超过 32，与在线连接总数无关：

1. `SNAPSHOT`：旧工作进程**冻结全部连接 I/O**，停 accept，并把内核中已到达的
   数据排空到应用层；随后只回传**纯状态**（分块记录，半行 `recv_buf`、
   未发 `send_buf`、带版号的 `pending` 队列），**不附带任何 fd**——监管进程
   在快照阶段零连接 fd 占用。
2. 容量预检：监管进程自身可用 fd 余量至少 32（不足直接 503，**在冻结旧进程
   之前**拒绝，线上服务不受影响）；新工作进程的 nofile 须容得下
   “在线连接数 + 16 预留”，不足则 `RESUME` 旧进程并返回 503。
3. supervisor `fork+exec` 新工作进程，先转交监听 fd；连接 fd 按**每批 31 个**
   接力中转：令旧进程发一批 → 监管收一个立即转发给新进程并关闭中转副本 →
   批末收旧进程 `FDS_DONE`、等新进程 `FDS_ACK`（逐批流控，防
   `ETOOMANYREFS`）后再发下一批。任一时点监管额外持有的 fd 至多为
   1 个新控制通道 + 31 个连接 fd = **32**。应用缓冲状态随后分块移交，
   新进程在同一批 fd 上原样恢复，回 `READY` 后保持冻结。
4. 旧进程收 `QUIT` 退出并被回收，supervisor 关闭中转 fd 副本后才发 `GO`
   放行新进程，保证任一时刻同一连接只有一个进程在操作。
5. 任一阶段失败（握手超时、新进程接管前退出、协议错误）：杀掉新进程、
   关闭全部中转 fd 副本、向旧进程发 `RESUME` 并等 `RESUMED` 确认；存活的
   旧进程沿原连接按原版立即恢复服务，本轮资源释放干净，可再次换版。

由此满足：

- 换版**前已收到换行符**的行 → 旧版回执（快照前已切行，回执带旧版号）；
- 换版时**只有半行**的 → 缓冲随 fd 移交，补齐后按**新版**回执；
- 探头**停读**或只发半行 → 不阻塞换版，旧进程直接退出，连接由新进程继承，
  探头**无需重连**；其他探头全程照常收回执；
- 新进程启动/握手失败、交接中断 → supervisor 回 `RESUME`，旧进程解冻继续
  按原版服务，PID/版号不变，接口返回 503。

### 文件描述符预算与可调项

| 环境变量 | 作用 | 默认 |
|----------|------|------|
| `FD_HEADROOM_FLOOR` | 换版要求的监管进程最小可用 fd 余量（也是额外占用上界） | 32 |
| `SUPERVISOR_NOFILE` | 监管进程启动后压低自身软 nofile（受限环境模拟/调优） | 不设置 |
| `WORKER_NOFILE_SOFT` | 工作进程握手前把软 nofile 调到该值（不超过 hard） | 不设置 |
| `WORKER_NOFILE_HARD` | 工作进程启动时先压 hard nofile（模拟容量更小的容器） | 不设置 |

`GET /status` 额外返回 `fd_soft_limit / fd_open / fd_headroom /
reload_extra_fd_peak`（上一轮换版监管进程额外占用 fd 的高水位），便于观测。

## 缓冲策略

每条连接独立维护应用层收/发缓冲，**各上限 128 字节**：

- 接收缓冲满 → 只对该连接摘掉 `POLLIN` 暂停读入，其他连接不受影响；
- 发送缓冲满 → 回执进入该连接的 FIFO `pending` 队列，可写后按序发出；
- 行一旦从接收缓冲切出即记账，任何重传/交接路径都不会重复回执。

## 测试

```bash
pip install -r requirements.txt httpx
python tests/integration_test.py        # 7 组端到端用例
python tests/fd_limit_test.py           # 描述符受限 + 交接中断（6 组）
python tests/manual_reload_demo.py      # 实测：停读/半行换版
python tests/buffer_cap_test.py         # 128B 上限与连接隔离、回执不丢不重
python tests/revision_boundary_test.py  # 版号边界
python tests/stress_test.py             # 多探头 + 并发换版压力
```

覆盖：逐行回执/顺序/半行、换版版号语义、停读与半行不阻塞换版、
新进程失败回退、**在线连接数超过监管余量仍可换版且额外 fd 峰值 ≤ 32**、
**余量不足在冻结前快速 503（旧连接不冻住）**、**新进程容量不足预检拒绝**、
**交接中断（新进程接管前中途退出）旧进程原版恢复且可再次换版**、
**200 连接分批交接无死锁/无 ETOOMANYREFS**、128B 缓冲隔离、
超长/非整数行丢弃、`/docs` 可用、多次换版 PID 轮换。
