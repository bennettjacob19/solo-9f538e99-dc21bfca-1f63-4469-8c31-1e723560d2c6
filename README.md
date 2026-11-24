# 温度探头校正服务

温度探头通过 TCP 长连接上报整数温度；独立**工作进程**加上校正偏移后，沿原连接
逐行回 `校正值,版号`。FastAPI 提供 `/docs` 管理接口，提交新偏移即触发**换版**：
平滑交接全部探头连接，半行不丢、停读不阻塞、新进程失败自动回退原版。

## 目录结构

```
app/
  main.py        FastAPI 应用与进程入口（supervisor / worker 两种模式）
  supervisor.py  工作进程监管、换版编排（fork+exec、SCM_RIGHTS 传 fd、失败回退）
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

工作进程与 supervisor 之间用 Unix `SOCK_SEQPACKET` + `SCM_RIGHTS` 传递 fd：

1. `SNAPSHOT`：旧工作进程**冻结全部连接 I/O**，停 accept，并把内核中已到达的数据
   排空到应用层；随后把每个连接的 fd 与应用层缓冲状态（半行 `recv_buf`、
   未发 `send_buf`、带版号的 `pending` 队列）一并交给 supervisor。
2. supervisor `fork+exec` 新工作进程，经控制通道转交监听 fd、连接 fd 与缓冲状态；
   新进程在**同一批 fd** 上原样恢复，回 `READY` 后保持冻结。
3. 旧进程收 `QUIT` 退出并被回收，supervisor 关闭中转 fd 副本后才发 `GO` 放行新进程，
   保证任一时刻同一连接只有一个进程在操作。

由此满足：

- 换版**前已收到换行符**的行 → 旧版回执（快照前已切行，回执带旧版号）；
- 换版时**只有半行**的 → 缓冲随 fd 移交，补齐后按**新版**回执；
- 探头**停读**或只发半行 → 不阻塞换版，旧进程直接退出，连接由新进程继承，
  探头**无需重连**；其他探头全程照常收回执；
- 新进程启动/握手失败 → supervisor 回 `RESUME`，旧进程解冻继续按原版服务。

## 缓冲策略

每条连接独立维护应用层收/发缓冲，**各上限 128 字节**：

- 接收缓冲满 → 只对该连接摘掉 `POLLIN` 暂停读入，其他连接不受影响；
- 发送缓冲满 → 回执进入该连接的 FIFO `pending` 队列，可写后按序发出；
- 行一旦从接收缓冲切出即记账，任何重传/交接路径都不会重复回执。

## 测试

```bash
pip install -r requirements.txt httpx
python tests/integration_test.py        # 7 组端到端用例
python tests/manual_reload_demo.py      # 实测：停读/半行换版
python tests/buffer_cap_test.py         # 128B 上限与连接隔离、回执不丢不重
python tests/revision_boundary_test.py  # 版号边界
```

覆盖：逐行回执/顺序/半行、换版版号语义、停读与半行不阻塞换版、
新进程失败回退、128B 缓冲隔离、超长/非整数行丢弃、`/docs` 可用、多次换版 PID 轮换。
