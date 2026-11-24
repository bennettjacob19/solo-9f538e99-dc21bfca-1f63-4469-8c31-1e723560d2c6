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

| 方法 | 路径           | 说明                                                        |
|------|----------------|-------------------------------------------------------------|
| GET  | `/connections` | 在线探头连接列表（含对端 `peer_ip` / `peer_port` / `peer`） |
| POST | `/trace`       | 按对端 IP:端口 选中连接，开始追踪接下来 **1～64** 条回执     |
| GET  | `/trace`       | 查询最近一轮：逐条返回校正值、版号、进度                     |
| POST | `/offset`      | body `{"offset": 10}`，提交新偏移并换版                      |
| GET  | `/status`      | 当前工作进程 PID、版号、偏移、存活状态                       |
| GET  | `/health`      | 健康检查                                                    |

> 为兼容不同调用习惯，另提供别名：`GET /conns`、`/peers`；
> `POST /traces`、`/trace/start`；`GET /traces`、`/trace/latest`、
> `/trace/items`（裸数组）、`/receipts` 等。字段同时给英文状态码
> （`pending / handed_over / acked / unconfirmed`）、中文 `progress_cn`
> （待发 / 已交出 / 已确认 / 未确认）以及 `value/rev` 等别名。

换版成功返回 `old_pid / pid / revision / offset / inherited_conns`；
新工作进程启动失败返回 `503`，旧进程继续按原版接单，PID/版号不变。

## 回执追踪（探头迟迟收不到回执时）

操作顺序（也可直接在 `/docs` 的 Swagger 表单里点）：

```bash
# 1) 按对端 IP:端口 找到在线连接（探头侧临时端口即 peer_port）
curl -s localhost:8000/connections | python -m json.tool

# 2) 选中它，追踪接下来生成的 8 条回执（count 取 1..64）
curl -s -X POST localhost:8000/trace \
  -H 'Content-Type: application/json' \
  -d '{"peer_ip":"172.17.0.1","peer_port":51234,"count":8}'

# 3) 随时查询当前进度（探头不再上报也能查；换版握手空窗期也能查）
curl -s 'localhost:8000/trace?peer_ip=172.17.0.1&peer_port=51234'
```

每条记录：

```json
{
  "index": 1,
  "corrected_value": 205,
  "revision": 2,
  "state": "acked",
  "progress": "acked",
  "progress_cn": "已确认",
  "pending": false, "handed_over": true, "acked": true,
  "unconfirmed": false
}
```

进度判定（对应需求原话）：

| `state`        | 中文     | 判定（累计、单调）                                              |
|----------------|----------|-----------------------------------------------------------------|
| `pending`      | 待发     | 整条回执尚未全部 `send()` 给本机 TCP（行尾绝对字节 `end_abs` 超过已交出字节） |
| `handed_over`  | 已交出   | 整行字节都已被本机内核接受（`end_abs <= tcpi_bytes_acked + SIOCOUTQ`） |
| `acked`        | 已确认   | 对端 TCP 累计确认已越过本行行尾（`end_abs <= TCP_INFO.tcpi_bytes_acked`） |
| `unconfirmed`  | 未确认   | **结算终态**：连接关闭或成功移交时，除当时已确认外的其余记录     |

一轮最多保留选定的 `count` 条，额外回执不再记录；全局只保留最近一轮
（再次 `POST /trace` 即替换旧轮）。轮次顶层有 `state`
（`active` / `settled`）与 `settle_reason`（`closed` / `handover` /
`worker_exit`），结算后条目视图冻结，仍可 `GET /trace` 查阅。

### 追踪在换版 / 关闭 / 回退时的行为（关键设计）

- 轮次对象保存在 **supervisor 进程**。`POST /trace` 时工作进程经
  `SCM_RIGHTS` 回传目标连接 fd，supervisor 持有它的一个独立 dup；
  查询只是在该 fd 上做一次 `TCP_INFO` + `SIOCOUTQ`（O(1)），
  **不询问工作进程、不扫描收发/pending 积压**。
- 因此换版时旧进程交出套接字后，即使新进程尚未 `READY/GO`，
  supervisor 仍能用自己的 dup 读到内核里最新的累计 ACK——
  这期间到达的确认立刻可查。查询走独立的短锁，不参与换版握手，
  接管新进程或 RESUME 旧进程都不需要等任何查询请求结束。
- 工作进程在 accept/close 时通过控制通道异步上报 `CONN_OPEN/CLOSE`、
  生成回执时限量上报 `TRACE_ITEM`；supervisor 用一个控制通道读线程
  统一接收，换版同步握手前先 join 该线程并对插队消息做分流。
- **连接关闭**：worker 先 `sock.close()` 再以非阻塞方式发
  `CONN_CLOSE`（追踪不推迟探头套接字关闭）；supervisor 立即结算本轮
  为 `closed` 并关闭自己的 dup，释放套接字引用。查询时若发现
  `tcpi_state` 已进入 CLOSE/CLOSE_WAIT 等也会兜底结算。
- **成功换版移交**：新进程 `GO` 后结算本轮为 `handover`，
  当时已确认的保留 `acked`，其余一律 `unconfirmed`。
- **换版失败并 RESUME 原进程**：追踪配置仍在旧进程内存中，
  轮次 id 不变、保持 `active`，后续回执与确认继续进入同一轮。
- 回执字节使用连接级绝对坐标：worker 维护单调的 `gen_total`
  （已生成）/ `handed_total`（已 `send()`），随快照跨版本连续传递，
  与 `tcpi_bytes_acked` 的 payload 字节坐标对齐。

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
pip install -r requirements.txt httpx pytest
python tests/integration_test.py        # 7 组端到端用例
python tests/trace_test.py              # 11 组回执追踪用例（含握手期 ACK 实测）
python tests/manual_reload_demo.py      # 实测：停读/半行换版
python tests/buffer_cap_test.py         # 128B 上限与连接隔离、回执不丢不重
python tests/revision_boundary_test.py  # 版号边界
python tests/stress_test.py             # 多探头 + 并发换版压力
# 或：pytest tests/trace_test.py -q
```

`trace_test.py` 覆盖：在线连接列表与 `/docs`、校正值/版号/已确认、
参数校验(1..64)/容量上限/最近一轮替换、真实 TCP 下的
`待发 / 已交出 / 已确认` 三态、关闭结算、混合结算单元用例、
**握手屏障期间收到 ACK 并即时可见**、换版失败 RESUME 后继续本轮、
大积压下 ARM/查询低延迟、追踪不推迟关闭。

为确定性构造窄窗口/待发状态，提供两个**仅测试用**环境变量
（不设置时行为完全不变，compose 也不启用）：

- `RELOAD_BARRIER_DIR=<dir>`：supervisor 收到旧进程交出的 fd 后写
  `reached` 文件并等待 `release` 文件，把“旧已交出、新未就绪”的
  握手空窗拉成确定窗口，用于实测窗口内的 ACK 可见性；
  另有 `RELOAD_HANDSHAKE_DELAY=<秒>` 固定延时。
- `WORKER_TEST_SNDBUF=<字节>`：把新 accept 连接的 `SO_SNDBUF` 调小，
  以稳定复现“整行尚未全部交给本机 TCP”的待发态。

## 交付说明（改动 / 实测结果 / 未完成项）

### 改动文件

- `app/trace.py`（新增）：进度状态机、`TraceRound/TraceManager`、
  `TCP_INFO.tcpi_bytes_acked` 与 `SIOCOUTQ` 读取、记录/轮次 JSON。
- `app/worker.py`：连接对端地址与 O(1) `by_peer` 索引；
  `gen_total/handed_total` 绝对字节坐标；限量 trace 记录与
  `TRACE_ITEM/TRACE_ARMED`、`CONN_OPEN/CLOSE` 控制消息；
  快照携带对端/坐标/trace 镜像；关闭时先 `close()` 后非阻塞上报；
  `WORKER_TEST_SNDBUF` 测试钩子。
- `app/supervisor.py`：在线连接注册表、全局最近一轮 `TraceManager`、
  控制通道读线程与 ARM 请求关联、reload 握手前停读线程并分流插队消息、
  快照对账/合并、握手屏障钩子、成功移交结算、失败回退后重启读线程、
  worker 崩溃结算与清表。
- `app/main.py`：`GET /connections`、`POST /trace`、`GET /trace`
  及若干路径/字段别名，`/docs` 表单模型。
- `tests/trace_test.py`（新增）：11 组真实子进程 + 真实 TCP/HTTP 用例。
- `tests/integration_test.py`：仅给 `Server` 增加可选 `extra_env`，
  默认行为不变。

### 实测结果（Linux 7.0 x86_64，Python 3.11，loopback）

- `tests/trace_test.py`：**11/11 通过**；连跑 3 轮（含一次
  `pytest tests/trace_test.py`：11 passed in ~23s）无失败、无明显抖动。
- 原有用例全部保持通过：`integration_test.py` **7/7**、
  `buffer_cap_test.py`、`revision_boundary_test.py`、
  `stress_test.py`（480/480 回执、三次换版到 rev4）、
  `manual_reload_demo.py` 均通过。
- 关键的“握手期间收到确认”实测：用 `RELOAD_BARRIER_DIR` 把 reload
  卡在“旧进程已交出套接字、新进程尚未 spawn”的屏障；此时
  `GET /trace` 约 **62ms** 返回且轮次仍 `active`、reload 线程仍阻塞；
  让探头在屏障期间排空接收缓冲，内核自动回 ACK，查询当场看到
  16/16 条变为 `acked`；放行后 reload 返回 200，轮次结算为
  `settled/handover`，已确认条目保留。
- 三态均在真实 TCP 上复现：普通连接排空后全部 `acked`；
  `TCP_WINDOW_CLAMP` 窄窗口 + 前置积压稳定得到 `handed_over`
  （未确认）；再加 `WORKER_TEST_SNDBUF=4096` 与更大积压稳定得到
  `pending`。直接关闭不读时结算为 `closed`，未确认条目置 `unconfirmed`。

### 未完成项 / 限制（如实说明）

- 本构建沙箱内**没有 docker CLI/daemon 与 `CAP_NET_ADMIN`**，
  因此未实际执行 `docker compose up --build`；已校验
  `docker-compose.yml` YAML 合法、端口/环境/健康检查与
  `COPY app ./app`（会带上新增 `trace.py`）无误，并用与容器 CMD
  相同的 `python -m app.main` 子进程方式完成全部端到端实测。
  compose 配置与基线一致，理论上可直接 `docker compose up --build`。
- “只保留最近一轮”按**全局最近一轮**实现（任意连接发起新追踪都会
  替换旧轮）；若期望“每条连接各保留一轮”，把 `TraceManager.current`
  换成按 key 的字典即可，状态机/接口无需改。
- `tcpi_bytes_acked` 偏移按现行 `struct tcp_info` 固定在 120，
  适用于 Linux ≥ 4.6（实测内核 7.0）；更老内核无该字段时退化为
  “worker 上报的 handed_total − SIOCOUTQ”，但 handed_total 在纯运行期
  不做周期上报，极端情况下已交出态可能偏保守（显示待发）。
- 接口与基线一样**无鉴权**，仅建议部署在受信管理网络；未做 HTTPS。
- 为避免逐条扫描，追踪能力依赖 supervisor 持有的 socket dup；
  连接关闭事件到达后会立即关闭该 dup，正常只带来亚毫秒~毫秒级
  事件延迟，已用“关闭→settle < 2s（实测远小）”用例约束。
