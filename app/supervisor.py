"""工作进程监管与换版编排（与 FastAPI 同进程）。

换版流程
========
1. 向旧工作进程发 ``SNAPSHOT``；旧进程冻结全部连接 I/O，交回监听 fd 之外的
   每个连接 fd 与应用层缓冲状态（半行/未发回执/版号）。
2. fork+exec 新工作进程，经控制通道 SCM_RIGHTS 传监听 fd、连接 fd 与状态，
   新进程在同一批 fd 上原样恢复后回 ``READY``。
3. 成功：向旧进程发 ``QUIT`` 并切换当前句柄，探头连接不断；
   失败（超时/退出/握手错误）：向旧进程发 ``RESUME`` 解冻，继续用原版接单，
   接口返回错误且当前 PID/版号不变。
"""
from __future__ import annotations

import os
import select
import socket
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .netutil import control_pair, recv_fd, send_fd
from .trace import (
    TraceManager,
    normalize_limit,
    normalize_peer,
)

HANDSHAKE_TIMEOUT = 10.0
ARM_TIMEOUT = 4.0


class ReloadFailed(RuntimeError):
    """新工作进程启动失败，旧进程仍在服务。"""


class TraceUnavailable(RuntimeError):
    """追踪请求当前无法完成（无工作进程 / 工作进程无此连接）。"""

    def __init__(self, message: str, status_code: int = 503):
        super().__init__(message)
        self.status_code = status_code


@dataclass
class WorkerHandle:
    pid: int
    control: socket.socket
    offset: int
    revision: int
    started_at: float
    alive: bool = True
    exitcode: int | None = None
    superseded: bool = False  # 换版成功后旧进程置 True，退出属预期
    # 正常服务期唯一的控制通道读线程（reload 同步握手前必须先 join 它）
    reader_stop: threading.Event | None = None
    reader_thread: threading.Thread | None = None
    # arm_id -> {"event": Event, "msg": dict|None, "fd": int|None}
    arms: dict[str, dict[str, Any]] = field(default_factory=dict)


class Supervisor:
    def __init__(self, host: str, tcp_port: int, initial_offset: int = 0):
        self.host = host
        self.tcp_port = tcp_port
        self.offset = initial_offset
        self.revision = 1
        self.lock = threading.RLock()
        self.worker: WorkerHandle | None = None
        self._listen_sock: socket.socket | None = None
        # 在线连接注册表：(peer_ip, peer_port) -> 信息。
        # 由工作进程的 CONN_OPEN/CLOSE 事件维护，快照时对账。
        self._peers: dict[tuple[str, int], dict[str, Any]] = {}
        # 注册表/追踪轮次专用锁：HTTP 查询只取这把短锁，
        # 绝不等待 reload 的 self.lock 长握手。
        self.state_lock = threading.RLock()
        self.traces = TraceManager()

    # ---------------- 生命周期 ----------------
    def start(self) -> None:
        listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen.bind((self.host, self.tcp_port))
        listen.listen(128)
        self._listen_sock = listen
        self.worker = self._spawn(
            offset=self.offset, revision=self.revision, conn_fds=[], state={"conns": []}
        )
        self._start_reader(self.worker)
        self._start_watcher(self.worker)

    def shutdown(self) -> None:
        with self.lock:
            worker = self.worker
            if worker is None:
                return
            worker.superseded = True
            self._stop_reader(worker)
            try:
                send_fd(worker.control, {"type": "SHUTDOWN"}, None)
            except OSError:
                pass
            worker.control.close()
            self.worker = None
            with self.state_lock:
                self._peers.clear()
            self.traces.shutdown()

    # ---------------- 查询 ----------------
    def status(self) -> dict:
        with self.lock:
            w = self.worker
            return {
                "pid": w.pid if w else None,
                "alive": bool(w and w.alive),
                "offset": self.offset,
                "revision": self.revision,
                "tcp_port": self.tcp_port,
                "started_at": w.started_at if w else None,
            }

    # ---------------- 在线连接注册表 ----------------
    def list_connections(self) -> list[dict[str, Any]]:
        """O(在线连接数) 的内存表读取；不询问工作进程、不扫积压。"""
        with self.state_lock:
            trace_key = None
            trace_state = None
            trace_id = None
            if self.traces.current is not None:
                trace_key = self.traces.current.key
                trace_state = self.traces.current.state
                trace_id = self.traces.current.trace_id
            out: list[dict[str, Any]] = []
            for (ip, port), info in self._peers.items():
                traced = trace_key == (ip, port)
                out.append({
                    "peer_ip": ip,
                    "peer_port": port,
                    "peer": f"{ip}:{port}",
                    "addr": f"{ip}:{port}",
                    "ip": ip,
                    "host": ip,
                    "port": port,
                    "online": True,
                    "pid": info.get("pid"),
                    "worker_pid": info.get("pid"),
                    "since": info.get("since"),
                    "trace_id": trace_id if traced else None,
                    "tracing": traced and trace_state == "active",
                    "trace_state": trace_state if traced else None,
                })
            out.sort(key=lambda c: (c["peer_ip"], c["peer_port"]))
            return out

    def _upsert_peer(self, ip: str, port: int, pid: int | None) -> None:
        if not ip or not port:
            return
        with self.state_lock:
            self._peers[(ip, int(port))] = {
                "pid": pid, "since": time.time(),
            }

    def _remove_peer(self, ip: str, port: int) -> None:
        with self.state_lock:
            self._peers.pop((ip, int(port)), None)

    def _reconcile_snapshot_peers(
        self, old_pid: int, snap_conns: list[dict[str, Any]]
    ) -> list[tuple[str, int, dict[str, Any]]]:
        """用快照作为冻结时刻的权威在线集合对账注册表。"""
        snap_keys: set[tuple[str, int]] = set()
        enriched: list[tuple[str, int, dict[str, Any]]] = []
        for item in snap_conns:
            ip = str(item.get("peer_ip", ""))
            try:
                port = int(item.get("peer_port", 0) or 0)
            except (TypeError, ValueError):
                continue
            if not ip or not port:
                continue
            key = (ip, port)
            snap_keys.add(key)
            enriched.append((ip, port, item))
        with self.state_lock:
            for key, info in list(self._peers.items()):
                # 只清理属于当前旧 worker 且快照中已不存在的连接
                if info.get("pid") == old_pid and key not in snap_keys:
                    self._peers.pop(key, None)
            for ip, port in snap_keys:
                self._peers.setdefault((ip, port), {"pid": old_pid,
                                                    "since": time.time()})
        return enriched

    # ---------------- 追踪：开始 / 查询 ----------------
    def start_trace(self, params: dict[str, Any]) -> dict[str, Any]:
        """按对端 IP:端口 选中在线连接并开始最新一轮追踪。"""
        try:
            limit = normalize_limit(
                params.get("count", params.get("n", params.get("limit",
                             params.get("max", params.get("receipts")))))
            )
        except ValueError:
            raise
        try:
            peer_ip, peer_port = normalize_peer(
                params.get("peer_ip", params.get("ip", params.get("host",
                            params.get("peer_host")))),
                params.get("peer_port", params.get("port")),
                peer=params.get("peer", params.get("addr",
                             params.get("address", params.get("peer_addr")))),
            )
        except ValueError:
            raise

        # 与 reload 串行：保证 ARM 期间不会发生控制通道交接；
        # ARM 是有界快速 RPC，HTTP 查询路径并不取这把锁。
        with self.lock:
            handle = self.worker
            if handle is None or not handle.alive:
                raise TraceUnavailable("no active worker", 503)
            arm_id = uuid.uuid4().hex
            box: dict[str, Any] = {
                "event": threading.Event(), "msg": None, "fd": None,
            }
            handle.arms[arm_id] = box
            try:
                send_fd(handle.control, {
                    "type": "TRACE_ARM",
                    "arm_id": arm_id,
                    "peer_ip": peer_ip,
                    "peer_port": peer_port,
                    "limit": limit,
                }, None)
            except OSError as exc:
                handle.arms.pop(arm_id, None)
                raise TraceUnavailable(f"control channel failed: {exc!r}", 503) from exc
            woke = box["event"].wait(ARM_TIMEOUT)
            handle.arms.pop(arm_id, None)
            if not woke:
                raise TraceUnavailable("trace arm timed out", 504)
            msg = box["msg"]
            fd = box["fd"]
            if msg is None:
                # 读线程退出 / worker 死亡
                if fd is not None:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
                raise TraceUnavailable("worker lost during trace arm", 503)
            if not msg.get("ok"):
                if fd is not None:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
                known = [c["peer"] for c in self.list_connections()]
                raise TraceUnavailable(
                    f"no online connection for {peer_ip}:{peer_port}; "
                    f"online={known}", 404,
                )
            rnd = self.traces.start(
                peer_ip=peer_ip,
                peer_port=peer_port,
                limit=limit,
                fd=fd,
                baseline=msg,
                pid=handle.pid,
                revision=handle.revision,
            )
            return rnd.to_dict()

    def query_trace(self, params: dict[str, Any] | None = None) -> dict[str, Any]:
        params = params or {}
        trace_id = params.get("trace_id") or params.get("id")
        peer_ip = params.get("peer_ip", params.get("ip", params.get("host",
                        params.get("peer_host"))))
        peer_port = params.get("peer_port", params.get("port"))
        try:
            if peer_ip is not None or peer_port is not None:
                peer_ip, peer_port = normalize_peer(peer_ip, peer_port,
                                                    peer=params.get("peer")
                                                    or params.get("addr"))
            else:
                peer_ip = peer_port = None
        except ValueError:
            peer_ip = peer_port = None
        # 纯内存 + 本机 socket 计数读取，绝不与 worker/reload 握手交互，
        # 因此换版空窗期也能立即返回最新 ACK 进度。
        return self.traces.query(peer_ip, peer_port, trace_id)

    # ---------------- worker 控制通道读线程 ----------------
    def _start_reader(self, handle: WorkerHandle) -> None:
        stop = threading.Event()
        t = threading.Thread(
            target=self._read_loop, args=(handle, stop),
            name=f"ctrl-read-{handle.pid}", daemon=True,
        )
        handle.reader_stop = stop
        handle.reader_thread = t
        t.start()

    def _stop_reader(self, handle: WorkerHandle) -> None:
        stop = handle.reader_stop
        t = handle.reader_thread
        if stop is not None:
            stop.set()
        if t is not None and t is not threading.current_thread():
            t.join(timeout=2.0)
        handle.reader_stop = None
        handle.reader_thread = None

    def _read_loop(self, handle: WorkerHandle, stop: threading.Event) -> None:
        ctrl = handle.control
        while not stop.is_set():
            try:
                ready, _, _ = select.select([ctrl], [], [], 0.1)
            except (OSError, ValueError):
                break
            if not ready:
                continue
            try:
                msg, fd = recv_fd(ctrl)
            except (BlockingIOError, InterruptedError):
                continue
            except (OSError, EOFError):
                break
            try:
                self._dispatch_worker_message(handle, msg, fd)
            except Exception as exc:  # 读线程绝不能因单条消息异常而退出
                sys.stderr.write(f"[supervisor] dispatch error: {exc!r}\n")
        # 读循环结束：唤醒所有等待 ARM 的请求，避免干等超时
        for box in list(handle.arms.values()):
            if box["msg"] is None:
                box["msg"] = {"type": "WORKER_GONE", "ok": False}
            box["event"].set()

    def _dispatch_worker_message(
        self, handle: WorkerHandle, msg: dict[str, Any], fd: int | None
    ) -> None:
        kind = msg.get("type")
        if kind == "TRACE_ARMED":
            arm_id = msg.get("arm_id")
            box = handle.arms.get(arm_id) if arm_id else None
            if box is None:
                # 请求已超时/清理：关闭收到的 fd 副本，防止泄漏
                if fd is not None:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
                return
            box["msg"] = msg
            box["fd"] = fd
            box["event"].set()
            return
        # 其余附带 fd 的异步消息目前不使用 fd
        if fd is not None and kind != "TRACE_ARMED":
            try:
                os.close(fd)
            except OSError:
                pass
        if kind == "TRACE_ITEM":
            self.traces.handle_item_message(msg)
        elif kind == "CONN_OPEN":
            try:
                self._upsert_peer(str(msg.get("peer_ip", "")),
                                  int(msg.get("peer_port", 0)),
                                  int(msg.get("pid", handle.pid)))
            except (TypeError, ValueError):
                pass
        elif kind == "CONN_CLOSE":
            try:
                ip = str(msg.get("peer_ip", ""))
                port = int(msg.get("peer_port", 0))
            except (TypeError, ValueError):
                return
            self._remove_peer(ip, port)
            # 连接关闭：结算本轮（若正追踪该对端），保留已确认、其余未确认
            self.traces.settle_for_peer(ip, port, "closed")

    # ---------------- 测试注入：换版握手屏障 ----------------
    def _handshake_barrier(self) -> None:
        """仅当设置 RELOAD_BARRIER_DIR 时启用的确定性握手窗口。

        supervisor 收到旧进程交出的全部 socket 后写 ``reached``，
        并等待 ``release`` 文件出现；用于端到端实测“握手期间收到 ACK”。
        未设置环境变量时零行为变化。
        """
        delay = os.environ.get("RELOAD_HANDSHAKE_DELAY")
        if delay:
            try:
                time.sleep(max(0.0, float(delay)))
            except ValueError:
                pass
        gate = os.environ.get("RELOAD_BARRIER_DIR")
        if not gate:
            return
        d = Path(gate)
        try:
            d.mkdir(parents=True, exist_ok=True)
            (d / "reached").write_text(str(os.getpid()))
            deadline = time.monotonic() + 30.0
            release = d / "release"
            while time.monotonic() < deadline and not release.exists():
                time.sleep(0.02)
        except OSError as exc:
            sys.stderr.write(f"[supervisor] barrier error: {exc!r}\n")

    # ---------------- 换版 ----------------
    def reload(self, new_offset: int) -> dict:
        with self.lock:
            old = self.worker
            if old is None:
                raise ReloadFailed("no active worker")
            new_revision = self.revision + 1

            # 1) 旧进程快照（冻结 I/O，交回 fd 与缓冲状态）。
            #    先停掉控制通道读线程，避免与这里的同步 recv 抢消息。
            self._stop_reader(old)
            try:
                old.control.settimeout(HANDSHAKE_TIMEOUT)
                send_fd(old.control, {"type": "SNAPSHOT"}, None)
                # 读线程停止前/worker 冻结前排他产生的异步 TRACE_ITEM /
                # CONN_* 事件可能排在 SNAPSHOT_STATE 前面，先分流处理。
                while True:
                    snap, extra_fd = recv_fd(old.control)
                    if snap.get("type") == "SNAPSHOT_STATE":
                        if extra_fd is not None:
                            try:
                                os.close(extra_fd)
                            except OSError:
                                pass
                        break
                    self._dispatch_worker_message(old, snap, extra_fd)
                n = int(snap.get("n", 0))
                conn_fds: list[int] = []
                for _ in range(n):
                    msg, fd = recv_fd(old.control)
                    if msg.get("type") != "FD" or fd is None:
                        raise ReloadFailed("bad fd in snapshot")
                    conn_fds.append(fd)
                snap_conns = snap.get("conns", [])
                state = {"conns": snap_conns}
            except (OSError, EOFError, TimeoutError) as exc:
                # 快照阶段失败：尝试重启读线程以维持事件处理，再上抛
                try:
                    old.control.setblocking(False)
                except OSError:
                    pass
                self._start_reader(old)
                raise ReloadFailed(f"snapshot failed: {exc!r}") from exc

            # 用快照对账在线连接，并把冻结排空期间产生的追踪记录并入本轮
            enriched = self._reconcile_snapshot_peers(old.pid, snap_conns)
            for ip, port, item in enriched:
                self.traces.merge_snapshot(
                    ip, port, item.get("trace"),
                    handed_total=item.get("handed_total"),
                    gen_total=item.get("gen_total"),
                )

            # 测试/实测钩子：此刻旧进程已交出全部套接字、新进程尚未就绪。
            # HTTP 查询不持有 self.lock，故在屏障等待期间仍可立即读到
            # 内核中新近到达的 ACK。
            self._handshake_barrier()

            # 2) 启动新进程并握手到 READY（暂不 GO，新进程冻结等待放行）；
            #    失败则回退旧进程
            try:
                new = self._spawn(
                    offset=new_offset,
                    revision=new_revision,
                    conn_fds=conn_fds,
                    state=state,
                    send_go=False,
                )
            except ReloadFailed:
                self._rollback(old, conn_fds)
                # 旧进程 RESUME 后继续本轮，重启读线程接收 TRACE_ITEM
                old.control.setblocking(False)
                self._start_reader(old)
                raise

            # 3) 旧进程退出 -> 回收 -> 关闭中转 fd 副本 -> 放行新进程。
            #    严格串行，任何时刻同一连接 fd 只有一个工作进程在操作。
            old.superseded = True
            try:
                old.control.settimeout(HANDSHAKE_TIMEOUT)
                send_fd(old.control, {"type": "QUIT"}, None)
            except OSError:
                pass
            old.control.close()
            self._wait_exit(old.pid, timeout=3.0)
            for fd in conn_fds:  # 旧进程已退出，关闭本进程中转副本
                try:
                    os.close(fd)
                except OSError:
                    pass
            try:
                new.control.settimeout(HANDSHAKE_TIMEOUT)
                send_fd(new.control, {"type": "GO"}, None)
                new.control.setblocking(False)
            except OSError as exc:
                # 极端情况：放行失败，新进程由 watcher 兜底重拉
                raise ReloadFailed(f"GO failed: {exc!r}") from exc

            self.offset = new_offset
            self.revision = new_revision
            self.worker = new

            # 在线连接全部由新进程同 fd 继承：更新 pid 归属
            with self.state_lock:
                for key in list(self._peers.keys()):
                    if self._peers[key].get("pid") == old.pid:
                        self._peers[key]["pid"] = new.pid
            # 成功移交即结算本轮：保留当时已确认，其余标未确认。
            # 必须在关闭追踪 fd 之前完成（settle 内部读取最后一次 TCP 计数）。
            self.traces.settle_current("handover")

            self._start_reader(new)
            self._start_watcher(new)
            return {
                "old_pid": old.pid,
                "pid": new.pid,
                "offset": new_offset,
                "revision": new_revision,
                "inherited_conns": len(conn_fds),
            }

    @staticmethod
    def _wait_exit(pid: int, timeout: float) -> None:
        """有界等待子进程退出；watcher 线程可能已 waitpid 收走。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                reaped, _ = os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                return
            if reaped == pid:
                return
            time.sleep(0.02)

    def _rollback(self, old: WorkerHandle, conn_fds: list[int]) -> None:
        # 先让旧进程解冻并重新接管连接，再关闭本进程的中转 fd 副本。
        # 旧 worker 通过自己的 socket 对象操作连接，不依赖本进程副本。
        try:
            old.control.settimeout(HANDSHAKE_TIMEOUT)
            send_fd(old.control, {"type": "RESUME"}, None)
            old.control.setblocking(False)
        except OSError:
            # 旧进程也已失联：交给 watcher 的崩溃恢复处理
            pass
        for fd in conn_fds:
            try:
                os.close(fd)
            except OSError:
                pass

    # ---------------- 进程拉起与握手 ----------------
    def _spawn(
        self,
        offset: int,
        revision: int,
        conn_fds: list[int],
        state: dict,
        send_go: bool = True,
    ) -> WorkerHandle:
        assert self._listen_sock is not None
        parent_ctrl, child_ctrl = control_pair()
        env = dict(os.environ)
        env.update(
            {
                "WORKER_CONTROL_FD": str(child_ctrl.fileno()),
                "WORKER_OFFSET": str(offset),
                "WORKER_REVISION": str(revision),
            }
        )
        # 确保 exec 出的 worker 能 import 到与本进程相同的依赖路径
        # （Docker 镜像内为系统 site-packages，此项为空；本地 /target 安装时必需）
        extra = [p for p in sys.path if p and os.path.isdir(p)]
        if extra:
            env["PYTHONPATH"] = os.pathsep.join(
                [env.get("PYTHONPATH", "")] + extra
            ).strip(os.pathsep)

        pid = os.fork()
        if pid == 0:  # 子进程
            self._child_exec(child_ctrl, parent_ctrl, env)

        # 父进程
        parent_ctrl.settimeout(HANDSHAKE_TIMEOUT)
        child_ctrl.close()
        handle = WorkerHandle(
            pid=pid,
            control=parent_ctrl,
            offset=offset,
            revision=revision,
            started_at=time.time(),
        )
        try:
            send_fd(
                parent_ctrl,
                {"type": "HELLO", "conns": len(conn_fds)},
                self._listen_sock.fileno(),
            )
            for fd in conn_fds:
                send_fd(parent_ctrl, {"type": "FD"}, fd)
            payload = dict(state)
            payload["type"] = "STATE"
            send_fd(parent_ctrl, payload, None)

            msg, _ = recv_fd(parent_ctrl)
            if msg.get("type") != "READY":
                raise ReloadFailed(f"bad READY: {msg!r}")
            # 初次启动直接 GO；换版时由 reload() 在旧进程退出后再 GO，
            # 保证同一连接 fd 不会被两个进程同时操作。
            if send_go:
                send_fd(parent_ctrl, {"type": "GO"}, None)
        except (OSError, EOFError, TimeoutError) as exc:
            self._abort_child(handle)
            raise ReloadFailed(f"worker handshake failed: {exc!r}") from exc
        finally:
            try:
                parent_ctrl.setblocking(False)
            except OSError:
                pass
        return handle

    @staticmethod
    def _child_exec(
        child_ctrl: socket.socket, parent_ctrl: socket.socket, env: dict
    ) -> None:
        try:
            parent_ctrl.close()
            # PEP 446：所有 socket fd 默认 CLOEXEC，execve 时自动关闭，
            # 因此只需把控制通道标记为可继承；监听/连接 fd 一律经
            # SCM_RIGHTS 在 exec 之后的新进程握手阶段显式转交。
            os.set_inheritable(child_ctrl.fileno(), True)
            os.execve(
                sys.executable,
                [sys.executable, "-m", "app.main", "worker"],
                env,
            )
        except BaseException:  # pragma: no cover - 子进程兜底
            os._exit(127)

    @staticmethod
    def _abort_child(handle: WorkerHandle) -> None:
        try:
            handle.control.close()
        except OSError:
            pass
        try:
            pid, _ = os.waitpid(handle.pid, os.WNOHANG)
            if pid == 0:
                import signal

                os.kill(handle.pid, signal.SIGKILL)
                os.waitpid(handle.pid, 0)
        except ChildProcessError:
            pass
        handle.alive = False

    # ---------------- 崩溃看护（预期外退出时同版重拉） ----------------
    def _start_watcher(self, handle: WorkerHandle) -> None:
        t = threading.Thread(
            target=self._watch, args=(handle,), name=f"watch-{handle.pid}", daemon=True
        )
        t.start()

    def _watch(self, handle: WorkerHandle) -> None:
        try:
            _, status = os.waitpid(handle.pid, 0)
        except ChildProcessError:
            return
        handle.alive = False
        handle.exitcode = os.waitstatus_to_exitcode(status)
        # 唤醒可能正在等待 ARM 的请求
        for box in list(handle.arms.values()):
            if box["msg"] is None:
                box["msg"] = {"type": "WORKER_GONE", "ok": False}
            box["event"].set()
        if handle.superseded:
            return
        sys.stderr.write(
            f"[supervisor] worker pid={handle.pid} died unexpectedly "
            f"(exit={handle.exitcode}), respawning same revision\n"
        )
        with self.lock:
            if self.worker is not handle:
                return
            # 当前 worker 崩溃：其连接全部失效，结算追踪并清空在线表
            self.traces.on_worker_gone(handle.pid)
            with self.state_lock:
                self._peers.clear()
            try:
                fresh = self._spawn(
                    offset=handle.offset,
                    revision=handle.revision,
                    conn_fds=[],
                    state={"conns": []},
                )
                self.worker = fresh
                self._start_reader(fresh)
                self._start_watcher(fresh)
            except ReloadFailed as exc:
                sys.stderr.write(f"[supervisor] respawn failed: {exc!r}\n")
