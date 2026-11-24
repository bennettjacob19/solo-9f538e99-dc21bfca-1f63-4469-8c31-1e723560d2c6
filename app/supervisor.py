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
import socket
import sys
import threading
import time
from dataclasses import dataclass

from .netutil import control_pair, recv_fd, send_fd

HANDSHAKE_TIMEOUT = 10.0


class ReloadFailed(RuntimeError):
    """新工作进程启动失败，旧进程仍在服务。"""


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


class Supervisor:
    def __init__(self, host: str, tcp_port: int, initial_offset: int = 0):
        self.host = host
        self.tcp_port = tcp_port
        self.offset = initial_offset
        self.revision = 1
        self.lock = threading.RLock()
        self.worker: WorkerHandle | None = None
        self._listen_sock: socket.socket | None = None

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
        self._start_watcher(self.worker)

    def shutdown(self) -> None:
        with self.lock:
            worker = self.worker
            if worker is None:
                return
            worker.superseded = True
            try:
                send_fd(worker.control, {"type": "SHUTDOWN"}, None)
            except OSError:
                pass
            worker.control.close()
            self.worker = None

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

    # ---------------- 换版 ----------------
    def reload(self, new_offset: int) -> dict:
        with self.lock:
            old = self.worker
            if old is None:
                raise ReloadFailed("no active worker")
            new_revision = self.revision + 1

            # 1) 旧进程快照（冻结 I/O，交回 fd 与缓冲状态）
            try:
                old.control.settimeout(HANDSHAKE_TIMEOUT)
                send_fd(old.control, {"type": "SNAPSHOT"}, None)
                snap, _ = recv_fd(old.control)
                if snap.get("type") != "SNAPSHOT_STATE":
                    raise ReloadFailed(f"bad snapshot header: {snap!r}")
                n = int(snap.get("n", 0))
                conn_fds: list[int] = []
                for _ in range(n):
                    msg, fd = recv_fd(old.control)
                    if msg.get("type") != "FD" or fd is None:
                        raise ReloadFailed("bad fd in snapshot")
                    conn_fds.append(fd)
                state = {"conns": snap.get("conns", [])}
            except (OSError, EOFError, TimeoutError) as exc:
                raise ReloadFailed(f"snapshot failed: {exc!r}") from exc

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
        if handle.superseded:
            return
        sys.stderr.write(
            f"[supervisor] worker pid={handle.pid} died unexpectedly "
            f"(exit={handle.exitcode}), respawning same revision\n"
        )
        with self.lock:
            if self.worker is not handle:
                return
            try:
                fresh = self._spawn(
                    offset=handle.offset,
                    revision=handle.revision,
                    conn_fds=[],
                    state={"conns": []},
                )
                self.worker = fresh
                self._start_watcher(fresh)
            except ReloadFailed as exc:
                sys.stderr.write(f"[supervisor] respawn failed: {exc!r}\n")
