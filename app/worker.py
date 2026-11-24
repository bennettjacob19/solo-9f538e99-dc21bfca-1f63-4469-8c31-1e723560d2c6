"""温度校正工作进程。

协议与缓冲
----------
1. 探头通过 TCP 长连接上报整数温度，每行至多 32 字节（不含 ``\\n``）。
2. 每条连接维护独立的应用层接收/发送缓冲，各上限 :data:`MAX_BUFFER` (128B)。
   接收缓冲满时只暂停该连接的读入（poll 摘掉 POLLIN），不影响其他连接。
3. 完整一行（收到 ``\\n``）生成且只生成一次回执
   ``校正后整数温度,版号\\n``，校正值 = 上报整数 + 当前偏移。

换版语义（与 supervisor 配合）
------------------------------
- SNAPSHOT 时工作进程冻结全部连接 I/O，把每个连接的 fd 经 SCM_RIGHTS
  交出，并序列化应用层缓冲（半行 recv_buf、未发 send_buf、带版号的
  pending 队列）。新进程凭这些原样恢复，因此：
    * 换版前已收到换行符的行，回执保留旧版号；
    * 只收到半行的，缓冲随 fd 一起交给新进程，补齐后按新版回执；
    * 探头停读或只发半行都不阻塞换版——旧进程直接退出，
      内核 socket 与应用缓冲分别由 fd 继承和状态移交保留；
    * 探头无需重连，其他探头照常收回执。
- 新进程 READY 失败时 supervisor 发 RESUME，旧进程解冻，继续按原版接单。
"""
from __future__ import annotations

import base64
import os
import re
import select
import socket
import sys
import time
from dataclasses import dataclass, field

from .netutil import recv_fd, send_fd

MAX_LINE_BODY = 32          # 每行至多 32 字节（不含换行符）
MAX_BUFFER = 128            # 每条连接应用层收/发缓冲各最多 128 字节
HANDSHAKE_TIMEOUT = 10.0
FROZEN_VERDICT_DEADLINE = 30.0  # 冻结后等待 supervisor 裁决的总时限
_INT_RE = re.compile(rb"^[+-]?\d+$")


@dataclass
class Conn:
    sock: socket.socket
    recv_buf: bytearray = field(default_factory=bytearray)
    send_buf: bytearray = field(default_factory=bytearray)
    # 已从 recv_buf 切出、等待进入发送缓冲的回执：(回执字节, 生成时版号)
    pending: list[tuple[bytes, int]] = field(default_factory=list)
    read_paused: bool = False
    eof: bool = False
    closed: bool = False

    @property
    def fd(self) -> int:
        return self.sock.fileno()

    def mask(self) -> int:
        if self.closed:
            return 0
        mask = 0
        if not self.read_paused and not self.eof:
            mask |= select.POLLIN
        if self.send_buf:
            mask |= select.POLLOUT
        return mask


def _parse_int(line: bytes) -> int | None:
    try:
        text = line.strip()
    except Exception:  # pragma: no cover
        return None
    if not _INT_RE.match(text):  # 严格整数：可选符号 + 纯数字，拒绝 1_0、abc 等
        return None
    try:
        return int(text)
    except ValueError:
        return None


class Worker:
    def __init__(
        self,
        listen_sock: socket.socket,
        control: socket.socket,
        offset: int,
        revision: int,
    ):
        self.offset = offset
        self.revision = revision
        self.control = control
        self.listen_sock = listen_sock
        self.poller = select.poll()
        self.conns: dict[int, Conn] = {}
        self.frozen = False
        self.poller.register(self.control.fileno(), select.POLLIN)
        self.poller.register(self.listen_sock.fileno(), select.POLLIN)

    # ---------------- 主循环 ----------------
    def run(self) -> None:
        while True:
            for fd, events in self.poller.poll(500):
                self._dispatch(fd, events)
            self._reap()

    def _dispatch(self, fd: int, events: int) -> None:
        if fd == self.control.fileno():
            self._on_control()
            return
        if events & (select.POLLERR | select.POLLHUP) and not (
            events & select.POLLIN
        ):
            # 无数据可读且对端挂断/出错：尽力发完缓冲后关闭
            if fd in self.conns:
                conn = self.conns[fd]
                if not self._flush_send(conn):
                    conn.closed = True
            return
        if fd == self.listen_sock.fileno():
            self._on_accept()
        elif fd in self.conns:
            try:
                self._on_conn(self.conns[fd], events)
            except (ConnectionError, OSError) as exc:
                # 单个探头的 RST/错误只关闭该连接，绝不影响工作进程与其他探头
                conn = self.conns.get(fd)
                self._log(f"fd={fd} connection error: {exc!r}")
                if conn is not None:
                    conn.closed = True

    # ---------------- 控制通道 ----------------
    def _on_control(self) -> None:
        try:
            msg, _ = recv_fd(self.control)
        except EOFError:
            os._exit(0)
        kind = msg.get("type")
        if kind in ("QUIT", "SHUTDOWN"):
            self._log(f"{kind} -> exit")
            os._exit(0)
        if kind == "SNAPSHOT":
            self._handle_snapshot()
        else:
            self._log(f"unknown control message: {msg!r}")

    def _handle_snapshot(self) -> None:
        # 冻结：先停 accept，再逐连接停 poll 并排空内核中已到达的数据。
        # 这样“换版前已收到换行符”的行一定在旧进程生成旧版回执；
        # 排空之后才到达的字节属于换版后，半行补齐按新版回执。
        self.frozen = True
        self.control.setblocking(True)
        self.poller.unregister(self.listen_sock.fileno())

        state = {"conns": []}
        live: list[Conn] = []
        for conn in list(self.conns.values()):
            if conn.closed:
                continue
            self._poll_unregister(conn.fd)
            # 已摘除 poll，不会再有事件驱动读入；显式排到 EAGAIN 为止
            self._read_conn(conn)
            self._enqueue_lines(conn)
            live.append(conn)
            state["conns"].append(
                {
                    "recv": base64.b64encode(bytes(conn.recv_buf)).decode(),
                    "send": base64.b64encode(bytes(conn.send_buf)).decode(),
                    "pending": [
                        [base64.b64encode(reply).decode(), rev]
                        for reply, rev in conn.pending
                    ],
                    "eof": conn.eof,
                    "paused": conn.read_paused,
                }
            )
        send_fd(self.control, {"type": "SNAPSHOT_STATE", "n": len(live),
                               "conns": state["conns"]}, None)
        for conn in live:  # 顺序与 state["conns"] 一一对应
            send_fd(self.control, {"type": "FD"}, conn.fd)
        self._log(f"snapshot sent: {len(live)} live conns")

        # 等待裁决：QUIT（新版就绪，旧版退出）或 RESUME（新进程失败，解冻）。
        # 短超时循环避免与 supervisor 握手超时竞争；设总时限，
        # supervisor 异常时旧进程能自我解冻，不会永久冻结停服。
        self.control.settimeout(5.0)
        deadline = time.monotonic() + FROZEN_VERDICT_DEADLINE
        verdict: dict | None = None
        while verdict is None and time.monotonic() < deadline:
            try:
                msg, _ = recv_fd(self.control)
            except TimeoutError:
                continue
            except EOFError:
                os._exit(0)
            if msg.get("type") in ("QUIT", "RESUME"):
                verdict = msg
        if verdict is None:
            self._log("verdict timeout, self-resume")
            verdict = {"type": "RESUME"}
        if verdict["type"] == "QUIT":
            self._log("superseded by new worker -> exit")
            os._exit(0)

        self.control.setblocking(False)
        self.frozen = False
        self.poller.register(self.listen_sock.fileno(), select.POLLIN)
        for conn in live:
            if not conn.closed:
                self.poller.register(conn.fd, conn.mask())
        self._log("resumed after failed handover")

    # ---------------- accept ----------------
    def _on_accept(self) -> None:
        while True:
            try:
                sock, addr = self.listen_sock.accept()
            except BlockingIOError:
                return
            sock.setblocking(False)
            conn = Conn(sock=sock)
            self.conns[conn.fd] = conn
            self.poller.register(conn.fd, select.POLLIN)
            self._log(f"accepted fd={conn.fd} addr={addr}")

    # ---------------- 连接读写 ----------------
    def _on_conn(self, conn: Conn, events: int) -> None:
        if conn.closed:
            return
        if events & select.POLLOUT:
            self._flush_send(conn)
        if events & select.POLLIN:
            self._read_conn(conn)
        self._enqueue_lines(conn)
        if conn.send_buf:
            self._flush_send(conn)
        if conn.eof and not conn.send_buf and not conn.pending and not conn.recv_buf:
            conn.closed = True

    def _read_conn(self, conn: Conn) -> None:
        while True:
            space = MAX_BUFFER - len(conn.recv_buf)
            if space <= 0:  # 应用层接收缓冲满：只暂停该连接读入
                # 缓冲满且没有任何换行 -> 是一个超过 128B 的超长半行，
                # 永远无法成为合法行（行体上限 32B），丢弃并恢复读入，
                # 避免该连接被永久暂停。
                if b"\n" not in conn.recv_buf:
                    self._log(
                        f"fd={conn.fd} unterminated overlong half-line, "
                        f"drop {len(conn.recv_buf)}B"
                    )
                    conn.recv_buf.clear()
                    conn.read_paused = False
                    self._poll_modify(conn)
                    continue
                conn.read_paused = True
                self._poll_modify(conn)
                return
            try:
                chunk = conn.sock.recv(space)
            except BlockingIOError:
                return
            except InterruptedError:
                continue
            if chunk == b"":
                conn.eof = True
                return
            conn.recv_buf.extend(chunk)
            if len(chunk) < space:
                return

    def _enqueue_lines(self, conn: Conn) -> None:
        buf = conn.recv_buf
        while b"\n" in buf:
            nl = buf.index(10)
            line = bytes(buf[:nl])
            del buf[: nl + 1]  # 行一旦切出就不会被重复回执
            if len(line) > MAX_LINE_BODY:
                self._log(f"fd={conn.fd} overlong line {len(line)}B, dropped")
                continue
            value = _parse_int(line)
            if value is None:
                self._log(f"fd={conn.fd} non-integer line {line!r}, dropped")
                continue
            reply = f"{value + self.offset},{self.revision}\n".encode("ascii")
            if len(reply) > MAX_BUFFER:
                self._log(f"fd={conn.fd} reply exceeds buffer cap, dropped")
                continue
            if len(reply) > MAX_BUFFER - len(conn.send_buf):
                conn.pending.append((reply, self.revision))  # FIFO，记下版号
                continue
            conn.send_buf.extend(reply)
        if conn.read_paused and len(conn.recv_buf) < MAX_BUFFER:
            conn.read_paused = False
        self._drain_pending(conn)
        self._poll_modify(conn)

    def _drain_pending(self, conn: Conn) -> None:
        while conn.pending:
            reply, _rev = conn.pending[0]
            if len(reply) > MAX_BUFFER - len(conn.send_buf):
                return
            conn.send_buf.extend(reply)
            conn.pending.pop(0)

    def _flush_send(self, conn: Conn) -> bool:
        """尽量发送发送缓冲。返回是否已全部发完。"""
        buf = conn.send_buf
        while buf:
            try:
                sent = conn.sock.send(buf)
            except BlockingIOError:
                break
            except InterruptedError:
                continue
            except OSError:
                conn.closed = True
                return False
            if sent <= 0:
                break
            del buf[:sent]
        self._drain_pending(conn)
        if conn.read_paused and len(conn.recv_buf) < MAX_BUFFER:
            conn.read_paused = False
        self._poll_modify(conn)
        return not buf

    # ---------------- 状态快照/恢复 ----------------
    def load_state(self, state: dict, conn_socks: list[socket.socket]) -> None:
        for sock, item in zip(conn_socks, state.get("conns", [])):
            sock.setblocking(False)
            conn = Conn(
                sock=sock,
                recv_buf=bytearray(base64.b64decode(item["recv"])),
                send_buf=bytearray(base64.b64decode(item["send"])),
                pending=[
                    (base64.b64decode(reply), rev)
                    for reply, rev in item.get("pending", [])
                ],
                eof=bool(item.get("eof")),
                read_paused=bool(item.get("paused")),
            )
            # 缓冲上限不变量校验，防御异常快照
            if len(conn.recv_buf) > MAX_BUFFER or len(conn.send_buf) > MAX_BUFFER:
                raise ValueError("inherited state exceeds buffer cap")
            self.conns[conn.fd] = conn
            self.poller.register(conn.fd, conn.mask())

    def snapshot_fds(self) -> list[int]:
        return [c.fd for c in self.conns.values() if not c.closed]

    # ---------------- 杂项 ----------------
    def _poll_modify(self, conn: Conn) -> None:
        mask = conn.mask()
        if mask:
            self._poll_register(conn.fd, mask)
        else:
            self._poll_unregister(conn.fd)

    def _poll_register(self, fd: int, mask: int) -> None:
        try:
            self.poller.modify(fd, mask)
        except FileNotFoundError:  # 未注册过 -> 新注册
            try:
                self.poller.register(fd, mask)
            except FileNotFoundError:
                pass

    def _poll_unregister(self, fd: int) -> None:
        try:
            self.poller.unregister(fd)
        except (FileNotFoundError, KeyError):
            pass

    def _reap(self) -> None:
        for conn in [c for c in self.conns.values() if c.closed]:
            self._poll_unregister(conn.fd)
            try:
                conn.sock.close()
            except OSError:
                pass
            self.conns.pop(conn.fd, None)

    def _log(self, msg: str) -> None:
        sys.stderr.write(
            f"[worker pid={os.getpid()} rev={self.revision}] {msg}\n"
        )
        sys.stderr.flush()


# ---------------- 进程入口（fresh start 与 exec 后共用） ----------------
def worker_entry() -> None:
    # 测试用：模拟新工作进程启动失败。必须在任何握手之前退出，
    # 这样旧工作进程仍冻结在 SNAPSHOT 阶段，supervisor 可安全 RESUME 回退。
    marker = os.environ.get("WORKER_FAIL_MARKER")
    if marker and os.path.exists(marker):
        sys.stderr.write("[worker] boot failure injected\n")
        sys.stderr.flush()
        os._exit(2)

    control_fd = int(os.environ["WORKER_CONTROL_FD"])
    offset = int(os.environ["WORKER_OFFSET"])
    revision = int(os.environ["WORKER_REVISION"])

    control = socket.socket(fileno=control_fd)
    control.setblocking(True)
    control.settimeout(HANDSHAKE_TIMEOUT)

    hello, listen_fd = recv_fd(control)
    if hello.get("type") != "HELLO" or listen_fd is None:
        sys.stderr.write("[worker] bad HELLO\n")
        os._exit(2)
    n = int(hello.get("conns", 0))

    conn_socks: list[socket.socket] = []
    for _ in range(n):
        msg, fd = recv_fd(control)
        if msg.get("type") != "FD" or fd is None:
            sys.stderr.write("[worker] bad FD record\n")
            os._exit(2)
        conn_socks.append(socket.socket(fileno=fd))

    state, _ = recv_fd(control)
    if state.get("type") != "STATE":
        sys.stderr.write("[worker] bad STATE record\n")
        os._exit(2)

    listen_sock = socket.socket(fileno=listen_fd)
    listen_sock.setblocking(False)
    worker = Worker(listen_sock, control, offset, revision)
    worker.load_state(state, conn_socks)

    send_fd(control, {"type": "READY", "pid": os.getpid()}, None)
    go, _ = recv_fd(control)  # supervisor 确认接管后才放行
    if go.get("type") != "GO":
        os._exit(2)

    control.setblocking(False)
    worker._log(
        f"ready offset={offset} inherited_conns={len(conn_socks)}"
    )
    worker.run()
