"""回执追踪轮次与 TCP 级确认进度判定。

一轮（round）追踪某条探头上“接下来生成的”至多 :data:`MAX_TRACE_ITEMS` 条
回执。轮次对象保存在 **supervisor 进程** 内，并持有一个经 SCM_RIGHTS 得到的
探头 socket 副本 fd，因此：

- 查询进度只是一次 ``TCP_INFO`` / ``SIOCOUTQ`` 读取，O(1)，
  不扫描连接的收/发积压，也不需要询问工作进程；
- 换版旧进程交出套接字后的空窗期，socket 仍在内核中，对端 ACK 由内核处理，
  supervisor 用自己持有的 fd 一样能读到最新确认进度；
- 工作进程接管 / 恢复、HTTP 查询之间互不阻塞（各用各的锁）。

进度模型（与需求逐条对应）
==========================
- :data:`PENDING` 待发：整条回执尚未全部 ``send()`` 给本机 TCP；
- :data:`HANDED_OVER` 已交出：整行字节都已被本机内核接受；
- :data:`ACKED` 已确认：对端 TCP 累计确认已越过本行行尾；
- :data:`UNCONFIRMED` 未确认：连接关闭或成功移交而结算时，
  除当时已确认外的其余记录的终态。

坐标：工作进程维护连接级单调计数器 ``gen_total``（已生成回执字节总数）与
``handed_total``（已成功 ``send()`` 字节总数），并随快照跨换版连续传递。
每条回执记录其绝对字节区间 ``[start_abs, end_abs)``，原点为该连接第一个
服务端→探头 payload 字节。内核 ``tcp_info.tcpi_bytes_acked`` 正是同一坐标下
对端累计确认的 payload 字节数；``SIOCOUTQ = handed - acked``
（本内核头文件注释：output queue size, not sent + not acked）。
"""
from __future__ import annotations

import fcntl
import socket
import struct
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any

MIN_TRACE_ITEMS = 1
MAX_TRACE_ITEMS = 64

# 轮次 / 记录状态码（英文码便于程序断言，to_dict 同时给中文标签与多组别名）
ACTIVE = "active"
SETTLED = "settled"
IDLE = "idle"

PENDING = "pending"            # 待发
HANDED_OVER = "handed_over"    # 已交出（全部交给本机 TCP）
ACKED = "acked"                # 已确认（对端累计 ACK 到行尾）
UNCONFIRMED = "unconfirmed"    # 结算后的未确认终态

STATE_CN: dict[str, str] = {
    PENDING: "待发",
    HANDED_OVER: "已交出",
    ACKED: "已确认",
    UNCONFIRMED: "未确认",
    ACTIVE: "追踪中",
    SETTLED: "已结算",
    IDLE: "无追踪",
}

_STAGE = {PENDING: 0, HANDED_OVER: 1, ACKED: 2, UNCONFIRMED: 3}
_PERCENT = {PENDING: 10, HANDED_OVER: 60, ACKED: 100, UNCONFIRMED: 0}

# Linux ioctl / socket 常量（不依赖 Python 是否导出）
_SIOCOUTQ = 0x5411
_TCP_INFO = getattr(socket, "TCP_INFO", 11)
# struct tcp_info 中 tcpi_bytes_acked 的固定偏移（见 /usr/include/linux/tcp.h）
_BYTES_ACKED_OFFSET = 120

# tcpi_state 取值（netinet/tcp.h）
_TCP_CLOSE = 7
_TCP_CLOSE_WAIT = 8
_TCP_LAST_ACK = 9
_TCP_CLOSING = 11
_PEER_GONE_STATES = {_TCP_CLOSE, _TCP_CLOSE_WAIT, _TCP_LAST_ACK, _TCP_CLOSING}


def normalize_limit(value: Any) -> int:
    """把外部入参规范化为 1..64 的整数；非法时抛 ValueError。"""
    if isinstance(value, bool):  # bool 是 int 子类，拒绝 True/False
        raise ValueError("count must be an integer")
    try:
        n = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError("count must be an integer 1..64") from exc
    if not MIN_TRACE_ITEMS <= n <= MAX_TRACE_ITEMS:
        raise ValueError("count must be between 1 and 64")
    return n


def normalize_peer(
    peer_ip: Any = None,
    peer_port: Any = None,
    *,
    peer: Any = None,
) -> tuple[str, int]:
    """从各种别名字段中解析 ``(peer_ip, peer_port)``。

    支持 ``"1.2.3.4:5"`` / ``"[::1]:5"`` 形式的合并字符串。
    """
    ip = peer_ip
    port = peer_port
    # 容错：即便合并串被放进了 ip/peer_ip 字段，也尝试拆出端口
    if port is None and isinstance(ip, str) and ":" in ip and ip.count(".") == 3:
        host, _, rest = ip.rpartition(":")
        ip = host
        try:
            port = int(rest)
        except ValueError:
            port = None
    if (ip is None or port is None) and isinstance(peer, str) and peer:
        text = peer.strip()
        if text.startswith("[") and "]" in text:  # [ipv6]:port
            host, _, rest = text[1:].partition("]")
            ip = ip or host
            rest = rest.lstrip(":")
            if port is None and rest:
                port = rest
        elif text.count(":") >= 1 and text.count(".") == 3:  # ipv4:port
            host, _, rest = text.rpartition(":")
            ip = ip or host
            if port is None:
                port = rest
        elif text.count(":") == 1:  # host:port（含裸 ipv6 不处理）
            host, _, rest = text.partition(":")
            ip = ip or host
            if port is None:
                port = rest
    if ip is not None:
        ip = str(ip).strip()
        if ip.startswith("::ffff:"):
            ip = ip[len("::ffff:"):]
    try:
        port_i = int(port)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError("peer_port required") from exc
    if not (0 < port_i < 65536):
        raise ValueError("peer_port out of range")
    if not ip:
        raise ValueError("peer_ip required")
    return ip, port_i


@dataclass
class TcpCounters:
    acked_abs: int | None
    outq: int | None
    tcp_state: int | None

    @property
    def handed_abs(self) -> int | None:
        # SIOCOUTQ = handed - acked（同一 payload 字节坐标）
        if self.acked_abs is not None and self.outq is not None:
            return self.acked_abs + self.outq
        return None


def read_tcp_counters(sock: socket.socket | None) -> TcpCounters | None:
    """在 supervisor 持有的 socket 副本上 O(1) 读取内核 TCP 计数。"""
    if sock is None:
        return None
    try:
        info = sock.getsockopt(socket.IPPROTO_TCP, _TCP_INFO, 256)
    except OSError:
        return None
    tcp_state: int | None = info[0] if info else None
    acked: int | None = None
    if len(info) >= _BYTES_ACKED_OFFSET + 8:
        try:
            acked = struct.unpack_from("=Q", info, _BYTES_ACKED_OFFSET)[0]
        except struct.error:
            acked = None
    outq: int | None = None
    try:
        buf = bytearray(struct.calcsize("i"))
        fcntl.ioctl(sock.fileno(), _SIOCOUTQ, buf)
        outq = struct.unpack("=i", bytes(buf))[0]
        if outq < 0:
            outq = 0
    except OSError:
        outq = None
    return TcpCounters(acked_abs=acked, outq=outq, tcp_state=tcp_state)


def present_item(raw: dict[str, Any], state: str) -> dict[str, Any]:
    """构造单条回执的对外 JSON（携带多组字段别名，方便不同调用方）。"""
    idx = int(raw["index"])
    value = raw["value"]
    rev = raw["revision"]
    label = STATE_CN[state]
    return {
        # 主键
        "index": idx,
        "corrected_value": value,
        "revision": rev,
        "progress": state,
        "state": state,
        "status": state,
        # 常见别名
        "seq": idx,
        "n": idx,
        "order": idx,
        "value": value,
        "correction": value,
        "correction_value": value,
        "result": value,
        "rev": rev,
        "version": rev,
        "progress_code": state,
        "state_code": state,
        # 中文进度
        "progress_cn": label,
        "state_cn": label,
        "status_cn": label,
        "label": label,
        "progress_label": label,
        "status_text": label,
        "progress_text": label,
        # 数值化进度
        "stage": _STAGE[state],
        "progress_stage": _STAGE[state],
        "percent": _PERCENT[state],
        "progress_percent": _PERCENT[state],
        # 布尔位
        "pending": state == PENDING,
        "is_pending": state == PENDING,
        "handed_over": state in (HANDED_OVER, ACKED),
        "handed": state in (HANDED_OVER, ACKED),
        "sent": state in (HANDED_OVER, ACKED),
        "delivered": state in (HANDED_OVER, ACKED),
        "acked": state == ACKED,
        "acknowledged": state == ACKED,
        "confirmed": state == ACKED,
        "is_acked": state == ACKED,
        "unconfirmed": state == UNCONFIRMED,
        "settled": state == UNCONFIRMED,
        # 调试 / 实测用的字节坐标
        "length": int(raw.get("length", 0)),
        "start_abs": int(raw.get("start_abs", 0)),
        "end_abs": int(raw.get("end_abs", 0)),
        # 中文键（直接对应“校正值、版号、进度”）
        "校正值": value,
        "版号": rev,
        "进度": label,
    }


class TraceRound:
    """一次追踪轮次；线程安全由 TraceManager 的锁保证。"""

    def __init__(
        self,
        *,
        trace_id: str,
        peer_ip: str,
        peer_port: int,
        limit: int,
        probe_sock: socket.socket | None,
        baseline: dict[str, Any] | None = None,
        pid: int | None = None,
        revision: int | None = None,
    ) -> None:
        self.trace_id = trace_id
        self.peer_ip = peer_ip
        self.peer_port = peer_port
        self.limit = limit
        self.probe_sock = probe_sock
        self.pid = pid
        self.revision = revision
        self.created_at = time.time()
        self.updated_at = self.created_at
        self.state = ACTIVE
        self.settle_reason: str | None = None
        self.items: list[dict[str, Any]] = []
        # 工作进程上报的“已交给本机 TCP”字节数下限（绝对坐标）
        self.worker_handed_total: int | None = (baseline or {}).get("handed_total")
        self.worker_gen_total: int | None = (baseline or {}).get("gen_total")
        # 结算后的冻结视图
        self._frozen_items: list[dict[str, Any]] | None = None

    @property
    def key(self) -> tuple[str, int]:
        return self.peer_ip, self.peer_port

    # ---------------- 记录维护 ----------------
    def upsert_item(self, item: dict[str, Any]) -> None:
        idx = int(item["index"])
        if idx < 1 or idx > self.limit:
            return
        for pos, old in enumerate(self.items):
            if int(old["index"]) == idx:
                self.items[pos] = {**old, **item}
                self.updated_at = time.time()
                return
        self.items.append(dict(item))
        self.items.sort(key=lambda x: int(x["index"]))
        self.updated_at = time.time()
        ht = item.get("handed_total")
        if isinstance(ht, int):
            self.worker_handed_total = max(self.worker_handed_total or 0, ht)

    def merge_worker_items(self, items: list[dict[str, Any]]) -> None:
        """合并工作进程快照中的镜像（按 index 幂等去重）。"""
        for item in items or []:
            try:
                self.upsert_item(item)
            except (KeyError, TypeError, ValueError):
                continue

    # ---------------- 进度判定 ----------------
    def _live_states(self) -> list[str]:
        counters = read_tcp_counters(self.probe_sock)
        acked_abs: int | None = None
        handed_abs: int | None = None
        if counters is not None:
            acked_abs = counters.acked_abs
            handed_abs = counters.handed_abs
        if self.worker_handed_total is not None:
            handed_abs = max(handed_abs or 0, int(self.worker_handed_total))
        if acked_abs is None and handed_abs is not None and counters is not None \
                and counters.outq is not None:
            acked_abs = max(0, handed_abs - counters.outq)

        states: list[str] = []
        for raw in self.items:
            end_abs = int(raw.get("end_abs", 0))
            if acked_abs is not None and end_abs <= acked_abs:
                states.append(ACKED)
            elif handed_abs is not None and end_abs <= handed_abs:
                states.append(HANDED_OVER)
            else:
                states.append(PENDING)
        return states

    # ---------------- 结算 / 关闭 ----------------
    def settle(self, reason: str) -> None:
        if self.state == SETTLED:
            return
        try:
            states = self._live_states()
        except Exception:
            states = [PENDING] * len(self.items)
        frozen: list[dict[str, Any]] = []
        for raw, live in zip(self.items, states):
            final = ACKED if live == ACKED else UNCONFIRMED
            frozen.append(present_item(raw, final))
        self._frozen_items = frozen
        self.state = SETTLED
        self.settle_reason = reason
        self.updated_at = time.time()
        self.close_fd()

    def close_fd(self) -> None:
        sock = self.probe_sock
        self.probe_sock = None
        if sock is not None:
            try:
                sock.close()  # 恰好关闭 supervisor 持有的这一个 dup
            except OSError:
                pass

    # ---------------- 序列化 ----------------
    def to_dict(self) -> dict[str, Any]:
        if self.state == SETTLED and self._frozen_items is not None:
            presented = self._frozen_items
        else:
            states = self._live_states()
            presented = [present_item(raw, st) for raw, st in zip(self.items, states)]
        acked_n = sum(1 for i in presented if i["state"] == ACKED)
        unconfirmed_n = sum(1 for i in presented if i["state"] == UNCONFIRMED)
        pending_n = sum(1 for i in presented if i["state"] == PENDING)
        handed_n = sum(1 for i in presented if i["state"] == HANDED_OVER)
        ip, port = self.key
        label = STATE_CN[self.state]
        return {
            "trace_id": self.trace_id,
            "id": self.trace_id,
            "peer_ip": ip,
            "peer_port": port,
            "ip": ip,
            "host": ip,
            "peer_host": ip,
            "port": port,
            "peer": f"{ip}:{port}",
            "addr": f"{ip}:{port}",
            "address": f"{ip}:{port}",
            "count": self.limit,
            "limit": self.limit,
            "n": self.limit,
            "max_records": self.limit,
            "selected": self.limit,
            "expected_count": self.limit,
            "generated_count": len(self.items),
            "item_count": len(self.items),
            "total": len(self.items),
            "state": self.state,
            "status": self.state,
            "round_state": self.state,
            "state_cn": label,
            "status_cn": label,
            "active": self.state == ACTIVE,
            "settled": self.state == SETTLED,
            "done": self.state == SETTLED,
            "settle_reason": self.settle_reason,
            "reason": self.settle_reason,
            "pid": self.pid,
            "worker_pid": self.pid,
            "revision": self.revision,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "items": presented,
            "receipts": presented,
            "records": presented,
            "results": presented,
            "summary": {
                "total": len(presented),
                "count": self.limit,
                "acked": acked_n,
                "acknowledged": acked_n,
                "handed_over": handed_n,
                "pending": pending_n,
                "unconfirmed": unconfirmed_n,
            },
            # 中文键
            "对端": f"{ip}:{port}",
            "条数": self.limit,
            "轮次状态": label,
        }


def idle_dict() -> dict[str, Any]:
    return {
        "trace_id": None,
        "id": None,
        "state": IDLE,
        "status": IDLE,
        "round_state": IDLE,
        "state_cn": STATE_CN[IDLE],
        "active": False,
        "settled": False,
        "items": [],
        "receipts": [],
        "records": [],
        "results": [],
        "count": None,
        "summary": {"total": 0, "acked": 0, "handed_over": 0,
                    "pending": 0, "unconfirmed": 0},
    }


class TraceManager:
    """全局只保留最近一轮（“只保留最近一轮”）。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.current: TraceRound | None = None

    # ---------------- 开始 / 替换 ----------------
    def start(
        self,
        *,
        peer_ip: str,
        peer_port: int,
        limit: int,
        fd: int | None,
        baseline: dict[str, Any] | None = None,
        pid: int | None = None,
        revision: int | None = None,
    ) -> TraceRound:
        with self._lock:
            if self.current is not None:
                self.current.close_fd()  # 丢弃上一轮，释放其 dup
            probe_sock: socket.socket | None = None
            if fd is not None:
                # 接管这个 SCM_RIGHTS 收到的 fd 的唯一所有权，
                # 之后只通过 round.close_fd() 关闭一次。
                probe_sock = socket.socket(fileno=fd)
            rnd = TraceRound(
                trace_id=uuid.uuid4().hex,
                peer_ip=peer_ip,
                peer_port=peer_port,
                limit=limit,
                probe_sock=probe_sock,
                baseline=baseline,
                pid=pid,
                revision=revision,
            )
            self.current = rnd
            return rnd

    # ---------------- 事件 ----------------
    def handle_item_message(self, msg: dict[str, Any]) -> None:
        with self._lock:
            rnd = self.current
            if rnd is None or rnd.state != ACTIVE:
                return
            if not self._msg_matches(rnd, msg):
                return
            item = msg.get("item")
            if isinstance(item, dict):
                rnd.upsert_item(item)

    def merge_snapshot(
        self,
        peer_ip: str,
        peer_port: int,
        trace_payload: dict[str, Any] | None,
        handed_total: int | None = None,
        gen_total: int | None = None,
    ) -> None:
        with self._lock:
            rnd = self.current
            if rnd is None or rnd.state != ACTIVE or rnd.key != (peer_ip, peer_port):
                return
            if isinstance(handed_total, int):
                rnd.worker_handed_total = max(rnd.worker_handed_total or 0, handed_total)
            if isinstance(gen_total, int):
                rnd.worker_gen_total = gen_total
            if isinstance(trace_payload, dict):
                rnd.merge_worker_items(trace_payload.get("items", []))

    @staticmethod
    def _msg_matches(rnd: TraceRound, msg: dict[str, Any]) -> bool:
        ip = msg.get("peer_ip")
        port = msg.get("peer_port")
        try:
            port_i = int(port) if port is not None else None
        except (TypeError, ValueError):
            port_i = None
        # 工作进程消息尽量带对端；缺字段时不丢弃（全局只有一轮）
        if ip is not None and port_i is not None:
            return rnd.key == (str(ip), port_i)
        return True

    # ---------------- 结算 ----------------
    def settle_current(self, reason: str) -> dict[str, Any] | None:
        with self._lock:
            rnd = self.current
            if rnd is None:
                return None
            rnd.settle(reason)
            return rnd.to_dict()

    def settle_for_peer(self, peer_ip: str, peer_port: int, reason: str) -> bool:
        with self._lock:
            rnd = self.current
            if rnd is None or rnd.key != (peer_ip, peer_port):
                return False
            rnd.settle(reason)
            return True

    def on_worker_gone(self, pid: int | None = None) -> None:
        """当前工作进程意外退出：其连接全部消失，结算本轮并释放 fd。"""
        with self._lock:
            rnd = self.current
            if rnd is None or rnd.state == SETTLED:
                return
            if pid is not None and rnd.pid is not None and rnd.pid != pid:
                return
            rnd.settle("worker_exit")

    # ---------------- 查询 ----------------
    def get_round(
        self,
        peer_ip: str | None = None,
        peer_port: int | None = None,
        trace_id: str | None = None,
        *,
        auto_settle_close: bool = True,
    ) -> tuple[TraceRound | None, bool]:
        """返回 (round, peer_matched)。

        无轮次时 round=None；给了对端但与当前全局轮次不符时
        ``peer_matched=False``。查询中若发现对端已关闭，顺手结算。
        """
        with self._lock:
            rnd = self.current
            if rnd is None:
                return None, True
            if trace_id is not None and rnd.trace_id != trace_id:
                return None, True
            peer_matched = True
            if peer_ip is not None and peer_port is not None:
                peer_matched = rnd.key == (peer_ip, peer_port)
            if not peer_matched:
                return rnd, False
            if rnd.state == ACTIVE and auto_settle_close:
                counters = read_tcp_counters(rnd.probe_sock)
                if counters is not None and counters.tcp_state in _PEER_GONE_STATES:
                    rnd.settle("closed")
            return rnd, True

    def query(
        self,
        peer_ip: str | None = None,
        peer_port: int | None = None,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        rnd, matched = self.get_round(peer_ip, peer_port, trace_id)
        if rnd is None:
            return idle_dict()
        if not matched:
            # 全局只保留最近一轮：查询的对端并非当前轮次时，对该对端而言无轮次
            data = idle_dict()
            data["matched"] = False
            data["current_peer"] = f"{rnd.peer_ip}:{rnd.peer_port}"
            data["current_trace_id"] = rnd.trace_id
            return data
        data = rnd.to_dict()
        data["matched"] = True
        return data

    def shutdown(self) -> None:
        with self._lock:
            if self.current is not None:
                self.current.close_fd()
                self.current = None
