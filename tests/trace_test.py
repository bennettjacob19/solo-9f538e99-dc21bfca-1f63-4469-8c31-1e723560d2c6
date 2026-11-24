"""回执追踪特性的端到端实测：真实子进程 + 真实 TCP/HTTP，无 mock。

覆盖需求逐条：
- GET /connections 按对端 IP/端口选中在线连接；/docs(openapi) 含新接口
- POST /trace 追踪接下来生成的 1..64 条回执，只保留最近一轮
- GET /trace 逐条返回校正值、版号、进度（待发/已交出/已确认/未确认）
- 探头不再上报也能查到当前进度（纯 TCP 计数器读取）
- 换版握手空窗期查询不等待、期间收到的 ACK 查得到（RELOAD_BARRIER_DIR 实测）
- 成功移交结算本轮：已确认保留，其余未确认
- 换版失败 RESUME 原连接：继续本轮
- 连接关闭结算本轮
- 开始追踪/查询不逐条扫描旧积压；额外记录至多本轮条数
- 追踪不推迟探头套接字关闭

直接运行：``python tests/trace_test.py``（也可被 pytest 收集）。
"""
from __future__ import annotations

import shutil
import socket
import sys
import tempfile
import threading
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, "/tmp/pylibs")

import httpx  # noqa: E402

from app.trace import (  # noqa: E402
    ACKED,
    HANDED_OVER,
    PENDING,
    UNCONFIRMED,
    TraceRound,
)
from tests.integration_test import FAIL_MARKER, Server, recv_line  # noqa: E402

IP = "127.0.0.1"
results: list[tuple[str, bool, str]] = []


def check(name: str, fn) -> None:
    try:
        fn()
        results.append((name, True, ""))
        print(f"PASS  {name}")
    except Exception as exc:  # noqa: BLE001
        results.append((name, False, repr(exc)))
        print(f"FAIL  {name}: {exc!r}")


def clamped_connect(tcp_port: int, rcvbuf: int = 4096, clamp: int = 1024) -> socket.socket:
    """小接收窗口的探头：不读时稳定制造“已交出未确认”。"""
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
    s.setsockopt(socket.IPPROTO_TCP, 10, int(clamp).to_bytes(4, "little"))
    s.connect((IP, tcp_port))
    return s


def wait_items(client: httpx.Client, base: str, port: int, n: int,
               timeout: float = 6.0) -> dict:
    deadline = time.time() + timeout
    data = {}
    while time.time() < deadline:
        r = client.get(f"{base}/trace", params={"peer_ip": IP, "peer_port": port},
                       timeout=5)
        data = r.json()
        if len(data.get("items", [])) >= n:
            return data
        time.sleep(0.05)
    raise AssertionError(f"only {len(data.get('items', []))}/{n} items: {data!r}")


def wait_all_state(base: str, port: int, state: str, timeout: float = 8.0) -> dict:
    deadline = time.time() + timeout
    data = {}
    while time.time() < deadline:
        data = httpx.get(f"{base}/trace",
                         params={"peer_ip": IP, "peer_port": port}, timeout=5).json()
        items = data.get("items", [])
        if items and all(i["state"] == state for i in items):
            return data
        time.sleep(0.08)
    dist = Counter(i["state"] for i in data.get("items", []))
    raise AssertionError(f"not all {state}: {dict(dist)}")


def wait_settled(base: str, port: int | None = None, timeout: float = 4.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        params = {"peer_ip": IP, "peer_port": port} if port else None
        d = httpx.get(f"{base}/trace", params=params, timeout=5).json()
        if d.get("state") == "settled":
            return d
        time.sleep(0.03)
    raise AssertionError(f"round not settled: {d.get('state')} {d.get('settle_reason')}")


def large_lines(prefix_base: int, n: int) -> bytes:
    return b"".join(f"{prefix_base + i}\n".encode() for i in range(n))


# ---------------- 用例 ----------------
def test_connections_listing_and_docs() -> None:
    srv = Server()
    try:
        a = srv.connect()
        b = srv.connect()
        pa, pb = a.getsockname()[1], b.getsockname()[1]
        time.sleep(0.3)  # 等 CONN_OPEN 事件到 supervisor

        r = httpx.get(f"{srv.base}/connections", timeout=5)
        assert r.status_code == 200
        body = r.json()
        assert "connections" in body and body["count"] >= 2
        ports = {c["peer_port"] for c in body["connections"]}
        assert pa in ports and pb in ports
        entry = next(c for c in body["connections"] if c["peer_port"] == pa)
        assert entry["peer_ip"] == IP and entry["online"] is True
        assert entry["peer"] == f"{IP}:{pa}"

        arr = httpx.get(f"{srv.base}/connections?format=array", timeout=5).json()
        assert isinstance(arr, list) and any(c["peer_port"] == pb for c in arr)
        for alias in ("/conns", "/peers"):
            assert httpx.get(f"{srv.base}{alias}", timeout=5).status_code == 200

        spec = httpx.get(f"{srv.base}/openapi.json", timeout=5).json()
        assert "/trace" in spec["paths"] and "/connections" in spec["paths"]
        docs = httpx.get(f"{srv.base}/docs", timeout=5)
        assert docs.status_code == 200

        a.close(); b.close()
    finally:
        srv.stop()


def test_basic_values_revision_and_ack() -> None:
    srv = Server()
    try:
        c = srv.connect()
        port = c.getsockname()[1]
        httpx.post(f"{srv.base}/trace",
                   json={"peer_ip": IP, "peer_port": port, "count": 3}, timeout=6)
        for v in (25, -3, 100):
            c.sendall(f"{v}\n".encode())
        for expected in (b"25,1\n", b"-3,1\n", b"100,1\n"):
            assert recv_line(c) == expected

        data = wait_all_state(srv.base, port, ACKED)
        assert [i["corrected_value"] for i in data["items"]] == [25, -3, 100]
        assert [i["revision"] for i in data["items"]] == [1, 1, 1]
        for i in data["items"]:
            assert i["state"] == ACKED and i["acked"] is True
            assert i["progress_cn"] == "已确认" and i["校正值"] == i["corrected_value"]
            assert i["版号"] == 1 and i["进度"] == "已确认"

        # 无参数 GET 返回全局最近一轮
        latest = httpx.get(f"{srv.base}/trace/latest", timeout=5).json()
        assert latest["trace_id"] == data["trace_id"]
        c.close()
    finally:
        srv.stop()


def test_validation_cap_and_latest_round() -> None:
    srv = Server()
    try:
        c = srv.connect()
        port = c.getsockname()[1]
        time.sleep(0.2)

        for bad in (0, 65, -1):
            r = httpx.post(f"{srv.base}/trace",
                           json={"peer_ip": IP, "peer_port": port, "count": bad}, timeout=5)
            assert 400 <= r.status_code < 500, (bad, r.status_code)
        r = httpx.post(f"{srv.base}/trace",
                       json={"peer_ip": IP, "peer_port": port, "count": "abc"}, timeout=5)
        assert 400 <= r.status_code < 500

        # 未知对端 -> 404
        r = httpx.post(f"{srv.base}/trace",
                       json={"peer_ip": IP, "peer_port": 59999, "count": 3}, timeout=6)
        assert r.status_code == 404, r.status_code

        # 别名 addr=n 也能开始
        r = httpx.post(f"{srv.base}/trace",
                       json={"addr": f"{IP}:{port}", "n": 2}, timeout=6)
        assert r.status_code == 200, r.text
        tid1 = r.json()["trace_id"]
        for i in range(5):
            c.sendall(f"{i}\n".encode())
        data = wait_items(httpx.Client(base_url=srv.base), srv.base, port, 2)
        assert data["count"] == 2 and len(data["items"]) == 2
        time.sleep(0.3)
        again = httpx.get(f"{srv.base}/trace",
                          params={"peer_ip": IP, "peer_port": port}, timeout=5).json()
        assert len(again["items"]) == 2  # 额外回执不保留

        # 重新开始一轮：换 id、清空旧记录、容量变为 4
        r = httpx.post(f"{srv.base}/trace",
                       json={"peer_ip": IP, "peer_port": port, "count": 4}, timeout=6)
        assert r.status_code == 200
        tid2 = r.json()["trace_id"]
        assert tid2 != tid1 and r.json()["items"] == [] and r.json()["count"] == 4
        for i in range(6):
            c.sendall(f"{100 + i}\n".encode())
        data = wait_items(httpx.Client(base_url=srv.base), srv.base, port, 4)
        assert len(data["items"]) == 4
        assert [i["corrected_value"] for i in data["items"]] == [100, 101, 102, 103]
        c.close()
    finally:
        srv.stop()


def test_handed_over_state() -> None:
    srv = Server()
    try:
        p = clamped_connect(srv.tcp_port, 4096, 1024)
        port = p.getsockname()[1]
        time.sleep(0.3)
        p.sendall(large_lines(10**30, 400))   # 未追踪的前置积压
        time.sleep(0.8)
        r = httpx.post(f"{srv.base}/trace",
                       json={"peer_ip": IP, "peer_port": port, "count": 16}, timeout=6)
        assert r.status_code == 200
        p.sendall(large_lines(9 * 10**30, 16))
        deadline = time.time() + 6
        dist: Counter = Counter()
        data = {}
        while time.time() < deadline:
            data = httpx.get(f"{srv.base}/trace",
                             params={"peer_ip": IP, "peer_port": port}, timeout=5).json()
            dist = Counter(i["state"] for i in data["items"])
            if len(data["items"]) == 16 and dist.get(HANDED_OVER, 0) >= 1:
                break
            time.sleep(0.1)
        assert len(data["items"]) == 16
        assert dist.get(ACKED, 0) == 0, dict(dist)   # 探头不读 + 小窗口 -> 不可能已确认
        assert dist.get(HANDED_OVER, 0) >= 1, dict(dist)
        stages = [i["stage"] for i in data["items"]]
        assert stages == sorted(stages)              # FIFO：进度单调
        labels = {i["progress_cn"] for i in data["items"] if i["state"] == HANDED_OVER}
        assert labels == {"已交出"}
        p.close()
    finally:
        srv.stop()


def test_pending_state_with_sndbuf_hook() -> None:
    srv = Server(extra_env={"WORKER_TEST_SNDBUF": "4096"})
    try:
        p = clamped_connect(srv.tcp_port, 2048, 512)
        port = p.getsockname()[1]
        time.sleep(0.3)
        p.sendall(large_lines(10**30, 800))
        time.sleep(1.0)
        r = httpx.post(f"{srv.base}/trace",
                       json={"peer_ip": IP, "peer_port": port, "count": 64}, timeout=6)
        assert r.status_code == 200
        p.sendall(large_lines(9 * 10**30, 64))
        data = wait_items(httpx.Client(base_url=srv.base), srv.base, port, 64)
        deadline = time.time() + 6
        while time.time() < deadline:
            data = httpx.get(f"{srv.base}/trace",
                             params={"peer_ip": IP, "peer_port": port}, timeout=5).json()
            if Counter(i["state"] for i in data["items"]).get(PENDING, 0) >= 48:
                break
            time.sleep(0.1)
        dist = Counter(i["state"] for i in data["items"])
        assert dist.get(ACKED, 0) == 0, dict(dist)
        assert dist.get(PENDING, 0) >= 48, dict(dist)
        assert any(i["progress_cn"] == "待发" for i in data["items"])
        p.close()
    finally:
        srv.stop()


def test_close_settlement_marks_unconfirmed() -> None:
    srv = Server()
    try:
        p = clamped_connect(srv.tcp_port, 4096, 1024)
        port = p.getsockname()[1]
        time.sleep(0.3)
        p.sendall(large_lines(10**30, 400))
        time.sleep(0.8)
        httpx.post(f"{srv.base}/trace",
                   json={"peer_ip": IP, "peer_port": port, "count": 16}, timeout=6)
        p.sendall(large_lines(9 * 10**30, 16))
        wait_items(httpx.Client(base_url=srv.base), srv.base, port, 16)
        time.sleep(0.5)

        p.close()  # 不读，直接关闭：当时应无已确认
        data = wait_settled(srv.base, port)
        assert data["settle_reason"] == "closed"
        states = [i["state"] for i in data["items"]]
        assert all(s in (ACKED, UNCONFIRMED) for s in states)
        assert states.count(UNCONFIRMED) >= 1
        # 结算后视图冻结：再次查询仍是 settled
        time.sleep(0.2)
        again = httpx.get(f"{srv.base}/trace",
                          params={"peer_ip": IP, "peer_port": port}, timeout=5).json()
        assert again["state"] == "settled"
    finally:
        srv.stop()


def test_mixed_settlement_unit() -> None:
    """确定性验证：结算时已确认保留，待发/已交出一律转未确认。"""
    rnd = TraceRound(trace_id="t", peer_ip=IP, peer_port=1, limit=3,
                     probe_sock=None)
    rnd.items = [
        {"index": 1, "value": 10, "revision": 2, "length": 4,
         "start_abs": 0, "end_abs": 4},
        {"index": 2, "value": 11, "revision": 2, "length": 4,
         "start_abs": 4, "end_abs": 8},
        {"index": 3, "value": 12, "revision": 2, "length": 4,
         "start_abs": 8, "end_abs": 12},
    ]
    rnd._live_states = lambda: [ACKED, HANDED_OVER, PENDING]  # type: ignore
    rnd.settle("handover")
    d = rnd.to_dict()
    assert [i["state"] for i in d["items"]] == [ACKED, UNCONFIRMED, UNCONFIRMED]
    assert [i["进度"] for i in d["items"]] == ["已确认", "未确认", "未确认"]
    assert d["settle_reason"] == "handover"


def test_ack_visible_during_handshake() -> None:
    gate = tempfile.mkdtemp(prefix="trace-barrier-")
    shutil.rmtree(gate, ignore_errors=True)
    srv = Server(extra_env={"RELOAD_BARRIER_DIR": gate})
    reload_resp: list[httpx.Response] = []

    def do_reload() -> None:
        reload_resp.append(
            httpx.post(f"{srv.base}/offset", json={"offset": 5}, timeout=30)
        )

    try:
        p = clamped_connect(srv.tcp_port, 4096, 1024)
        port = p.getsockname()[1]
        time.sleep(0.3)
        p.sendall(large_lines(10**30, 400))
        time.sleep(0.8)
        httpx.post(f"{srv.base}/trace",
                   json={"peer_ip": IP, "peer_port": port, "count": 16}, timeout=6)
        p.sendall(large_lines(9 * 10**30, 16))
        wait_items(httpx.Client(base_url=srv.base), srv.base, port, 16)
        time.sleep(0.6)

        th = threading.Thread(target=do_reload)
        th.start()

        reached = Path(gate) / "reached"
        dl = time.time() + 10
        while time.time() < dl and not reached.exists():
            time.sleep(0.02)
        assert reached.exists(), "reload did not reach handshake barrier"
        assert th.is_alive(), "reload should be blocked inside handshake window"

        # 握手空窗期查询必须立即返回，且轮次仍 active（尚未结算）
        t0 = time.time()
        d = httpx.get(f"{srv.base}/trace",
                      params={"peer_ip": IP, "peer_port": port}, timeout=5).json()
        query_ms = (time.time() - t0) * 1000
        assert query_ms < 2000, f"query blocked for {query_ms:.0f}ms"
        assert d["state"] == "active"
        assert th.is_alive()

        # 旧 worker 已冻结、新 worker 未就绪；此时探头读取，ACK 由内核处理，
        # 必须能在查询中看到“握手期间收到的确认”。
        p.settimeout(0.2)
        saw_ack_mid = False
        total = 0
        dl = time.time() + 8
        while time.time() < dl and not saw_ack_mid:
            try:
                chunk = p.recv(65536)
                if not chunk:
                    break
                total += len(chunk)
            except (BlockingIOError, socket.timeout, OSError):
                pass
            d = httpx.get(f"{srv.base}/trace",
                          params={"peer_ip": IP, "peer_port": port}, timeout=5).json()
            if any(i["state"] == ACKED for i in d["items"]):
                saw_ack_mid = True
        assert saw_ack_mid, "ACK arriving during handshake window not visible"
        assert th.is_alive(), "barrier must still hold reload while we observe"

        (Path(gate) / "release").write_text("go")
        th.join(timeout=20)
        assert reload_resp, "reload thread produced no response"
        r: httpx.Response = reload_resp[0]
        assert r.status_code == 200, r.text

        d = wait_settled(srv.base, port)
        assert d["settle_reason"] == "handover"
        # 至少握手期间看到的已确认结果被保留为 acked
        assert sum(1 for i in d["items"] if i["state"] == ACKED) >= 1
        assert all(i["state"] in (ACKED, UNCONFIRMED) for i in d["items"])
        p.close()
    finally:
        (Path(gate) / "release").write_text("go")  # 防止异常时 reload 干等
        shutil.rmtree(gate, ignore_errors=True)
        srv.stop()


def test_failed_reload_resume_continues_round() -> None:
    FAIL_MARKER.unlink(missing_ok=True)
    srv = Server()
    try:
        c = srv.connect()
        port = c.getsockname()[1]
        c.sendall(b"1\n")
        assert recv_line(c) == b"1,1\n"
        r = httpx.post(f"{srv.base}/trace",
                       json={"peer_ip": IP, "peer_port": port, "count": 5}, timeout=6)
        tid = r.json()["trace_id"]
        c.sendall(b"2\n")
        assert recv_line(c) == b"2,1\n"
        time.sleep(0.2)

        FAIL_MARKER.write_text("x")
        r = srv.reload(50)
        assert r.status_code == 503
        assert srv.status()["revision"] == 1

        d = httpx.get(f"{srv.base}/trace",
                      params={"peer_ip": IP, "peer_port": port}, timeout=5).json()
        assert d["state"] == "active" and d["trace_id"] == tid
        assert len(d["items"]) == 1

        FAIL_MARKER.unlink(missing_ok=True)
        # RESUME 后旧进程继续本轮，新回执仍进入同一轮
        c.sendall(b"3\n")
        assert recv_line(c) == b"3,1\n"
        time.sleep(0.2)
        d = httpx.get(f"{srv.base}/trace",
                      params={"peer_ip": IP, "peer_port": port}, timeout=5).json()
        assert d["trace_id"] == tid and len(d["items"]) == 2

        # 故障解除后换版成功 -> 结算
        r = srv.reload(50)
        assert r.status_code == 200
        d = wait_settled(srv.base, port)
        assert d["settle_reason"] == "handover"
        c.close()
    finally:
        FAIL_MARKER.unlink(missing_ok=True)
        srv.stop()


def test_arm_and_query_fast_under_backlog() -> None:
    srv = Server()
    try:
        c = srv.connect()
        port = c.getsockname()[1]
        c.setblocking(True)
        c.sendall(b"".join(f"{i}\n".encode() for i in range(20000)))
        time.sleep(1.0)  # 让服务端积压大量未发回执

        t0 = time.time()
        r = httpx.post(f"{srv.base}/trace",
                       json={"peer_ip": IP, "peer_port": port, "count": 64}, timeout=8)
        arm_ms = (time.time() - t0) * 1000
        assert r.status_code == 200
        assert arm_ms < 3000, f"arm scanned backlog? {arm_ms:.0f}ms"

        c.sendall(large_lines(7 * 10**30, 64))
        wait_items(httpx.Client(base_url=srv.base), srv.base, port, 64)
        t0 = time.time()
        for _ in range(20):
            d = httpx.get(f"{srv.base}/trace",
                          params={"peer_ip": IP, "peer_port": port}, timeout=5).json()
        avg_ms = (time.time() - t0) / 20 * 1000
        assert len(d["items"]) == 64
        assert avg_ms < 300, f"query too slow under backlog: {avg_ms:.1f}ms"
        c.close()
    finally:
        srv.stop()


def test_trace_does_not_delay_close() -> None:
    srv = Server()
    try:
        latencies = []
        for k in range(6):
            c = srv.connect()
            port = c.getsockname()[1]
            httpx.post(f"{srv.base}/trace",
                       json={"peer_ip": IP, "peer_port": port, "count": 4}, timeout=6)
            c.sendall(b"1\n2\n")
            recv_line(c)
            c.close()
            t0 = time.time()
            wait_settled(srv.base, port, timeout=3)
            latencies.append(time.time() - t0)
        worst = max(latencies)
        assert worst < 2.0, f"close->settle worst {worst*1000:.0f}ms"
        assert srv.status()["alive"]
    finally:
        srv.stop()


CASES = [
    ("connections listing + /docs", test_connections_listing_and_docs),
    ("basic values/revision/acked", test_basic_values_revision_and_ack),
    ("validation/cap/latest-round", test_validation_cap_and_latest_round),
    ("handed_over state (real TCP)", test_handed_over_state),
    ("pending state (real TCP + sndbuf hook)", test_pending_state_with_sndbuf_hook),
    ("close settles unconfirmed", test_close_settlement_marks_unconfirmed),
    ("mixed settlement unit", test_mixed_settlement_unit),
    ("ACK visible during handshake", test_ack_visible_during_handshake),
    ("failed reload resumes round", test_failed_reload_resume_continues_round),
    ("arm/query fast under backlog", test_arm_and_query_fast_under_backlog),
    ("trace does not delay close", test_trace_does_not_delay_close),
]


def main() -> int:
    FAIL_MARKER.unlink(missing_ok=True)
    for name, fn in CASES:
        check(name, fn)
    failed = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    if failed:
        for n, _ok, err in results:
            if not _ok:
                print(f"  - {n}: {err}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
