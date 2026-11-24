"""端到端集成测试：真实子进程 + 真实 TCP/HTTP，无 mock。

覆盖：
1. 逐行回执、顺序、只回一次、半行补齐
2. 换版：完整行旧版号、半行新版号、PID 变化、连接不中断、无需重连
3. 探头停读 / 只发半行时换版仍完成；其他探头照常收回执
4. 新工作进程启动失败 -> 503 回退，旧 PID/旧偏移继续服务
5. 单连接收/发缓冲 128B 上限：满了只暂停该连接，不影响其他连接
6. 超长行/非整数行丢弃；/docs 可用
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
FAIL_MARKER = Path("/tmp/worker-fail-marker")


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Server:
    def __init__(self, extra_env: dict[str, str] | None = None) -> None:
        self.http_port = free_port()
        self.tcp_port = free_port()
        env = dict(os.environ)
        env["HTTP_PORT"] = str(self.http_port)
        env["TCP_PORT"] = str(self.tcp_port)
        env["WORKER_FAIL_MARKER"] = str(FAIL_MARKER)
        env["PYTHONUNBUFFERED"] = "1"
        if extra_env:
            env.update(extra_env)
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "app.main"],
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.base = f"http://127.0.0.1:{self.http_port}"
        self._wait_ready()

    def _wait_ready(self) -> None:
        deadline = time.time() + 20
        while time.time() < deadline:
            if self.proc.poll() is not None:
                out = self.proc.stdout.read() if self.proc.stdout else ""
                raise RuntimeError(f"server exited early:\n{out}")
            try:
                r = httpx.get(f"{self.base}/health", timeout=1)
                if r.status_code == 200 and r.json()["ok"]:
                    return
            except httpx.HTTPError:
                time.sleep(0.1)
        raise RuntimeError("server did not become ready")

    def status(self) -> dict:
        return httpx.get(f"{self.base}/status", timeout=5).json()

    def reload(self, offset: int) -> httpx.Response:
        return httpx.post(
            f"{self.base}/offset", json={"offset": offset}, timeout=15
        )

    def connect(self) -> socket.socket:
        s = socket.create_connection(("127.0.0.1", self.tcp_port), timeout=5)
        s.settimeout(5)
        return s

    def stop(self) -> None:
        self.proc.terminate()
        try:
            out, _ = self.proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            out, _ = self.proc.communicate(timeout=5)
        if self.proc.returncode not in (0, -15):
            sys.stdout.write(out or "")

    @property
    def logs(self) -> str:
        return self.proc.stdout.read() if self.proc.stdout else ""


def recv_line(sock: socket.socket, timeout: float = 5) -> bytes:
    sock.settimeout(timeout)
    buf = bytearray()
    while not buf.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            raise AssertionError(f"connection closed before newline, got {buf!r}")
        buf.extend(chunk)
    return bytes(buf)


results: list[tuple[str, bool, str]] = []


def check(name: str, fn) -> None:
    try:
        fn()
        results.append((name, True, ""))
        print(f"PASS  {name}")
    except Exception as exc:  # noqa: BLE001
        results.append((name, False, repr(exc)))
        print(f"FAIL  {name}: {exc!r}")


# ---------------- 测试用例 ----------------
def t_basic_echo_and_order() -> None:
    srv = Server()
    try:
        c = srv.connect()
        c.sendall(b"25\n")
        assert recv_line(c) == b"25,1\n"
        c.sendall(b"-3\n100\n")
        assert recv_line(c) == b"-3,1\n"
        assert recv_line(c) == b"100,1\n"
        # 半行分两次到达
        c.sendall(b"4")
        time.sleep(0.2)
        c.sendall(b"2\n")
        assert recv_line(c) == b"42,1\n"
        c.close()
    finally:
        srv.stop()


def t_reload_revision_semantics() -> None:
    srv = Server()
    try:
        pid0 = srv.status()["pid"]
        a = srv.connect()
        b = srv.connect()

        # 换版前完整行：先收回执（旧版）
        a.sendall(b"25\n")
        assert recv_line(a) == b"25,1\n"

        # b 只发半行就换版
        b.sendall(b"7")
        time.sleep(0.3)

        # a 再发一个完整行但不读，换版期间应按旧版入队
        a.sendall(b"30\n")

        r = srv.reload(10)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["revision"] == 2 and body["offset"] == 10
        assert body["pid"] != pid0 and body["old_pid"] == pid0
        assert srv.status()["pid"] == body["pid"]

        # 旧版完整行的回执（交接前已切行）
        assert recv_line(a) == b"30,1\n"
        # 半行补齐 -> 新版
        b.sendall(b"3\n")
        assert recv_line(b) == b"83,2\n"
        # 同一连接继续，无需重连
        a.sendall(b"25\n")
        assert recv_line(a) == b"35,2\n"
        a.close()
        b.close()
    finally:
        srv.stop()


def t_silent_and_halfline_probe_during_reload() -> None:
    srv = Server()
    try:
        silent = srv.connect()   # 停读探头
        half = srv.connect()     # 只发半行的探头
        busy = srv.connect()     # 正常探头

        silent.sendall(b"9\n")           # 回执留在它的发送缓冲里，不读
        half.sendall(b"1")               # 只有半行
        busy.sendall(b"50\n")
        assert recv_line(busy) == b"50,1\n"

        t0 = time.time()
        r = srv.reload(7)
        elapsed = time.time() - t0
        assert r.status_code == 200, r.text
        assert elapsed < 8, f"reload blocked by slow probes for {elapsed:.1f}s"

        # 停读探头连接仍在（同一 TCP 连接，未被踢）
        assert silent.getpeername()
        # 半行补齐按新版：12 + 7 = 19
        half.sendall(b"2\n")
        assert recv_line(half) == b"19,2\n"
        # 其他探头照常
        busy.sendall(b"50\n")
        assert recv_line(busy) == b"57,2\n"

        # 停读探头最终也能读到它那条旧版回执
        assert recv_line(silent) == b"9,1\n"
        silent.close()
        half.close()
        busy.close()
    finally:
        srv.stop()


def t_new_worker_failure_fallback() -> None:
    srv = Server()
    try:
        FAIL_MARKER.write_text("x")
        try:
            c = srv.connect()
            c.sendall(b"1\n")
            assert recv_line(c) == b"1,1\n"

            r = srv.reload(99)
            assert r.status_code == 503, r.text
            st = srv.status()
            assert st["revision"] == 1 and st["offset"] == 0 and st["alive"]

            # 旧进程继续按原版接单
            c.sendall(b"1\n")
            assert recv_line(c) == b"1,1\n"
            c.close()
        finally:
            FAIL_MARKER.unlink(missing_ok=True)

        # 故障解除后换版成功
        r = srv.reload(99)
        assert r.status_code == 200, r.text
        c = srv.connect()
        c.sendall(b"1\n")
        assert recv_line(c) == b"100,2\n"
        c.close()
    finally:
        srv.stop()


def t_buffer_cap_isolates_connection() -> None:
    srv = Server()
    try:
        slow = srv.connect()   # 从不读 -> 发送缓冲最终打满
        fast = srv.connect()   # 正常读

        # 灌入足够多的行（每行 ~6B，128B 发送缓冲 + 128B 接收缓冲后暂停读入）
        for i in range(60):
            slow.sendall(f"{i}\n".encode())
        time.sleep(0.5)

        # 慢连接的压力不能影响快连接
        for i in range(5):
            fast.sendall(b"10\n")
            assert recv_line(fast) == b"10,1\n"

        # 换版也必须完成（慢连接冻结/交接不被缓冲阻塞）
        r = srv.reload(3)
        assert r.status_code == 200, r.text

        # 慢连接排空后仍能收到部分回执且连接存活
        slow.settimeout(3)
        first = recv_line(slow)
        assert first.endswith(b",1\n") or first.endswith(b",2\n"), first
        assert slow.getpeername()
        slow.close()

        fast.sendall(b"10\n")
        assert recv_line(fast) == b"13,2\n"
        fast.close()
    finally:
        srv.stop()


def t_bad_lines_and_docs() -> None:
    srv = Server()
    try:
        c = srv.connect()
        c.sendall(b"x" * 40 + b"\n")     # 超长行（>32B），丢弃
        c.sendall(b"abc\n")              # 非整数，丢弃
        c.sendall(b"1_0\n")              # 非严格整数，丢弃
        c.sendall(b"7\n")                # 正常行仍可回执
        assert recv_line(c) == b"7,1\n"

        # 无换行的超长半行（>128B）：不能把连接永久读暂停
        c.sendall(b"9" * 200)            # 半行且超过接收缓冲
        time.sleep(0.4)
        c.sendall(b"\n8\n")              # 前面的半行应被丢弃，8 正常回执
        assert recv_line(c) == b"8,1\n"
        c.close()

        docs = httpx.get(f"{srv.base}/docs", timeout=5)
        assert docs.status_code == 200 and b"swagger" in docs.content.lower()
        spec = httpx.get(f"{srv.base}/openapi.json", timeout=5).json()
        assert "/offset" in spec["paths"] and "/status" in spec["paths"]
    finally:
        srv.stop()


def t_multiple_reloads_and_pids() -> None:
    srv = Server()
    try:
        c = srv.connect()
        pids = [srv.status()["pid"]]
        for offset, rev in ((1, 2), (2, 3), (-5, 4)):
            r = srv.reload(offset)
            assert r.status_code == 200, r.text
            st = srv.status()
            assert st["revision"] == rev and st["offset"] == offset
            assert st["pid"] not in pids  # 每次都是全新工作进程
            pids.append(st["pid"])
            c.sendall(b"10\n")
            assert recv_line(c) == f"{10 + offset},{rev}\n".encode()
        c.close()
    finally:
        srv.stop()


def main() -> int:
    FAIL_MARKER.unlink(missing_ok=True)
    cases = [
        ("basic echo / ordering / half-line", t_basic_echo_and_order),
        ("reload revision semantics", t_reload_revision_semantics),
        ("silent & half-line probe during reload", t_silent_and_halfline_probe_during_reload),
        ("new worker failure -> fallback", t_new_worker_failure_fallback),
        ("128B buffer cap isolates connection", t_buffer_cap_isolates_connection),
        ("bad lines dropped & /docs available", t_bad_lines_and_docs),
        ("multiple reloads rotate pids", t_multiple_reloads_and_pids),
    ]
    for name, fn in cases:
        check(name, fn)
    failed = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
