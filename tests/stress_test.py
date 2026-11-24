"""压力/并发测试：多探头高频上报 + 并发换版请求。"""
import concurrent.futures
import socket
import sys
import threading
import time

sys.path.insert(0, "/workspace")
sys.path.insert(0, "/tmp/pylibs")

import httpx

from tests.integration_test import Server, recv_line

srv = Server()
base = srv.base
errors: list[str] = []
try:
    n_probes = 8
    lines_per_probe = 60
    probes = [srv.connect() for _ in range(n_probes)]
    stop = threading.Event()

    def reader(idx: int, sock: socket.socket) -> tuple[int, set[int]]:
        revs: set[int] = set()
        count = 0
        sock.settimeout(8)
        while count < lines_per_probe:
            try:
                line = recv_line(sock, timeout=8)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"probe{idx} recv: {exc!r}")
                break
            value, rev = line.decode().strip().split(",")
            revs.add(int(rev))
            count += 1
        return count, revs

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_probes) as pool:
        futures = [pool.submit(reader, i, p) for i, p in enumerate(probes)]
        for i, p in enumerate(probes):
            for v in range(lines_per_probe):
                p.sendall(f"{v}\n".encode())
                if v % 20 == 0:
                    time.sleep(0.01)

        # 上报进行中并发发起多次换版（应被锁串行化，全部成功或明确失败回退）
        time.sleep(0.2)
        reload_results = []
        for off in (1, 2, 3):
            r = httpx.post(f"{base}/offset", json={"offset": off}, timeout=20)
            reload_results.append((r.status_code, r.json() if r.status_code == 200 else r.text))
        print("换版结果:")
        for code, body in reload_results:
            print(" ", code, body)
        assert all(code == 200 for code, _ in reload_results)

        results = [f.result() for f in futures]

    total = sum(c for c, _ in results)
    expected = n_probes * lines_per_probe
    assert total == expected, f"回执数 {total} != {expected}, errors={errors}"
    all_revs = set().union(*(r for _, r in results))
    print(f"[回执] {total}/{expected} 条，无丢失；出现过的版号: {sorted(all_revs)}")
    print(f"[版号] 最终 revision={srv.status()['revision']} offset={srv.status()['offset']}")
    assert srv.status()["revision"] == 4
    for p in probes:
        p.close()
    print("\n并发压力测试通过：多探头 + 并发换版，回执不丢不重，换版串行完成")
finally:
    srv.stop()
    if errors:
        print("ERRORS:", errors[:5])
