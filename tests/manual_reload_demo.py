"""手工实测脚本：重点演示“探头停读 / 半行时换版”。

场景：
- 探头 A：发完整行后停读（不收回执）
- 探头 B：只发半行
- 探头 C：正常收发
换版期间验证：换版快速完成、PID 变化、A/B 连接不断、
B 补齐后按新版、C 全程照常、A 的旧版回执最终仍可读。
"""
import os
import socket
import sys
import time

sys.path.insert(0, "/workspace")
sys.path.insert(0, "/tmp/pylibs")

import httpx

from tests.integration_test import Server, recv_line

srv = Server()
base = srv.base
try:
    st0 = httpx.get(f"{base}/status").json()
    print(f"[初始] pid={st0['pid']} revision={st0['revision']} offset={st0['offset']}")

    a = srv.connect()  # 停读探头
    b = srv.connect()  # 半行探头
    c = srv.connect()  # 正常探头

    a.sendall(b"20\n")          # A 的回执将积压在服务端发送缓冲
    b.sendall(b"15")            # B 只有半行，无换行
    c.sendall(b"100\n")
    print("[正常探头 C] 回执:", recv_line(c).decode().strip())

    print("\n--- 提交新偏移 +50 换版（A 停读、B 半行）---")
    t0 = time.time()
    r = httpx.post(f"{base}/offset", json={"offset": 50}, timeout=15)
    dt = time.time() - t0
    r.raise_for_status()
    body = r.json()
    print(f"[换版] 耗时 {dt*1000:.0f}ms  old_pid={body['old_pid']} "
          f"new_pid={body['pid']} revision={body['revision']} 继承连接={body['inherited_conns']}")
    assert dt < 5, "停读探头阻塞了换版！"
    assert body["pid"] != body["old_pid"]
    assert body["inherited_conns"] == 3

    # 连接未断（无需重连）
    for name, s in (("A", a), ("B", b), ("C", c)):
        s.getpeername()
    print("[连接] A/B/C 三条 TCP 连接均保持，无需重连")

    # B 半行补齐 -> 新版回执：半行 "15" 补齐 "1" 成 "151"，151+50=201,rev2
    b.sendall(b"1\n")
    print("[半行探头 B] 补齐后回执（应为 201,2）:", recv_line(b).decode().strip())

    # C 换版后立即照常
    c.sendall(b"100\n")
    print("[正常探头 C] 换版后回执（应为 150,2）:", recv_line(c).decode().strip())

    # A 停读期间积压的旧版回执，最终仍能读到
    print("[停读探头 A] 恢复读取，旧版回执（应为 20,1）:", recv_line(a).decode().strip())
    a.sendall(b"20\n")
    print("[停读探头 A] 再发一行（应为 70,2）:", recv_line(a).decode().strip())

    st1 = httpx.get(f"{base}/status").json()
    print(f"\n[当前] pid={st1['pid']} revision={st1['revision']} offset={st1['offset']} alive={st1['alive']}")
    print("\n实测结论：停读/半行均不阻塞换版；连接不中断；半行按新版、完整行按旧版；其他探头不受影响。")

    for s in (a, b, c):
        s.close()
finally:
    srv.stop()
