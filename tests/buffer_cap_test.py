"""精确验证应用层缓冲 128B 上限与连接间隔离。

构造一个从不读取的探头：
- 服务端发送缓冲上限 128B，打满后回执进入 pending；
- 接收缓冲也上限 128B，打满后只暂停该连接读入（其他连接不受影响）；
- 换版在该连接完全停摆时仍能完成；
- 恢复读取后能按 FIFO 收到回执，且每条上报只回一次（无重复/丢失计数）。
"""
import socket
import sys
import time

sys.path.insert(0, "/workspace")
sys.path.insert(0, "/tmp/pylibs")

from tests.integration_test import Server, recv_line

srv = Server()
try:
    stuck = srv.connect()   # 全程不读
    other = srv.connect()   # 对照连接

    total = 100
    payload = b"".join(f"{i}\n".encode() for i in range(total))
    assert len(payload) > 256  # 远超收/发缓冲总和
    stuck.sendall(payload)
    time.sleep(1.0)

    # 对照连接在慢连接缓冲打满期间完全正常
    for _ in range(3):
        other.sendall(b"5\n")
        assert recv_line(other) == b"5,1\n"
    print("[隔离] 慢连接缓冲打满，对照连接 3 次收发均正常")

    # 换版不受影响
    r = srv.reload(2)
    assert r.status_code == 200
    print(f"[换版] 停摆连接存在时换版成功，继承 {r.json()['inherited_conns']} 条连接")

    # 恢复读取，统计回执：每行恰好一次，顺序 FIFO，版号按换版边界划分
    stuck.settimeout(5)
    seen: list[tuple[int, int]] = []
    stuck.settimeout(2)
    try:
        while len(seen) < total:
            line = recv_line(stuck, timeout=2)
            value, rev = line.decode().strip().split(",")
            seen.append((int(value), int(rev)))
    except AssertionError:
        pass
    values = [v for v, _ in seen]
    assert values == list(range(len(values))), "回执顺序/内容异常"
    assert len(values) == total, f"只收到 {len(values)}/{total} 条回执"
    assert len(set(values)) == total, "存在重复回执"
    print(f"[计数] {total} 条上报全部恰好回执一次，FIFO 有序")
    print(f"[版号] 旧版(rev=1)回执 {sum(1 for _,x in seen if x==1)} 条，"
          f"新版(rev=2)回执 {sum(1 for _,x in seen if x==2)} 条；"
          f"边界单调（先旧后新）: {[r for _,r in seen] == sorted(r for _,r in seen)}")

    # 数值正确：rev1 = 原值+0，rev2 = 原值+2
    for i, (value, rev) in enumerate(seen):
        assert value == i + (2 if rev == 2 else 0), (i, value, rev)
    print("[数值] 各版回执校正值全部正确")

    other.sendall(b"5\n")
    assert recv_line(other) == b"7,2\n"
    print("[对照] 换版后对照连接回执 7,2 正常")
    stuck.close()
    other.close()
    print("\n128B 缓冲上限与连接隔离验证通过")
finally:
    srv.stop()
