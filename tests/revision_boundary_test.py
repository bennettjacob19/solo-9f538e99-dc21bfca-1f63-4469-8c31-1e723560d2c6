"""验证版号边界：慢喂数据，换版前后分别发完整行与半行。"""
import sys

sys.path.insert(0, "/workspace")
sys.path.insert(0, "/tmp/pylibs")

from tests.integration_test import Server, recv_line

srv = Server()
try:
    c = srv.connect()

    # 换版前：一条完整行（旧版），一条半行（跨版本）
    c.sendall(b"10\n")
    assert recv_line(c) == b"10,1\n"
    c.sendall(b"20")  # 半行，无换行

    r = srv.reload(5)
    assert r.status_code == 200

    # 半行补齐 -> 新版：205,2
    c.sendall(b"5\n")
    assert recv_line(c) == b"210,2\n"

    # 换版后的完整行全部新版
    for v in (1, 2, 3):
        c.sendall(f"{v}\n".encode())
        assert recv_line(c) == f"{v + 5},2\n".encode()

    print("版号边界验证通过：旧完整行->rev1，跨版半行与新行->rev2，校正值正确")
    c.close()
finally:
    srv.stop()
