"""Unix 域 socket 上传递文件描述符（SCM_RIGHTS）与定长记录的辅助函数。

控制通道使用 SOCK_SEQPACKET：每条记录自带边界，辅助数据（fd）与数据一定
随同一次 recvmsg 到达，避免 STREAM 上拆包导致的错位。
"""
from __future__ import annotations

import json
import socket
import struct
from typing import Any

# Linux: SOL_SOCKET / SCM_RIGHTS
_RECV_FD_BUF = socket.CMSG_SPACE(struct.calcsize("i"))


def send_fd(sock: socket.socket, payload: dict[str, Any], fd: int | None) -> None:
    """发送一条记录，可附带一个 fd。

    SEQPACKET 以一次 sendmsg 为一个数据报边界；消息远小于 SO_SNDBUF，
    内核要么整包接收，要么 EAGAIN，因此不能用 sendall（部分发送后的
    重试会变成第二个数据报，造成接收端错位）。
    """
    data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    while True:
        try:
            if fd is None:
                sent = sock.send(data)
            else:
                sent = sock.sendmsg(
                    [data],
                    [(socket.SOL_SOCKET, socket.SCM_RIGHTS, struct.pack("i", fd))],
                )
            if sent != len(data):  # 理论上不会发生（seqpacket 原子发送）
                raise OSError("seqpacket partial send, message boundary broken")
            return
        except InterruptedError:
            continue


def recv_fd(sock: socket.socket) -> tuple[dict[str, Any], int | None]:
    """接收一条记录，返回 (payload, fd 或 None)。对端关闭时抛 EOFError。"""
    msg, ancdata, _flags, _addr = sock.recvmsg(1 << 20, _RECV_FD_BUF)
    if not msg and not ancdata:
        raise EOFError("control socket closed")
    fd: int | None = None
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
            (fd,) = struct.unpack("i", cmsg_data[: struct.calcsize("i")])
            break
    return json.loads(msg.decode("utf-8")), fd


def control_pair() -> tuple[socket.socket, socket.socket]:
    """创建一对已连接的 SEQPACKET unix socket。"""
    a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    return a, b
