"""进程入口与 FastAPI 应用。

运行模式
========
- ``python -m app.main``            启动监管进程（FastAPI + 工作进程监管）
- ``python -m app.main worker``     工作进程模式（由 supervisor fork+exec）
"""
from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .supervisor import ReloadFailed, Supervisor

TCP_HOST = os.environ.get("TCP_HOST", "0.0.0.0")
TCP_PORT = int(os.environ.get("TCP_PORT", "9000"))
INITIAL_OFFSET = int(os.environ.get("INITIAL_OFFSET", "0"))


class OffsetIn(BaseModel):
    offset: int = Field(..., description="新的校正偏移（整数），提交即换版")


class ReloadOut(BaseModel):
    old_pid: int
    pid: int
    offset: int
    revision: int
    inherited_conns: int


def create_app() -> FastAPI:
    supervisor = Supervisor(TCP_HOST, TCP_PORT, initial_offset=INITIAL_OFFSET)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        supervisor.start()
        try:
            yield
        finally:
            supervisor.shutdown()

    app = FastAPI(
        title="温度探头校正服务",
        version="1.0.0",
        description=(
            "探头 TCP 长连接上报整数温度，工作进程逐行回 "
            "`校正值,版号`。提交新偏移即平滑换版：半行不丢、"
            "停读不阻塞、新进程失败自动回退原版。"
        ),
        lifespan=lifespan,
    )

    @app.post("/offset", response_model=ReloadOut, summary="提交新偏移并换版")
    def submit_offset(body: OffsetIn) -> ReloadOut:
        try:
            result = supervisor.reload(body.offset)
        except ReloadFailed as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return ReloadOut(**result)

    @app.get("/status", summary="查询当前工作进程 PID 与版号")
    def status() -> dict:
        return supervisor.status()

    @app.get("/health", summary="健康检查")
    def health() -> dict:
        st = supervisor.status()
        return {"ok": st["alive"], **st}

    return app


app = create_app()


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "worker":
        from .worker import worker_entry

        worker_entry()
        return

    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=os.environ.get("HTTP_HOST", "0.0.0.0"),
        port=int(os.environ.get("HTTP_PORT", "8000")),
        log_level=os.environ.get("LOG_LEVEL", "info"),
    )


if __name__ == "__main__":
    main()
