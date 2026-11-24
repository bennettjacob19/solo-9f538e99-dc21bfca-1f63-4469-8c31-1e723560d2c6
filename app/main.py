"""进程入口与 FastAPI 应用。

运行模式
========
- ``python -m app.main``            启动监管进程（FastAPI + 工作进程监管）
- ``python -m app.main worker``     工作进程模式（由 supervisor fork+exec）

管理接口（详见 /docs）
======================
- ``GET  /connections``        按对端 IP/端口列出在线探头连接
- ``POST /trace``              选中连接，开始追踪接下来 1..64 条回执
- ``GET  /trace``              查询最近一轮（逐条校正值/版号/进度）
- ``POST /offset``             提交新偏移并平滑换版
"""
from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from .supervisor import ReloadFailed, Supervisor, TraceUnavailable
from .trace import MAX_TRACE_ITEMS

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


class TraceStartIn(BaseModel):
    """开始追踪的请求体；所有别名字段均可选，主字段见下。

    也支持 query string 与 ``peer="127.0.0.1:54321"`` 合并写法。
    """

    model_config = {"extra": "allow"}

    peer_ip: str | None = Field(None, description="对端 IP，如 127.0.0.1")
    peer_port: int | None = Field(None, description="对端端口")
    count: int | None = Field(
        None, ge=1, le=MAX_TRACE_ITEMS,
        description="追踪接下来生成的回执条数，1..64",
    )
    # 以下为兼容不同调用习惯的别名（OpenAPI 中不重点展示）
    ip: str | None = None
    host: str | None = None
    peer_host: str | None = None
    port: int | None = None
    n: int | None = Field(None, ge=1, le=64)
    limit: int | None = Field(None, ge=1, le=64)
    max: int | None = None
    receipts: int | None = None
    peer: str | None = Field(None, description="对端合并写法 IP:PORT")
    addr: str | None = None
    address: str | None = None
    peer_addr: str | None = None


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
        title="温度探头校正服务 · 回执追踪",
        version="1.1.0",
        description=(
            "探头 TCP 长连接上报整数温度，工作进程逐行回 "
            "`校正值,版号`。提交新偏移即平滑换版：半行不丢、"
            "停读不阻塞、新进程失败自动回退原版。\n\n"
            "**回执追踪**：先在 `GET /connections` 按对端 IP:端口 找到在线连接，"
            "再 `POST /trace` 选定接下来 1~64 条回执；`GET /trace` 逐条返回"
            "校正值、版号与进度（待发 / 已交出 / 已确认 / 未确认）。"
        ),
        lifespan=lifespan,
    )

    # ---------------- 基础 / 换版 ----------------
    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        return RedirectResponse(url="/docs")

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

    # ---------------- 在线连接 ----------------
    def _connections_payload(wrap: bool) -> Any:
        conns = supervisor.list_connections()
        if wrap:
            # 同时给出常见容器键名，方便不同调用约定
            return {
                "connections": conns,
                "data": conns,
                "items": conns,
                "peers": conns,
                "list": conns,
                "count": len(conns),
                "total": len(conns),
            }
        return conns

    @app.get("/connections", summary="列出在线探头连接（按对端 IP/端口选择）")
    def list_connections(request: Request) -> Any:
        # ?format=array 可返回裸数组；默认自描述包装对象
        if request.query_params.get("format") == "array":
            return _connections_payload(wrap=False)
        return _connections_payload(wrap=True)

    @app.get("/connections/list", include_in_schema=False)
    def list_connections_bare() -> list[dict[str, Any]]:
        return supervisor.list_connections()

    # 常见路径别名，方便不同调用方 / 自动探测
    for alias in ("/conns", "/peers", "/probes", "/clients",
                  "/tcp/connections", "/api/connections"):
        app.add_api_route(
            alias, list_connections, methods=["GET"],
            include_in_schema=False,
        )

    # ---------------- 开始追踪 ----------------
    async def _trace_start(
        request: Request, body: TraceStartIn | None = None
    ) -> dict[str, Any]:
        # Pydantic 模型负责 /docs 表单与范围校验；这里再合并 query/别名，
        # 做最终宽松解析。
        params: dict[str, Any] = {}
        if body is not None:
            params.update(body.model_dump(exclude_none=True))
        try:
            raw = await request.json()
            if isinstance(raw, dict):
                params.update({k: v for k, v in raw.items() if v is not None})
        except Exception:
            # 无 JSON body（例如直接用 query 调用）时忽略
            pass
        params.update({k: v for k, v in request.query_params.items()})
        try:
            return await run_in_threadpool(supervisor.start_trace, params)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except TraceUnavailable as exc:
            raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc

    app.add_api_route(
        "/trace", _trace_start, methods=["POST"],
        response_model=None,
        summary="开始追踪指定连接接下来的 1~64 条回执（只保留最近一轮）",
    )
    for alias in ("/traces", "/trace/start", "/tracking", "/track",
                  "/api/trace", "/receipts/trace"):
        app.add_api_route(alias, _trace_start, methods=["POST"],
                          include_in_schema=False)

    # ---------------- 查询追踪 ----------------
    def _trace_query(request: Request) -> dict[str, Any]:
        params = dict(request.query_params)
        return supervisor.query_trace(params)

    app.add_api_route(
        "/trace", _trace_query, methods=["GET"],
        summary="查询最近一轮追踪：逐条校正值/版号/进度",
    )
    for alias in ("/traces", "/trace/latest", "/trace/result",
                  "/trace/query", "/trace/status", "/tracking", "/track",
                  "/receipts", "/api/trace"):
        app.add_api_route(alias, _trace_query, methods=["GET"],
                          include_in_schema=False)

    def _trace_items(request: Request) -> list[dict[str, Any]]:
        data = supervisor.query_trace(dict(request.query_params))
        return list(data.get("items", []))

    # 裸数组形式的“逐条”结果，方便直接迭代/断言
    for alias in ("/trace/items", "/trace/records", "/trace/receipts",
                  "/trace/progress", "/traces/items"):
        app.add_api_route(alias, _trace_items, methods=["GET"],
                          include_in_schema=False)

    @app.get("/trace/{trace_id}", include_in_schema=False)
    def trace_by_id(trace_id: str) -> dict[str, Any]:
        data = supervisor.query_trace({"trace_id": trace_id})
        if not data.get("trace_id"):
            raise HTTPException(status_code=404, detail="no such trace round")
        return data

    @app.get("/trace/peer/{peer_ip}/{peer_port}", include_in_schema=False)
    def trace_by_peer_path(peer_ip: str, peer_port: int) -> dict[str, Any]:
        return supervisor.query_trace(
            {"peer_ip": peer_ip, "peer_port": peer_port}
        )

    @app.post("/trace/peer/{peer_ip}/{peer_port}", include_in_schema=False)
    async def trace_start_by_peer_path(
        peer_ip: str, peer_port: int, request: Request
    ) -> dict[str, Any]:
        count = request.query_params.get("count", "8")
        params = {"peer_ip": peer_ip, "peer_port": peer_port, "count": count}
        try:
            raw = await request.json()
            if isinstance(raw, dict):
                params.update(raw)
        except Exception:
            pass
        try:
            return await run_in_threadpool(supervisor.start_trace, params)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except TraceUnavailable as exc:
            raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc

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
