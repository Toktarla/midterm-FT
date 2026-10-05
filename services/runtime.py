"""Routes shared by the three backend services."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from ftuis import chaos
from ftuis.config import Settings
from ftuis.events import emit
from ftuis.storage import DatabaseUnavailable, Store


def install(app: FastAPI, settings: Settings, store: Store, reset_fn: Callable[[], None]) -> None:
    @app.exception_handler(DatabaseUnavailable)
    async def database_down(_request, exc: DatabaseUnavailable) -> JSONResponse:
        emit("database_unavailable", detail=str(exc))
        return JSONResponse({"error": "database unavailable"}, status_code=503)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {
            "status": "ok",
            "instance": settings.instance_id,
            "node": settings.node_id,
            "service": settings.service,
            "mode": settings.mode,
        }

    @app.get("/ping")
    async def ping() -> dict[str, str | bool]:
        return {"ok": True, "instance": settings.instance_id, "node": settings.node_id}

    @app.get("/admin/chaos")
    def chaos_view() -> dict[str, object]:
        # Same process and same read path the request handler uses.
        return chaos.load()

    @app.post("/admin/release-db")
    def release_db() -> dict[str, bool]:
        store.close()
        return {"ok": True}

    @app.post("/admin/reset")
    def reset() -> dict[str, bool]:
        reset_fn()
        return {"ok": True}

    @app.middleware("http")
    async def chaos_middleware(request, call_next):
        path = request.url.path
        if path.startswith("/admin") or path == "/health":
            return await call_next(request)
        delay = chaos.delay_s(settings.instance_id)
        if delay:
            await asyncio.sleep(delay)
        if chaos.instance_failed(settings.instance_id):
            return JSONResponse(
                {"error": "injected failure", "instance": settings.instance_id},
                status_code=500,
            )
        return await call_next(request)
