from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import uvicorn
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .collector import PollingService
from .models import HostCreate, HostPublic, HostUpdate, ReorderRequest, Settings, SettingsUpdate
from .storage import JsonStore

logger = logging.getLogger(__name__)
PACKAGE_DIR = Path(__file__).resolve().parent


def create_app(data_dir: Path | None = None) -> FastAPI:
    resolved_data_dir = data_dir or Path(os.getenv("HOSTWATCH_DATA_DIR", "data")).resolve()
    store = JsonStore(resolved_data_dir / "config.json")
    polling = PollingService(store)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await polling.start()
        try:
            yield
        finally:
            await polling.stop()
            store.close()

    app = FastAPI(title="HostWatch", version="0.1.0", lifespan=lifespan)
    app.state.store = store
    app.state.polling = polling
    app.mount("/static", StaticFiles(directory=PACKAGE_DIR / "static"), name="static")

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(PACKAGE_DIR / "static" / "index.html")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/hosts", response_model=list[HostPublic])
    async def list_hosts() -> list[dict]:
        return store.list_hosts()

    @app.post("/api/hosts", response_model=HostPublic, status_code=status.HTTP_201_CREATED)
    async def create_host(data: HostCreate) -> dict:
        host = store.create_host(data)
        asyncio.create_task(
            polling.collect_host(host["id"], force=True),
            name=f"collect-host-{host['id']}",
        )
        return host

    @app.patch("/api/hosts/{host_id}", response_model=HostPublic)
    async def update_host(host_id: int, data: HostUpdate) -> dict:
        try:
            host = store.update_host(host_id, data)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if host is None:
            raise HTTPException(status_code=404, detail="主机不存在")
        polling.host_updated(host_id)
        asyncio.create_task(
            polling.collect_host(host_id, force=True),
            name=f"collect-updated-host-{host_id}",
        )
        return host

    @app.delete("/api/hosts/{host_id}", status_code=status.HTTP_204_NO_CONTENT)
    async def delete_host(host_id: int) -> None:
        if not store.delete_host(host_id):
            raise HTTPException(status_code=404, detail="主机不存在")
        polling.remove_host(host_id)

    @app.post("/api/hosts/reorder", status_code=status.HTTP_204_NO_CONTENT)
    async def reorder_hosts(data: ReorderRequest) -> None:
        if not store.reorder_hosts(data.host_ids):
            raise HTTPException(status_code=400, detail="排序列表必须包含全部且不重复的主机 ID")

    @app.post("/api/hosts/{host_id}/refresh")
    async def refresh_host(host_id: int) -> dict:
        if store.get_host(host_id) is None:
            raise HTTPException(status_code=404, detail="主机不存在")
        return await polling.collect_host(host_id, force=True)

    @app.get("/api/hosts/{host_id}/metrics")
    async def host_metrics(
        host_id: int,
        since: float | None = Query(default=None, ge=0),
        max_points: int = Query(default=480, ge=2, le=1000),
    ) -> list[dict]:
        if store.get_host(host_id) is None:
            raise HTTPException(status_code=404, detail="主机不存在")
        return polling.get_metrics(host_id, since=since, max_points=max_points)

    @app.get("/api/metrics")
    async def all_metrics(
        since: float | None = Query(default=None, ge=0),
        max_points: int = Query(default=480, ge=2, le=1000),
    ) -> dict:
        settings = store.get_settings()
        host_ids = [host["id"] for host in store.list_hosts()]
        metrics = polling.get_all_metrics(
            host_ids, settings.history_minutes, since, max_points
        )
        return {"metrics": metrics, "server_time": time.time()}

    @app.get("/api/settings", response_model=Settings)
    async def get_settings() -> Settings:
        return store.get_settings()

    @app.put("/api/settings", response_model=Settings)
    async def update_settings(data: SettingsUpdate) -> Settings:
        result = store.update_settings(data)
        polling.update_retention(result.history_minutes)
        polling.wake()
        return result

    @app.get("/api/dashboard")
    async def dashboard() -> dict:
        settings = store.get_settings()
        hosts = store.list_hosts()
        items = []
        for host in hosts:
            items.append({
                **host,
                "status": polling.get_status(host["id"]),
            })
        return {"settings": settings.model_dump(), "hosts": items, "server_time": time.time()}

    return app


app = create_app()


def run() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    uvicorn.run(
        app,
        host=os.getenv("HOSTWATCH_HOST", "0.0.0.0"),
        port=int(os.getenv("HOSTWATCH_PORT", "8000")),
        reload=False,
    )
