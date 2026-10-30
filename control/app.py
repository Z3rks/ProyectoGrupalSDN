"""Construcción de las aplicaciones FastAPI del servicio de control."""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from common.errors import DFShaError, install_error_handlers, operation_id

from . import auth, routes_internal, routes_public
from .etcd import EtcdUnavailable
from .leader import LeaderService
from .settings import Settings
from .state import State, build_state

log = logging.getLogger("dfsha.control")


async def _bootstrap(st: State) -> None:
    """Crea el admin inicial. Reintenta hasta que etcd tenga quórum (el control puede arrancar antes)."""
    delay = 1.0
    while True:
        try:
            await auth.bootstrap_admin(st)
            log.info("cuenta de administrador lista")
            return
        except EtcdUnavailable:
            log.warning("etcd no disponible todavía; reintento en %.0fs", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10)


def create_apps(cfg: Settings) -> tuple[FastAPI, FastAPI, State]:
    """Devuelve (app_pública, app_interna, estado). Ambas apps comparten el mismo State."""
    st = build_state(cfg)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        boot = asyncio.create_task(_bootstrap(st))
        st.leader = LeaderService(st)
        st.leader.start()
        yield
        boot.cancel()
        await st.leader.stop()
        await st.etcd_http.aclose()
        await st.node_http.aclose()

    public = FastAPI(title="DFSha control (público)", lifespan=lifespan)
    internal = FastAPI(title="DFSha control (interno)")
    for app in (public, internal):
        app.state.st = st
        install_error_handlers(app)

        @app.exception_handler(EtcdUnavailable)
        async def _no_quorum(request, exc):   # sin mayoría de etcd: decisión CP, se rechaza
            err = DFShaError(503, "CONTROL_NO_QUORUM", "el clúster de metadatos no tiene quórum", retryable=True)
            return JSONResponse(err.body(operation_id(request)), status_code=503)

    public.include_router(routes_public.router)
    internal.include_router(routes_internal.router)

    @public.get("/healthz")
    async def healthz():
        return {"ok": True, "instance": cfg.instance_id}

    return public, internal, st
