"""Aplicaciones FastAPI del nodo de datos (listener público + listener interno).

PÚBLICO (TLS 1.3, lo usa el cliente; autoriza con la capacidad firmada del control):
    PUT  /v1/blocks/{id}   escribe el bloque en este nodo (primario) Y lo replica a los demás
    GET  /v1/blocks/{id}   lee un bloque                    HEAD /v1/blocks/{id}  comprueba que existe

INTERNO (mTLS, solo procesos con certificado de la CA interna):
    PUT    /internal/v1/blocks/{id}/replica   el primario entrega la copia a la réplica
    GET    /internal/v1/blocks/{id}/raw       otro nodo lee la copia (reparación)
    POST   /internal/v1/repair                el control ordena copiar un bloque desde otro nodo
    DELETE /internal/v1/blocks/{id}           el control ordena borrar (GC)
    GET    /internal/v1/inventory             el control pide el inventario de bloques (GC)

FLUJO DE ESCRITURA (pipeline síncrono, cliente -> primario -> réplica):
    1. validar capacidad  2. validar largo y SHA-256  3. guardar local (TEMP->STORED)
    4. enviar a la réplica por mTLS y esperar su confirmación (<= 10 s)
    5. avisar al control (confirm)  6. responder 201.
  Solo se responde 201 cuando las DOS copias están validadas (garantía G1).
"""
from __future__ import annotations

import asyncio
import logging
import random
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from common import capabilities
from common.errors import DFShaError, install_error_handlers
from common.tls import client_ssl_context

from .blockstore import BlockCorrupt, BlockNotFound, BlockStore, IntegrityError
from .settings import Settings

log = logging.getLogger("dfsha.datanode")


class NodeState:
    """Todo lo que comparten los handlers de este nodo."""

    def __init__(self, cfg: Settings) -> None:
        self.cfg = cfg
        self.store = BlockStore(cfg.data_dir, cfg.master_key)
        ctx = client_ssl_context(cfg.tls, with_client_cert=True)   # llamadas S2S con mTLS
        self.http = httpx.AsyncClient(verify=ctx, timeout=httpx.Timeout(cfg.replica_timeout_s, connect=3.0))
        self.registered = asyncio.Event()

    # ------------------------------------------------------------ llamadas al control (con failover)
    async def control_call(self, method: str, path: str, **kwargs) -> httpx.Response:
        """Prueba cada instancia de control (en orden aleatorio) hasta que una responda."""
        urls = list(self.cfg.control_internal)
        random.shuffle(urls)
        last: Exception | None = None
        for base in urls:
            try:
                resp = await self.http.request(method, base.rstrip("/") + path, **kwargs)
            except httpx.HTTPError as exc:
                last = exc
                continue
            if resp.status_code == 503:   # esa instancia no tiene quórum: probar otra
                last = DFShaError(503, "CONTROL_NO_QUORUM", "control sin quórum", retryable=True)
                continue
            return resp
        raise DFShaError(503, "CONTROL_NO_QUORUM", f"ninguna instancia de control responde: {last}", retryable=True)


def _check_len_hash(request: Request, data: bytes, cfg: Settings) -> str:
    sha = request.headers.get("X-Block-SHA256", "")
    try:
        declared = int(request.headers.get("X-Block-Length", "-1"))
    except ValueError:
        declared = -1
    if len(data) > cfg.max_block:
        raise DFShaError(413, "BLOCK_TOO_LARGE", "el bloque supera el tamaño máximo")
    if declared != len(data) or len(sha) != 64:
        raise DFShaError(400, "INVALID_REQUEST", "X-Block-Length o X-Block-SHA256 ausentes o incorrectos")
    return sha


def _capability_token(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Capability "):
        raise DFShaError(401, "UNAUTHENTICATED", "falta la capacidad (Authorization: Capability <token>)")
    return auth[len("Capability "):].strip()


def create_apps(cfg: Settings) -> tuple[FastAPI, FastAPI, NodeState]:
    ns = NodeState(cfg)
    background: list[asyncio.Task] = []

    # ============================================================ tareas de fondo
    async def register_loop() -> None:
        """Se anuncia al control (alta dinámica) y envía el inventario inicial."""
        delay = 1.0
        while True:
            try:
                usage = await asyncio.to_thread(ns.store.usage)
                resp = await ns.control_call(
                    "POST", f"/internal/v1/nodes/{cfg.node_id}/register",
                    json={"endpoint": cfg.endpoint, "internal_endpoint": cfg.internal_endpoint, **usage},
                )
                if resp.status_code == 200:
                    log.info("registrado en el control como %s (%s)", cfg.node_id, cfg.endpoint)
                    await send_block_report()
                    ns.registered.set()
                    return
            except (DFShaError, httpx.HTTPError) as exc:
                log.warning("registro pendiente: %s", exc)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10)

    async def send_block_report() -> None:
        """Inventario -> control. El control responde qué copias debemos borrar (huérfanas/excedentes)."""
        inventory = await asyncio.to_thread(ns.store.inventory)
        resp = await ns.control_call("POST", f"/internal/v1/nodes/{cfg.node_id}/block-report", json={"blocks": inventory})
        if resp.status_code == 200:
            for block_id in resp.json().get("drop", []):
                await asyncio.to_thread(ns.store.delete, block_id)
                log.info("copia %s descartada por orden del control (huérfana o excedente)", block_id)

    async def heartbeat_loop() -> None:
        """Latido cada 5 s con espacio libre y bloques corruptos detectados."""
        await ns.registered.wait()
        while True:
            await asyncio.sleep(cfg.heartbeat_s)
            try:
                usage = await asyncio.to_thread(ns.store.usage)
                corrupt = sorted(ns.store.pending_corrupt)
                resp = await ns.control_call(
                    "POST", f"/internal/v1/nodes/{cfg.node_id}/heartbeat", json={**usage, "corrupt": corrupt}
                )
                if resp.status_code == 404:                       # el control no nos conoce: re-alta
                    ns.registered.clear()
                    await register_loop()                         # vuelve cuando el alta tuvo éxito
                    continue
                if resp.status_code == 200:
                    ns.store.pending_corrupt.difference_update(corrupt)
                    if resp.json().get("report"):                 # estuvimos ausentes: reconciliar
                        await send_block_report()
            except (DFShaError, httpx.HTTPError) as exc:
                log.warning("latido fallido: %s", exc)

    async def scrub_loop() -> None:
        """Verificación periódica de integridad en reposo (detecta corrupción silenciosa)."""
        while True:
            await asyncio.sleep(cfg.scrub_s)
            bad = await asyncio.to_thread(ns.store.scrub)
            if bad:
                log.error("scrubbing: %d bloques corruptos", len(bad))

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        background.append(asyncio.create_task(register_loop()))
        background.append(asyncio.create_task(heartbeat_loop()))
        background.append(asyncio.create_task(scrub_loop()))
        yield
        for t in background:
            t.cancel()
        await ns.http.aclose()

    public = FastAPI(title=f"DFSha nodo {cfg.node_id} (público)", lifespan=lifespan)
    internal = FastAPI(title=f"DFSha nodo {cfg.node_id} (interno)")
    for app in (public, internal):
        app.state.ns = ns
        install_error_handlers(app)

    # ============================================================ API PÚBLICA
    @public.get("/healthz")
    async def healthz():
        return {"ok": True, "node": cfg.node_id}

    @public.put("/v1/blocks/{block_id}", status_code=201)
    async def put_block(block_id: str, request: Request):
        claims = capabilities.verify(cfg.cap_key, _capability_token(request), op="put", block_id=block_id, node_id=cfg.node_id)
        data = await request.body()
        sha = _check_len_hash(request, data, cfg)
        if await asyncio.to_thread(ns.store.free_bytes) < len(data) * 2:
            raise DFShaError(507, "INSUFFICIENT_STORAGE", "el nodo no tiene espacio suficiente")

        # 1) copia local (TEMP -> STORED, cifrada)
        try:
            await asyncio.to_thread(ns.store.put, block_id, data, sha)
        except IntegrityError:
            raise DFShaError(422, "INTEGRITY_CHECK_FAILED", "el contenido no coincide con X-Block-SHA256")

        # 2) pipeline síncrono hacia las réplicas (mTLS); si una falla, el cliente reasignará el bloque
        replicas = claims.get("replicas", [])
        async def send(rep: dict):
            resp = await ns.http.put(
                f"{rep['internal_endpoint']}/internal/v1/blocks/{block_id}/replica", content=data,
                headers={"X-Block-SHA256": sha, "X-Block-Length": str(len(data)), "Content-Type": "application/octet-stream"},
            )
            if resp.status_code not in (200, 201):
                raise httpx.HTTPError(f"réplica {rep['node_id']} respondió {resp.status_code}")
        results = await asyncio.gather(*(send(r) for r in replicas), return_exceptions=True)
        for rep, result in zip(replicas, results):
            if isinstance(result, Exception):
                log.warning("réplica %s no confirmó el bloque %s: %s", rep["node_id"], block_id, result)
                raise DFShaError(503, "NODE_UNAVAILABLE", f"La réplica {rep['node_id']} no respondió; reasignar el bloque",
                                 retryable=True, extra={"failed_node": rep["node_id"]})

        # 3) avisar al control: recién ahora el bloque cuenta como confirmado (2 copias validadas)
        nodes = [cfg.node_id] + [r["node_id"] for r in replicas]
        resp = await ns.control_call(
            "POST", f"/internal/v1/blocks/{block_id}/confirm",
            json={"handle_id": claims["handle_id"], "idx": claims["idx"], "nodes": nodes, "sha256": sha, "length": len(data)},
        )
        if resp.status_code != 200:
            body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
            raise DFShaError(resp.status_code, body.get("code", "NODE_UNAVAILABLE"), body.get("message", "el control rechazó la confirmación"), retryable=bool(body.get("retryable")))
        return {"block_id": block_id, "replicas": nodes, "sha256": sha}

    @public.get("/v1/blocks/{block_id}")
    async def get_block(block_id: str, request: Request):
        capabilities.verify(cfg.cap_key, _capability_token(request), op="get", block_id=block_id, node_id=cfg.node_id)
        try:
            data, sha = await asyncio.to_thread(ns.store.get, block_id)
        except BlockNotFound:
            raise DFShaError(404, "NOT_FOUND", "el bloque no está en este nodo")
        except BlockCorrupt:
            raise DFShaError(422, "INTEGRITY_CHECK_FAILED", "copia corrupta; use otra réplica", retryable=True)
        return Response(data, media_type="application/octet-stream", headers={"X-Block-SHA256": sha})

    @public.head("/v1/blocks/{block_id}")
    async def head_block(block_id: str, request: Request):
        capabilities.verify(cfg.cap_key, _capability_token(request), op="get", block_id=block_id, node_id=cfg.node_id)
        h = await asyncio.to_thread(ns.store.header, block_id)
        if h is None:
            return Response(status_code=404)
        return Response(status_code=200, headers={"X-Block-SHA256": h["sha256"], "X-Block-Length": str(h["length"])})

    # ============================================================ API INTERNA (mTLS)
    @internal.put("/internal/v1/blocks/{block_id}/replica", status_code=201)
    async def put_replica(block_id: str, request: Request):
        data = await request.body()
        sha = _check_len_hash(request, data, cfg)
        try:
            await asyncio.to_thread(ns.store.put, block_id, data, sha)
        except IntegrityError:
            raise DFShaError(422, "INTEGRITY_CHECK_FAILED", "la réplica recibió un contenido que no coincide con el hash")
        return {"block_id": block_id, "stored": True}

    @internal.get("/internal/v1/blocks/{block_id}/raw")
    async def raw_block(block_id: str):
        try:
            data, sha = await asyncio.to_thread(ns.store.get, block_id)
        except BlockNotFound:
            raise DFShaError(404, "NOT_FOUND", "bloque inexistente")
        except BlockCorrupt:
            raise DFShaError(422, "INTEGRITY_CHECK_FAILED", "copia corrupta")
        return Response(data, media_type="application/octet-stream", headers={"X-Block-SHA256": sha})

    @internal.post("/internal/v1/repair")
    async def repair(body: dict):
        """Copia un bloque desde otro nodo (orden del control) y valida el hash antes de aceptarlo."""
        block_id, sha = body["block_id"], body["sha256"]
        src = body["source"]["internal_endpoint"]
        try:
            resp = await ns.http.get(f"{src}/internal/v1/blocks/{block_id}/raw")
        except httpx.HTTPError as exc:
            raise DFShaError(503, "NODE_UNAVAILABLE", f"no se pudo leer del nodo origen: {exc}", retryable=True)
        if resp.status_code != 200:
            raise DFShaError(503, "NODE_UNAVAILABLE", "el nodo origen no pudo entregar el bloque", retryable=True)
        try:
            await asyncio.to_thread(ns.store.put, block_id, resp.content, sha)
        except IntegrityError:
            raise DFShaError(422, "INTEGRITY_CHECK_FAILED", "el bloque copiado no coincide con el hash esperado")
        log.info("bloque %s reparado desde %s", block_id, body["source"].get("node_id"))
        return {"ok": True}

    @internal.delete("/internal/v1/blocks/{block_id}")
    async def delete_block(block_id: str):
        return {"deleted": await asyncio.to_thread(ns.store.delete, block_id)}

    @internal.get("/internal/v1/inventory")
    async def inventory():
        return {"node_id": cfg.node_id, "blocks": await asyncio.to_thread(ns.store.inventory)}

    return public, internal, ns
