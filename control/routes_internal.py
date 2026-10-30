"""API INTERNA del servicio de control (nodos -> control). Sección 4.3 del diseño.

Se sirve en un puerto distinto con mTLS obligatorio (ver common/server.py): solo procesos con
un certificado firmado por la CA interna pueden llegar hasta aquí. Ningún JWT de usuario sirve.
"""
from __future__ import annotations

import time

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from common.errors import DFShaError

from . import blocks as blockmod
from . import files, nodes
from .state import State, get_state

router = APIRouter(prefix="/internal/v1")


class RegisterReq(BaseModel):
    endpoint: str                 # URL pública del nodo (la que usa el cliente)
    internal_endpoint: str        # URL interna (mTLS)
    free_bytes: int = 0
    used_bytes: int = 0
    block_count: int = 0


class HeartbeatReq(BaseModel):
    free_bytes: int = 0
    used_bytes: int = 0
    block_count: int = 0
    corrupt: list[str] = []       # bloques cuya copia local falló la verificación


class ReportReq(BaseModel):
    blocks: list[dict]            # [{block_id, sha256, length, mtime}]


class ConfirmReq(BaseModel):
    handle_id: str
    idx: int
    nodes: list[str]
    sha256: str
    length: int


@router.post("/nodes/{node_id}/register")
async def register(node_id: str, body: RegisterReq, st: State = Depends(get_state)):
    await nodes.register(st, node_id, body.model_dump())
    return {"ok": True}


@router.post("/nodes/{node_id}/heartbeat")
async def heartbeat(node_id: str, body: HeartbeatReq, st: State = Depends(get_state)):
    known, gap = await nodes.heartbeat(st, node_id, body.model_dump())
    if not known:
        raise DFShaError(404, "NOT_FOUND", "nodo no registrado; vuelva a registrarse")
    for block_id in body.corrupt:           # copia corrupta -> se descarta y se repara desde la otra réplica
        await blockmod.drop_copy(st, block_id, node_id)
    # Si el nodo estuvo ausente (más de SUSPECT) pedimos un inventario para reincorporar sus copias.
    return {"ok": True, "report": gap >= st.cfg.suspect_after_s}


@router.post("/nodes/{node_id}/block-report")
async def block_report(node_id: str, body: ReportReq, st: State = Depends(get_state)):
    return {"drop": await blockmod.block_report(st, node_id, body.blocks)}


@router.post("/blocks/{block_id}/confirm")
async def confirm(block_id: str, body: ConfirmReq, st: State = Depends(get_state)):
    await files.confirm_block(st, block_id, body.model_dump())
    return {"ok": True}


@router.get("/status")
async def status(st: State = Depends(get_state)):
    """Estado del clúster visto por esta instancia (evidencia para las pruebas y el informe)."""
    leader = st.leader
    return {
        "instance": st.cfg.instance_id,
        "is_leader": bool(leader and leader.is_leader),
        "time": time.time(),
        "nodes": [n.__dict__ for n in await nodes.list_nodes(st)],
        "metrics": dict(st.metrics),
    }
