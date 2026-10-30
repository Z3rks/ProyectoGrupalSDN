"""Registro dinámico de nodos de datos (RNF7: el cliente nunca conoce direcciones fijas).

Cada nodo se anuncia con POST /internal/v1/nodes/register y renueva su "latido"
(heartbeat) cada 5 s. El control guarda en etcd la clave /n/{node_id}:

    {"endpoint": "https://10.0.1.11:8443",       # lo que usa el CLIENTE
     "internal_endpoint": "https://10.0.1.11:8444",  # lo que usan los otros servidores (mTLS)
     "last_hb": 1767000000.0, "free_bytes": ..., "used_bytes": ..., "block_count": ...}

El ESTADO (HEALTHY / SUSPECT / DEAD) no se guarda: se calcula en el momento a partir de la
edad del último latido. Así cualquier instancia de control (no solo el líder) decide igual:
    edad < 15 s  -> HEALTHY    (recibe asignaciones nuevas)
    15–120 s     -> SUSPECT    (no recibe asignaciones; se sigue leyendo de ella si hace falta)
    > 120 s      -> DEAD       (el líder repara sus bloques en otros nodos)
"""
from __future__ import annotations

import time
from dataclasses import dataclass

from .etcd import dumps
from .state import State

HEALTHY, SUSPECT, DEAD = "HEALTHY", "SUSPECT", "DEAD"


@dataclass
class NodeInfo:
    node_id: str
    endpoint: str
    internal_endpoint: str
    last_hb: float
    free_bytes: int
    used_bytes: int
    block_count: int
    state: str = HEALTHY


def effective_state(st: State, last_hb: float, now: float | None = None) -> str:
    age = (now if now is not None else time.time()) - last_hb
    if age < st.cfg.suspect_after_s:
        return HEALTHY
    if age < st.cfg.dead_after_s:
        return SUSPECT
    return DEAD


async def list_nodes(st: State) -> list[NodeInfo]:
    """Todos los nodos registrados, con su estado efectivo calculado ahora."""
    now = time.time()
    out = []
    for kv in await st.etcd.range_all("/n/"):
        d = kv.json
        out.append(
            NodeInfo(
                node_id=kv.key[len("/n/") :],
                endpoint=d["endpoint"],
                internal_endpoint=d["internal_endpoint"],
                last_hb=d["last_hb"],
                free_bytes=d.get("free_bytes", 0),
                used_bytes=d.get("used_bytes", 0),
                block_count=d.get("block_count", 0),
                state=effective_state(st, d["last_hb"], now),
            )
        )
    return sorted(out, key=lambda n: n.node_id)


async def register(st: State, node_id: str, body: dict) -> None:
    """Alta (o actualización) de un nodo. Idempotente: un nodo que se reinicia vuelve a llamarla."""
    rec = {
        "endpoint": body["endpoint"],
        "internal_endpoint": body["internal_endpoint"],
        "last_hb": time.time(),
        "free_bytes": body.get("free_bytes", 0),
        "used_bytes": body.get("used_bytes", 0),
        "block_count": body.get("block_count", 0),
    }
    await st.etcd.put(f"/n/{node_id}", dumps(rec))


async def heartbeat(st: State, node_id: str, body: dict) -> tuple[bool, float]:
    """Registra un latido. Devuelve (nodo_conocido, segundos_desde_el_latido_anterior)."""
    kv = await st.etcd.get(f"/n/{node_id}")
    if kv is None:
        return False, 0.0
    rec = kv.json
    now = time.time()
    gap = now - rec["last_hb"]
    rec.update(
        last_hb=now,
        free_bytes=body.get("free_bytes", rec.get("free_bytes", 0)),
        used_bytes=body.get("used_bytes", rec.get("used_bytes", 0)),
        block_count=body.get("block_count", rec.get("block_count", 0)),
    )
    await st.etcd.put(f"/n/{node_id}", dumps(rec))
    return True, gap
