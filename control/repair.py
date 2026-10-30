"""Reparación de réplicas (CU13): restaura R copias cuando un nodo muere.

Lo ejecuta SOLO el líder. En cada pasada recorre /blk/* y, para cada bloque:

  copias vivas = nodos de su lista que no están DEAD
  - vivas >= R : nada que hacer.
  - vivas == 0 : bloque PERDIDO (se registra; nunca se sirve contenido corrupto o inventado).
  - 1 <= vivas < R : se elige un nodo HEALTHY que no lo tenga (destino) y se le ordena copiarlo
                     desde un nodo vivo (origen). El destino valida el SHA-256 antes de aceptarlo.
                     Luego se actualiza /blk con CAS.

Un nodo SUSPECT no se repara todavía: puede ser solo una pausa de red. Solo cuando pasa a DEAD
(120 s sin latido) se considera perdido.
"""
from __future__ import annotations

import logging
import time

import httpx

from . import blocks as blockmod
from . import nodes as nodesmod
from .state import State

log = logging.getLogger("dfsha.repair")


async def repair_pass(st: State, max_blocks: int = 200) -> int:
    """Una pasada de reparación. Devuelve cuántos bloques se repararon."""
    registry = {n.node_id: n for n in await nodesmod.list_nodes(st)}
    healthy = [n for n in registry.values() if n.state == nodesmod.HEALTHY]
    repaired = 0
    for kv in await st.etcd.range_all("/blk/"):
        rec = kv.json
        live = [n for n in rec["nodes"] if n in registry and registry[n].state != nodesmod.DEAD]
        if len(live) >= st.cfg.replication:
            continue
        block_id = kv.key[len("/blk/"):]
        if not live:
            if not rec.get("lost_logged"):
                log.error("bloque %s PERDIDO: ninguna copia en nodos vivos", block_id)
                st.metrics["blocks_lost"] += 1
            continue
        candidates = [n for n in healthy if n.node_id not in live]
        if not candidates:
            continue
        dest = min(candidates, key=lambda n: (n.block_count, n.node_id))
        src = registry[sorted(live, key=lambda n: registry[n].state != nodesmod.HEALTHY)[0]]
        started = time.monotonic()
        try:
            resp = await st.node_http.post(
                dest.internal_endpoint + "/internal/v1/repair",
                json={
                    "block_id": block_id, "sha256": rec["sha256"], "length": rec["length"],
                    "source": {"node_id": src.node_id, "internal_endpoint": src.internal_endpoint},
                },
            )
        except httpx.HTTPError as exc:
            log.warning("reparación de %s hacia %s falló: %s", block_id, dest.node_id, exc)
            continue
        if resp.status_code != 200:
            log.warning("reparación de %s rechazada por %s: %s", block_id, dest.node_id, resp.text[:120])
            continue
        if await blockmod.set_nodes(st, kv, live + [dest.node_id]):
            repaired += 1
            st.metrics["repairs"] += 1
            dest.block_count += 1
            log.info("bloque %s reparado: %s -> %s (%.0f ms)", block_id, src.node_id, dest.node_id, (time.monotonic() - started) * 1000)
        if repaired >= max_blocks:
            break
    return repaired
