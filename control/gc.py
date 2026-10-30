"""Recolección de basura (sección 5.4): libera versiones y bloques que nadie referencia.

Lo ejecuta SOLO el líder. Usa "marcar y barrer" (mark & sweep), que es más simple y robusto que
contar referencias cuando un bloque puede compartirse entre versiones (copy-on-write):

  1. PINES: qué versiones están fijadas por algún handle abierto (lease vigente) y qué bloques
     están asignados/pendientes de commit. Se lee ANTES que los manifiestos (ver nota).
  2. Versiones REEMPLAZADAS (/retired) con más de `grace_version` s y sin pines -> se borra su
     manifiesto. Archivos BORRADOS (/deleted) igual, con todas sus versiones.
  3. MARCA: conjunto de block_id referenciados por algún manifiesto restante o por un pin.
  4. BARRIDO de /blk: bloque no marcado y con más de `grace_orphan` s -> se pide a sus nodos que
     lo borren y se elimina el registro.
  5. BARRIDO de nodos: bloques que un nodo guarda pero que ningún metadato conoce (p. ej. una
     escritura que murió antes del confirm) y son más viejos que el periodo de gracia -> se borran.

NOTA de orden (corrección): un commit mueve un bloque de /h/*/pend a /f/*/b. Si leyéramos
primero /f y después /h, un commit intermedio dejaría el bloque fuera de AMBOS conjuntos y lo
borraríamos por error. Leyendo primero /h y luego /f, el bloque está siempre en al menos uno.
Además, el periodo de gracia de 1 h cubre cualquier escritura en vuelo.
"""
from __future__ import annotations

import logging
import time

import httpx

from . import nodes as nodesmod
from .etcd import c_mod, o_del
from .state import State

log = logging.getLogger("dfsha.gc")


async def gc_pass(st: State) -> dict:
    cfg, now = st.cfg, time.time()
    stats = {"versions": 0, "files": 0, "blocks": 0, "orphans": 0, "idem": 0}

    # 1) pines (handles) — ANTES de leer manifiestos
    pinned_versions: set[tuple[str, int]] = set()
    pinned_files: set[str] = set()
    referenced: set[str] = set()
    for kv in await st.etcd.range_all("/h/"):
        parts = kv.key.split("/")           # ['', 'h', hid]  o  ['', 'h', hid, 'pend'|'alloc', idx]
        if len(parts) == 3:
            d = kv.json
            pinned_versions.add((d["file_id"], d["base_version"]))
            pinned_files.add(d["file_id"])
        else:
            referenced.add(kv.json["block_id"])

    # 2a) versiones reemplazadas
    for kv in await st.etcd.range_all("/retired/"):
        _, _, fid, ver = kv.key.split("/")  # '/retired/{fid}/v{n}'
        n = int(ver[1:])
        if now - kv.json["ts"] > cfg.grace_version_s and (fid, n) not in pinned_versions:
            await st.etcd.txn([], [o_del(f"/f/{fid}/v{n}/", prefix=True), o_del(kv.key)])
            stats["versions"] += 1
    # 2b) archivos eliminados (rm / baja de usuario)
    for kv in await st.etcd.range_all("/deleted/"):
        fid = kv.key[len("/deleted/"):]
        if now - kv.json["ts"] > cfg.grace_version_s and fid not in pinned_files:
            await st.etcd.txn([], [o_del(f"/f/{fid}/", prefix=True), o_del(f"/retired/{fid}/", prefix=True), o_del(kv.key)])
            stats["files"] += 1

    # 3) marca: bloques referenciados por manifiestos vivos
    for kv in await st.etcd.range_all("/f/"):
        if "/b/" in kv.key:
            referenced.add(kv.json["block_id"])

    # 4) barrido del registro /blk
    registry = {n.node_id: n for n in await nodesmod.list_nodes(st)}
    known: set[str] = set()
    for kv in await st.etcd.range_all("/blk/"):
        bid = kv.key[len("/blk/"):]
        known.add(bid)
        rec = kv.json
        if bid in referenced or now - rec["ts"] <= cfg.grace_orphan_s:
            continue
        for nid in rec["nodes"]:
            n = registry.get(nid)
            if n and n.state != nodesmod.DEAD:
                await _delete_on_node(st, n.internal_endpoint, bid)
        await st.etcd.txn([c_mod(kv.key, kv.mod_rev)], [o_del(kv.key)])
        stats["blocks"] += 1

    # 5) barrido de inventarios: bloques en disco sin metadatos
    for n in registry.values():
        if n.state != nodesmod.HEALTHY:
            continue
        try:
            resp = await st.node_http.get(n.internal_endpoint + "/internal/v1/inventory")
            inventory = resp.json()["blocks"] if resp.status_code == 200 else []
        except (httpx.HTTPError, ValueError, KeyError):
            continue
        for b in inventory:
            if b["block_id"] in referenced or b["block_id"] in known:
                continue
            if now - b["mtime"] > cfg.grace_orphan_s:
                await _delete_on_node(st, n.internal_endpoint, b["block_id"])
                stats["orphans"] += 1

    # 6) resultados de idempotencia viejos (> 10 min)
    for kv in await st.etcd.range_all("/idem/"):
        if now - kv.json["ts"] > 600:
            await st.etcd.delete(kv.key)
            stats["idem"] += 1

    for k, v in stats.items():
        st.metrics["gc_" + k] += v
    if any(stats.values()):
        log.info("GC: %s", stats)
    return stats


async def _delete_on_node(st: State, internal_endpoint: str, block_id: str) -> None:
    try:
        await st.node_http.delete(f"{internal_endpoint}/internal/v1/blocks/{block_id}")
    except httpx.HTTPError as exc:
        log.warning("no se pudo borrar %s en %s: %s", block_id, internal_endpoint, exc)
