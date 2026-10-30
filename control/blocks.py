"""Registro de ubicación de bloques:  /blk/{block_id}  ->  dónde están sus copias.

    /blk/b-1a2b...  =  {"nodes": ["dn-a","dn-b"], "sha256": "...", "length": 4194304, "ts": ...}

¿Por qué separar la ubicación del manifiesto del archivo? Porque con copy-on-write un mismo
bloque puede aparecer en varias versiones de un archivo. Si la ubicación viviera dentro de
cada manifiesto, reparar una réplica obligaría a reescribir todos los manifiestos que lo
referencian. Así, reparar = actualizar UNA clave.
"""
from __future__ import annotations

import time

from .etcd import KV, c_mod, dumps, o_put
from .state import State


def blk_key(block_id: str) -> str:
    return f"/blk/{block_id}"


async def get_block(st: State, block_id: str) -> KV | None:
    return await st.etcd.get(blk_key(block_id))


async def set_nodes(st: State, kv: KV, nodes: list[str]) -> bool:
    """Cambia la lista de nodos de un bloque con CAS (falla si alguien lo modificó antes)."""
    rec = kv.json
    rec["nodes"] = nodes
    res = await st.etcd.txn([c_mod(kv.key, kv.mod_rev)], [o_put(kv.key, dumps(rec))])
    return res.succeeded


async def drop_copy(st: State, block_id: str, node_id: str) -> None:
    """Quita un nodo de la lista de un bloque (p. ej. porque reportó la copia corrupta).

    Al quedar el bloque con menos de R copias, el líder lo repara en el siguiente ciclo.
    """
    for _ in range(3):
        kv = await get_block(st, block_id)
        if kv is None or node_id not in kv.json["nodes"]:
            return
        if await set_nodes(st, kv, [n for n in kv.json["nodes"] if n != node_id]):
            return


async def block_report(st: State, node_id: str, blocks: list[dict]) -> list[str]:
    """Procesa el inventario que un nodo envía al arrancar o al reincorporarse.

    Compara por block_id + SHA-256 (no por fechas):
      * copia válida y al bloque le faltan réplicas  -> se REINCORPORA el nodo;
      * copia válida pero el bloque ya tiene R copias (se reparó mientras estuvo caído)
        -> copia EXCEDENTE: se le pide al nodo que la borre;
      * bloque que ningún metadato conoce y es más viejo que el periodo de gracia -> HUÉRFANO: se borra;
      * hash distinto al registrado -> copia corrupta: se borra (se repara desde la otra réplica).
    Devuelve la lista de block_id que el nodo debe eliminar.
    """
    from .nodes import DEAD, effective_state  # import local: evita ciclo con nodes.py

    drop: list[str] = []
    known = await st.etcd.get_many([blk_key(b["block_id"]) for b in blocks])
    node_kvs = {kv.key: kv for kv in await st.etcd.range_all("/n/")}
    now = time.time()
    for b in blocks:
        kv = known.get(blk_key(b["block_id"]))
        if kv is None:
            if now - b.get("mtime", now) > st.cfg.grace_orphan_s:
                drop.append(b["block_id"])
            continue
        rec = kv.json
        if rec["sha256"] != b["sha256"]:
            drop.append(b["block_id"])
            continue
        if node_id in rec["nodes"]:
            continue
        live = [
            n for n in rec["nodes"]
            if f"/n/{n}" in node_kvs and effective_state(st, node_kvs[f"/n/{n}"].json["last_hb"], now) != DEAD
        ]
        if len(live) < st.cfg.replication:
            await set_nodes(st, kv, live + [node_id])
            st.metrics["reincorporated"] += 1
        else:
            drop.append(b["block_id"])
    return drop
