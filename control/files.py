"""Operaciones de archivo del plano de control (RF2/RF3): handles, locks, manifiestos, commit.

Este módulo implementa la semántica de consistencia del diseño (sección 5):

  * Cada archivo es una secuencia de VERSIONES INMUTABLES. Una escritura crea bloques nuevos
    (copy-on-write) y se hace visible de golpe en el `close`, con UNA transacción de etcd.
  * Escritores de un mismo archivo se serializan con un LOCK exclusivo (clave con lease en etcd).
  * El commit es compare-and-swap: comprueba que el lock sigue siendo el mío (FENCING) y que la
    entrada del namespace no cambió desde el open. Si algo falló no se publica NADA.
  * Los lectores trabajan sobre la versión fijada al abrir (SNAPSHOT) y nunca bloquean.

Claves de etcd que usa este módulo
----------------------------------
  /h/{hid}                 handle abierto (lease 60 s)  -> uid, archivo, versión base, lock, ...
  /h/{hid}/alloc/{idx}     bloque asignado al índice idx (aún no confirmado)      [mismo lease]
  /h/{hid}/pend/{idx}      bloque confirmado con sus 2 copias, pendiente de commit [mismo lease]
  /lock/{uid}:{ruta}       lock exclusivo de escritura (lease 30 s)
  /lockid/{lock_id}        índice lock_id -> clave del lock
  /f/{fid}/v{n}/meta       cabecera de la versión n (tamaño, hash total, nº de bloques)
  /f/{fid}/v{n}/b/{idx}    manifiesto: block_id, longitud y SHA-256 del bloque idx
  /retired/{fid}/v{n}      versión reemplazada (el GC la borra tras el periodo de gracia)
  /idem/{uid}/{clave}      resultado guardado de una operación idempotente
"""
from __future__ import annotations

import json
import logging
import random
import time
import uuid

from common import capabilities
from common.errors import DFShaError

from . import namespace, nodes as nodesmod, paths, placement
from .auth import Principal
from .etcd import (
    KV,
    LeaseNotFound,
    c_absent,
    c_exists,
    c_mod,
    dumps,
    o_del,
    o_put,
)
from .state import State

log = logging.getLogger("dfsha.files")
BATCH = 100  # operaciones máximas por transacción de etcd


def _id(prefix: str) -> str:
    return prefix + uuid.uuid4().hex[:16]


def hkey(hid: str) -> str:
    return f"/h/{hid}"


def lock_key(uid: str, path: str) -> str:
    return f"/lock/{uid}:{path}"


def lockid_key(lock_id: str) -> str:
    return f"/lockid/{lock_id}"


def ver_prefix(fid: str, version: int) -> str:
    return f"/f/{fid}/v{version}/"


def bkey(fid: str, version: int, idx: int) -> str:
    return f"/f/{fid}/v{version}/b/{idx:010d}"


# ====================================================================================
# Idempotencia: repetir una operación con la misma Idempotency-Key devuelve el mismo resultado
# ====================================================================================
async def _idem_get(st: State, uid: str, key: str | None) -> dict | None:
    if not key:
        return None
    kv = await st.etcd.get(f"/idem/{uid}/{key}")
    return kv.json["result"] if kv else None


async def _idem_put(st: State, uid: str, key: str | None, result: dict) -> None:
    if key:
        await st.etcd.put(f"/idem/{uid}/{key}", dumps({"result": result, "ts": time.time()}))


# ====================================================================================
# Locks
# ====================================================================================
async def _acquire_lock(st: State, uid: str, path: str, lock_id_hint: str | None) -> dict:
    """Toma el lock exclusivo del archivo `path` (o reutiliza uno explícito del mismo usuario).

    El lock es una clave creada con la transacción "si no existe" (c_absent): solo UN cliente
    puede ganarla. Está atada a un lease de 30 s; si el dueño muere y no renueva, desaparece sola.
    El token de FENCING es la revisión global de etcd en la que se creó: crece de forma
    monótona, así que un dueño antiguo nunca puede confundirse con el actual.
    """
    key = lock_key(uid, path)
    if lock_id_hint:  # el usuario ya hizo lock() explícito y ahora abre el archivo
        kv = await st.etcd.get(key)
        if kv is None:
            raise DFShaError(409, "LOCK_LOST", "el lock indicado ya no existe")
        rec = kv.json
        if rec["lock_id"] != lock_id_hint or rec["uid"] != uid:
            raise DFShaError(409, "LOCKED", "el archivo está bloqueado por otro titular", retryable=True, retry_after_s=5)
        return {"lock_id": rec["lock_id"], "fencing": kv.mod_rev, "explicit": True, "lease": rec["lease"]}

    lease = await st.etcd.lease_grant(st.cfg.lock_ttl_s)
    lock_id = _id("lk-")
    rec = {"lock_id": lock_id, "uid": uid, "path": path, "lease": lease, "ts": time.time()}
    res = await st.etcd.txn(
        [c_absent(key)],
        [o_put(key, dumps(rec), lease), o_put(lockid_key(lock_id), dumps({"key": key}), lease)],
    )
    if not res.succeeded:
        await st.etcd.lease_revoke(lease)
        raise DFShaError(409, "LOCKED", "el archivo tiene un bloqueo de escritura vigente", retryable=True, retry_after_s=5)
    return {"lock_id": lock_id, "fencing": res.revision, "explicit": False, "lease": lease}


async def _release_lock(st: State, uid: str, path: str, lock_id: str) -> None:
    """Libera el lock solo si sigue siendo el mismo (no borra el de otro cliente)."""
    key = lock_key(uid, path)
    kv = await st.etcd.get(key)
    if kv is None or kv.json["lock_id"] != lock_id:
        return
    res = await st.etcd.txn([c_mod(key, kv.mod_rev)], [o_del(key), o_del(lockid_key(lock_id))])
    if res.succeeded:
        await st.etcd.lease_revoke(kv.json["lease"])


async def lock_file(st: State, p: Principal, path: str) -> dict:
    """lock() explícito (CU11). El archivo debe existir."""
    path = paths.normalize(path)
    kv = await namespace.get_entry(st, p.uid, path)
    if kv is None:
        raise DFShaError(404, "NOT_FOUND", "el archivo no existe")
    if kv.json["type"] != "file":
        raise DFShaError(409, "IS_A_DIRECTORY", "no se puede bloquear un directorio")
    lk = await _acquire_lock(st, p.uid, path, None)
    return {"lock_id": lk["lock_id"], "fencing": lk["fencing"], "expires_in": st.cfg.lock_ttl_s}


async def _lock_by_id(st: State, p: Principal, lock_id: str) -> KV:
    idx = await st.etcd.get(lockid_key(lock_id))
    kv = await st.etcd.get(idx.json["key"]) if idx else None
    if kv is None or kv.json["uid"] != p.uid or kv.json["lock_id"] != lock_id:
        raise DFShaError(404, "NOT_FOUND", "lock inexistente o vencido")
    return kv


async def renew_lock(st: State, p: Principal, lock_id: str) -> dict:
    kv = await _lock_by_id(st, p, lock_id)
    try:
        ttl = await st.etcd.lease_keepalive(kv.json["lease"])
    except LeaseNotFound:
        raise DFShaError(409, "LOCK_LOST", "el lock venció")
    return {"lock_id": lock_id, "expires_in": ttl}


async def unlock(st: State, p: Principal, lock_id: str) -> None:
    kv = await _lock_by_id(st, p, lock_id)
    await _release_lock(st, p.uid, kv.json["path"], lock_id)


# ====================================================================================
# open / renew / close
# ====================================================================================
async def _load_handle(st: State, hid: str, uid: str) -> tuple[dict, KV]:
    kv = await st.etcd.get(hkey(hid))
    if kv is None:
        raise DFShaError(410, "HANDLE_EXPIRED", "el handle no existe o venció; reabra el archivo")
    h = kv.json
    if h["uid"] != uid:  # no revelamos que el handle existe
        raise DFShaError(404, "NOT_FOUND", "handle inexistente")
    return h, kv


async def open_file(
    st: State,
    p: Principal,
    path: str,
    mode: str,
    create: bool,
    truncate: bool,
    expected_version: int | None,
    lock_id: str | None,
    idem_key: str | None,
) -> dict:
    """open(path, modo).

    r : fija la versión vigente (snapshot) y crea un handle con lease de 60 s.
    w : además toma el lock exclusivo. Con truncate=true la nueva versión REEMPLAZA a la anterior.
    a : como w pero el cliente solo añade al final.
    """
    if mode not in ("r", "w", "a"):
        raise DFShaError(400, "INVALID_REQUEST", "modo inválido (r, w o a)")
    if mode == "a" and truncate:
        raise DFShaError(400, "INVALID_REQUEST", "truncate no aplica al modo a")
    path = paths.normalize(path)
    paths.split(path)  # rechaza la raíz
    uid = p.uid
    cached = await _idem_get(st, uid, idem_key)
    if cached is not None:
        return cached

    kv = await namespace.get_entry(st, uid, path)
    entry = kv.json if kv else None
    if entry and entry["type"] == "dir":
        raise DFShaError(409, "IS_A_DIRECTORY", "la ruta es un directorio")
    if entry and expected_version is not None and entry["version"] != expected_version:
        raise DFShaError(409, "VERSION_CONFLICT", f"la versión vigente es {entry['version']}")

    creating = False
    if mode == "r":
        if entry is None:
            raise DFShaError(404, "NOT_FOUND", "el archivo no existe")
    elif entry is None:
        if not create:
            raise DFShaError(404, "NOT_FOUND", "el archivo no existe (use create)")
        await namespace.require_parent_dir(st, uid, path)
        creating = True

    file_id = entry["file_id"] if entry else _id("f-")
    base_version = entry["version"] if entry else 0
    base_size = entry["size"] if entry else 0
    replace = bool(truncate) or creating

    lock = None
    if mode in ("w", "a"):
        lock = await _acquire_lock(st, uid, path, lock_id)

    hid = _id("h-")
    try:
        lease = await st.etcd.lease_grant(st.cfg.handle_ttl_s)
        handle = {
            "handle_id": hid, "uid": uid, "path": path, "mode": mode, "file_id": file_id,
            "base_version": base_version, "base_size": base_size,
            "ns_rev": kv.mod_rev if kv else 0,   # para el CAS del commit
            "creating": creating, "replace": replace, "lease": lease,
            "lock": lock, "ts": time.time(),
        }
        await st.etcd.put(hkey(hid), dumps(handle), lease)
    except Exception:
        if lock and not lock["explicit"]:
            await _release_lock(st, uid, path, lock["lock_id"])
        raise

    result = {
        "handle_id": hid, "file_id": file_id, "path": path, "mode": mode,
        "base_version": base_version,
        "size": 0 if replace else base_size,   # con truncate el cliente ve un archivo vacío
        "sha256_total": None if replace else (entry or {}).get("sha256"),
        "block_size": st.cfg.block_size, "handle_ttl": st.cfg.handle_ttl_s,
        "lock": ({"lock_id": lock["lock_id"], "fencing": lock["fencing"], "expires_in": st.cfg.lock_ttl_s} if lock else None),
    }
    await _idem_put(st, uid, idem_key, result)
    return result


async def renew_handle(st: State, p: Principal, hid: str) -> dict:
    """Renueva el lease del handle y del lock (el cliente lo llama cada ~10 s mientras trabaja)."""
    h, _ = await _load_handle(st, hid, p.uid)
    try:
        handle_ttl = await st.etcd.lease_keepalive(h["lease"])
    except LeaseNotFound:
        raise DFShaError(410, "HANDLE_EXPIRED", "el handle venció")
    lock_ttl = None
    if h["lock"]:
        kv = await st.etcd.get(lock_key(h["uid"], h["path"]))
        if kv is None or kv.mod_rev != h["lock"]["fencing"]:
            raise DFShaError(409, "LOCK_LOST", "el lock ya no es de este handle")
        try:
            lock_ttl = await st.etcd.lease_keepalive(kv.json["lease"])
        except LeaseNotFound:
            raise DFShaError(409, "LOCK_LOST", "el lock venció")
    return {"handle_ttl": handle_ttl, "lock_ttl": lock_ttl}


async def _end_handle(st: State, h: dict) -> None:
    """Cierra un handle sin publicar: revoca su lease (borra alloc/pend) y libera su lock."""
    await st.etcd.lease_revoke(h["lease"])
    lk = h.get("lock")
    if lk and not lk["explicit"]:
        await _release_lock(st, h["uid"], h["path"], lk["lock_id"])


async def close_handle(
    st: State, p: Principal, hid: str, commit: bool, size: int | None, sha256_total: str | None, idem_key: str | None
) -> dict:
    """close(). En lectura libera el snapshot. En escritura con commit=true publica la versión."""
    cached = await _idem_get(st, p.uid, idem_key)
    if cached is not None:
        return cached
    h, _ = await _load_handle(st, hid, p.uid)
    if h["mode"] == "r" or not commit:
        await _end_handle(st, h)
        result = {"closed": True, "committed": False}
        await _idem_put(st, p.uid, idem_key, result)
        return result
    return await _commit(st, h, size, sha256_total, idem_key)


async def _commit(st: State, h: dict, size: int | None, sha256_total: str | None, idem_key: str | None) -> dict:
    """Publica la nueva versión de forma ATÓMICA (garantías G1–G4 del diseño).

    Pasos:
      1. Reunir, para cada índice 0..n-1 del archivo final, el bloque nuevo (pend) o el heredado
         de la versión base (copy-on-write) y validar longitudes.
      2. Escribir el manifiesto de la versión n+1 en lotes. Esas claves son INVISIBLES: nadie las
         referencia todavía. Cada lote exige que el lock siga siendo el mío (fencing).
      3. UNA transacción final que a la vez: verifica fencing + estado del namespace, crea la
         cabecera de la versión, apunta la entrada del namespace a la versión nueva y libera
         lock y handle. Si falla, el archivo sigue en la versión anterior (nada se publicó).
    """
    cfg = st.cfg
    if size is None or size < 0:
        raise DFShaError(400, "INVALID_REQUEST", "falta el tamaño final del archivo")
    bs = cfg.block_size
    nblocks = (size + bs - 1) // bs
    hid, fid, uid, path = h["handle_id"], h["file_id"], h["uid"], h["path"]
    lk = lock_key(uid, path)
    fencing = h["lock"]["fencing"]

    # 1) bloques nuevos confirmados + bloques heredados
    pend = {int(kv.key.rsplit("/", 1)[1]): kv.json for kv in await st.etcd.range_all(f"/h/{hid}/pend/")}
    inherited: dict[int, dict] = {}
    if not h["replace"] and h["base_version"] > 0 and any(i not in pend for i in range(nblocks)):
        for kv in await st.etcd.range_all(ver_prefix(fid, h["base_version"]) + "b/"):
            inherited[int(kv.key.rsplit("/", 1)[1])] = kv.json

    entries: dict[int, dict] = {}
    for i in range(nblocks):
        rec = pend.get(i) or inherited.get(i)
        if rec is None:
            raise DFShaError(422, "INCOMPLETE_FILE", f"falta el bloque {i}; no se publica el archivo")
        if rec["length"] != min(bs, size - i * bs):
            raise DFShaError(422, "INTEGRITY_CHECK_FAILED", f"el tamaño del bloque {i} no coincide con el archivo")
        if i in pend and len(set(pend[i]["nodes"])) < cfg.replication:
            raise DFShaError(503, "NODE_UNAVAILABLE", f"el bloque {i} no tiene {cfg.replication} copias", retryable=True)
        entries[i] = {"block_id": rec["block_id"], "length": rec["length"], "sha256": rec["sha256"]}

    # 2) manifiesto de la versión nueva (invisible hasta el paso 3)
    new_version = h["base_version"] + 1
    guard = [c_mod(lk, fencing), c_exists(hkey(hid))]
    lost = DFShaError(409, "LOCK_LOST", "el lock o el handle venció; no se publicó nada")
    res = await st.etcd.txn(guard, [o_del(ver_prefix(fid, new_version), prefix=True)])  # limpia intentos previos
    if not res.succeeded:
        raise lost
    idxs = sorted(entries)
    for i in range(0, len(idxs), BATCH):
        ops = [o_put(bkey(fid, new_version, j), dumps(entries[j])) for j in idxs[i : i + BATCH]]
        if not (await st.etcd.txn(guard, ops)).succeeded:
            raise lost

    # 3) publicación atómica
    now = time.time()
    nsk = paths.ns_key(uid, path)
    result = {"file_id": fid, "version": new_version, "size": size}
    cmp = [c_mod(lk, fencing), c_exists(hkey(hid))]
    ops = [
        o_put(f"/f/{fid}/v{new_version}/meta", dumps({"size": size, "sha256_total": sha256_total, "nblocks": nblocks, "state": "COMMITTED", "ts": now, "uid": uid})),
        o_put(nsk, dumps({"type": "file", "file_id": fid, "version": new_version, "size": size, "mtime": now, "sha256": sha256_total})),
        o_del(hkey(hid)),
        o_del(f"/h/{hid}/", prefix=True),
    ]
    if h["creating"]:   # el archivo no debe haber aparecido mientras tanto
        pcmp, pops = namespace.parent_guard(uid, path)
        cmp += [c_absent(nsk), *pcmp]
        ops += pops
    else:               # la entrada no cambió desde el open (nadie hizo rm ni commit)
        cmp.append(c_mod(nsk, h["ns_rev"]))
    if h["base_version"] > 0:
        ops.append(o_put(f"/retired/{fid}/v{h['base_version']}", dumps({"ts": now})))
    if not h["lock"]["explicit"]:
        ops += [o_del(lk), o_del(lockid_key(h["lock"]["lock_id"]))]
    if idem_key:   # el resultado se guarda EN LA MISMA transacción: un reintento nunca ve "a medias"
        ops.append(o_put(f"/idem/{uid}/{idem_key}", dumps({"result": result, "ts": now})))

    if not (await st.etcd.txn(cmp, ops)).succeeded:
        raise DFShaError(409, "VERSION_CONFLICT", "el archivo cambió o el lock se perdió; reabra y reaplique su cambio")

    await st.etcd.lease_revoke(h["lease"])
    if not h["lock"]["explicit"]:
        await st.etcd.lease_revoke(h["lock"]["lease"])   # la clave del lock ya se borró en la transacción
    st.metrics["commits"] += 1
    return result


# ====================================================================================
# allocate / confirm / manifest  (reparto y localización de bloques)
# ====================================================================================
async def allocate(st: State, p: Principal, hid: str, indices: list[int], exclude: list[str]) -> list[dict]:
    """Reserva bloques nuevos: elige primario y réplica y firma una capacidad de escritura.

    `exclude` permite al cliente descartar nodos que acaba de ver fallar (reasignación).
    """
    h, _ = await _load_handle(st, hid, p.uid)
    if h["mode"] == "r":
        raise DFShaError(400, "INVALID_REQUEST", "handle de solo lectura")
    if not indices or len(indices) > 256 or any(i < 0 for i in indices):
        raise DFShaError(400, "INVALID_REQUEST", "indices inválidos (1 a 256, no negativos)")
    cfg = st.cfg
    healthy = [n for n in await nodesmod.list_nodes(st) if n.state == nodesmod.HEALTHY and n.node_id not in set(exclude)]
    if len(healthy) < cfg.replication:   # decisión CP: mejor rechazar que confirmar con una sola copia
        raise DFShaError(503, "NODE_UNAVAILABLE", f"hay menos de {cfg.replication} nodos de datos sanos", retryable=True)
    by_id = {n.node_id: n for n in healthy}
    new_version = h["base_version"] + 1

    out, ops = [], []
    for idx in indices:
        chosen = placement.choose(list(by_id), h["file_id"], idx, cfg.replication)
        block_id = _id("b-")
        primary = by_id[chosen[0]]
        cap = capabilities.sign(
            cfg.cap_key,
            {
                "op": "put", "uid": p.uid, "block_id": block_id, "node_id": primary.node_id,
                "version": new_version, "handle_id": hid, "idx": idx,
                "replicas": [{"node_id": r, "internal_endpoint": by_id[r].internal_endpoint} for r in chosen[1:]],
            },
            cfg.cap_ttl_s,
        )
        ops.append(o_put(f"/h/{hid}/alloc/{idx}", dumps({"block_id": block_id, "nodes": chosen}), h["lease"]))
        out.append({
            "index": idx, "block_id": block_id,
            "primary": {"node_id": primary.node_id, "endpoint": primary.endpoint},
            "replicas": [r for r in chosen[1:]], "capability": cap,
        })
    for i in range(0, len(ops), BATCH):
        res = await st.etcd.txn([c_exists(hkey(hid))], ops[i : i + BATCH])
        if not res.succeeded:
            raise DFShaError(410, "HANDLE_EXPIRED", "el handle venció")
    return out


async def confirm_block(st: State, block_id: str, body: dict) -> None:
    """El nodo primario avisa: "las copias de este bloque están validadas" (S2S, mTLS).

    Se comprueba que coincide con lo que `allocate` asignó; recién entonces el bloque cuenta como
    confirmado (/pend) y queda registrada su ubicación (/blk). El commit solo acepta bloques confirmados.
    """
    hid, idx = body["handle_id"], int(body["idx"])
    akv = await st.etcd.get(f"/h/{hid}/alloc/{idx}")
    if akv is None:
        raise DFShaError(410, "HANDLE_EXPIRED", "asignación inexistente o vencida")
    alloc = akv.json
    if alloc["block_id"] != block_id or sorted(alloc["nodes"]) != sorted(body["nodes"]):
        raise DFShaError(403, "FORBIDDEN", "la confirmación no coincide con la asignación")
    now = time.time()
    rec = {"block_id": block_id, "nodes": body["nodes"], "sha256": body["sha256"], "length": int(body["length"]), "ts": now}
    res = await st.etcd.txn(
        [c_mod(akv.key, akv.mod_rev), c_exists(hkey(hid))],
        [o_put(f"/h/{hid}/pend/{idx}", dumps(rec), akv.lease), o_put(f"/blk/{block_id}", dumps(rec))],
    )
    if not res.succeeded:
        raise DFShaError(409, "VERSION_CONFLICT", "la asignación cambió", retryable=True)


async def manifest(st: State, p: Principal, hid: str, frm: int, limit: int) -> dict:
    """Manifiesto paginado de la versión fijada por el handle.

    Para cada bloque devuelve los nodos que lo tienen, su endpoint ACTUAL (leído del registro
    dinámico de nodos, RNF7) y una capacidad de lectura firmada para cada uno.
    """
    h, _ = await _load_handle(st, hid, p.uid)
    limit = max(1, min(limit, 1000))
    base = {"version": h["base_version"], "block_size": st.cfg.block_size}
    if h["replace"] or h["base_version"] == 0:
        return {**base, "size": 0, "sha256_total": None, "blocks": [], "next": None}

    fid, ver = h["file_id"], h["base_version"]
    prefix = ver_prefix(fid, ver) + "b/"
    kvs, more = await st.etcd.range(prefix, start=bkey(fid, ver, max(frm, 0)), limit=limit)
    meta = await st.etcd.get(f"/f/{fid}/v{ver}/meta")
    placements = await st.etcd.get_many([f"/blk/{kv.json['block_id']}" for kv in kvs])
    registry = {n.node_id: n for n in await nodesmod.list_nodes(st)}

    blocks = []
    for kv in kvs:
        e = kv.json
        loc = placements.get(f"/blk/{e['block_id']}")
        cands = []
        for nid in (loc.json["nodes"] if loc else []):
            n = registry.get(nid)
            if n is None or n.state == nodesmod.DEAD:
                continue
            cap = capabilities.sign(
                st.cfg.cap_key,
                {"op": "get", "uid": p.uid, "block_id": e["block_id"], "node_id": nid, "version": ver},
                st.cfg.cap_ttl_s,
            )
            cands.append({"node_id": nid, "endpoint": n.endpoint, "state": n.state, "capability": cap})
        random.shuffle(cands)                                  # reparte la carga de lectura entre réplicas
        cands.sort(key=lambda c: c["state"] != nodesmod.HEALTHY)  # sanos primero (sort estable)
        blocks.append({"index": int(kv.key.rsplit("/", 1)[1]), **e, "nodes": cands})

    nxt = (blocks[-1]["index"] + 1) if (more and blocks) else None
    return {**base, "size": meta.json["size"], "sha256_total": meta.json.get("sha256_total"), "blocks": blocks, "next": nxt}
