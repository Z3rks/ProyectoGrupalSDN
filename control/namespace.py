"""Sistema de archivos jerárquico por usuario (RF1: ls, mkdir, rmdir, rm).

El árbol de cada usuario vive en etcd bajo /ns/{uid}:{padre}|{nombre} (ver paths.py).

  directorio : {"type": "dir"}                       (valor CONSTANTE, ver nota)
  archivo    : {"type": "file", "file_id", "version", "size", "mtime", "sha256"}

Aislamiento (RF1): todas las funciones reciben `uid` del token ya validado y construyen las
claves con ese uid; es imposible nombrar el espacio de otro usuario.

NOTA sobre carreras entre `rmdir` y la creación de hijos:
  Al crear un hijo se hace un "touch" del padre (se reescribe su valor constante, lo que sube su
  mod_revision) y se exige que el padre exista. `rmdir` comprueba que está vacío y borra con la
  condición "el padre no cambió desde que lo leí". Si alguien creó un hijo entremedias, el
  mod_revision cambió, el borrado falla y se reintenta viendo el hijo -> nunca quedan huérfanos.
"""
from __future__ import annotations

import time

from common.errors import DFShaError

from . import paths
from .etcd import KV, c_absent, c_exists, c_mod, dumps, o_del, o_put
from .state import State

DIR_VALUE = dumps({"type": "dir"})


async def get_entry(st: State, uid: str, path: str) -> KV | None:
    """Lee la entrada del namespace de `path` (None si no existe). La raíz no tiene entrada."""
    return await st.etcd.get(paths.ns_key(uid, path))


async def require_parent_dir(st: State, uid: str, path: str) -> None:
    """Falla si el directorio padre de `path` no existe o no es un directorio."""
    parent, _ = paths.split(path)
    if parent == "/":
        return
    kv = await get_entry(st, uid, parent)
    if kv is None:
        raise DFShaError(404, "NOT_FOUND", "el directorio padre no existe")
    if kv.json["type"] != "dir":
        raise DFShaError(409, "NOT_A_DIRECTORY", "el padre no es un directorio")


def parent_guard(uid: str, path: str) -> tuple[list[dict], list[dict]]:
    """(condiciones, operaciones) para exigir y 'tocar' el padre dentro de una transacción."""
    parent, _ = paths.split(path)
    if parent == "/":
        return [], []
    pkey = paths.ns_key(uid, parent)
    return [c_exists(pkey)], [o_put(pkey, DIR_VALUE)]


# ---------------------------------------------------------------------------- operaciones
def _entry_view(name: str, d: dict) -> dict:
    return {
        "name": name,
        "type": d["type"],
        "size": d.get("size", 0),
        "version": d.get("version", 0),
        "mtime": d.get("mtime", 0),
    }


async def stat(st: State, uid: str, path: str) -> dict:
    path = paths.normalize(path)
    if path == "/":
        return {"path": "/", "type": "dir", "size": 0, "version": 0, "mtime": 0}
    kv = await get_entry(st, uid, path)
    if kv is None:
        raise DFShaError(404, "NOT_FOUND", "no existe")
    d = kv.json
    return {"path": path, **_entry_view(paths.split(path)[1], d), "sha256": d.get("sha256")}


async def ls(st: State, uid: str, path: str) -> list[dict]:
    """Hijos directos de un directorio: UNA lectura por prefijo."""
    path = paths.normalize(path)
    if path != "/":
        kv = await get_entry(st, uid, path)
        if kv is None:
            raise DFShaError(404, "NOT_FOUND", "el directorio no existe")
        if kv.json["type"] != "dir":
            raise DFShaError(409, "NOT_A_DIRECTORY", "no es un directorio")
    prefix = paths.ns_children_prefix(uid, path)
    kvs = await st.etcd.range_all(prefix)
    return [_entry_view(kv.key[len(prefix):], kv.json) for kv in kvs]


async def mkdir(st: State, uid: str, path: str) -> bool:
    """Crea un directorio de forma atómica. Idempotente: si ya existe devuelve False."""
    path = paths.normalize(path)
    key = paths.ns_key(uid, path)  # lanza 400 si path == "/"
    for _ in range(5):
        existing = await st.etcd.get(key)
        if existing is not None:
            if existing.json["type"] == "dir":
                return False
            raise DFShaError(409, "EXISTS", "ya existe un archivo con ese nombre")
        await require_parent_dir(st, uid, path)
        cmp, ops = parent_guard(uid, path)
        res = await st.etcd.txn([c_absent(key), *cmp], [o_put(key, DIR_VALUE), *ops])
        if res.succeeded:
            return True
    raise DFShaError(409, "VERSION_CONFLICT", "conflicto al crear el directorio", retryable=True)


async def rmdir(st: State, uid: str, path: str) -> None:
    """Elimina un directorio SOLO si está vacío (409 NOT_EMPTY si no)."""
    path = paths.normalize(path)
    key = paths.ns_key(uid, path)
    for _ in range(5):
        kv = await st.etcd.get(key)
        if kv is None:
            raise DFShaError(404, "NOT_FOUND", "el directorio no existe")
        if kv.json["type"] != "dir":
            raise DFShaError(409, "NOT_A_DIRECTORY", "no es un directorio")
        children, _more = await st.etcd.range(paths.ns_children_prefix(uid, path), limit=1)
        if children:
            raise DFShaError(409, "NOT_EMPTY", "el directorio no está vacío")
        res = await st.etcd.txn([c_mod(key, kv.mod_rev)], [o_del(key)])
        if res.succeeded:
            return
    raise DFShaError(409, "VERSION_CONFLICT", "conflicto al borrar el directorio", retryable=True)


async def rm(st: State, uid: str, path: str) -> bool:
    """Retira un archivo del namespace al instante; los bloques se liberan después (GC).

    Es idempotente (devuelve False si ya no existía). Se rechaza si hay un lock vigente.
    Una escritura en curso sobre este archivo fallará en su commit (la entrada ya no existe).
    """
    path = paths.normalize(path)
    key = paths.ns_key(uid, path)
    for _ in range(5):
        kv = await st.etcd.get(key)
        if kv is None:
            return False
        d = kv.json
        if d["type"] == "dir":
            raise DFShaError(409, "IS_A_DIRECTORY", "use rmdir para directorios")
        lock_key = f"/lock/{uid}:{path}"
        res = await st.etcd.txn(
            [c_mod(key, kv.mod_rev), c_absent(lock_key)],
            [o_del(key), o_put(f"/deleted/{d['file_id']}", dumps({"ts": time.time(), "uid": uid}))],
        )
        if res.succeeded:
            return True
        if await st.etcd.get(lock_key) is not None:
            raise DFShaError(409, "LOCKED", "el archivo tiene un bloqueo vigente", retryable=True, retry_after_s=5)
    raise DFShaError(409, "VERSION_CONFLICT", "conflicto al borrar el archivo", retryable=True)
