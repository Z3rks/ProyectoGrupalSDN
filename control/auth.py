"""Autenticación y cuentas (RNF6, sección 7 del diseño).

  * Contraseñas con argon2id (lento a propósito; nunca se guardan en claro).
  * Login -> JWT de acceso (15 min, firmado HS256) + token de refresco (8 h) opaco y aleatorio.
    El de refresco se guarda en etcd SOLO como hash SHA-256 y se rota en cada uso.
  * Límite de 5 intentos de login por minuto (por usuario+IP).
  * Respuestas de login idénticas para "usuario inexistente" y "contraseña incorrecta".
"""
from __future__ import annotations

import asyncio
import hashlib
import re
import secrets
import time
from dataclasses import dataclass

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError
from fastapi import Header, Request

from common.errors import DFShaError

from .etcd import c_absent, c_mod, dumps, o_del, o_put
from .state import State, get_state

UID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,31}$")
_ph = PasswordHasher()  # argon2id con parámetros por defecto de la librería
_DUMMY_HASH = _ph.hash("contraseña-ficticia")  # se verifica cuando el usuario no existe (igualar tiempos)


@dataclass
class Principal:
    """Identidad ya autenticada que llega a cada endpoint."""

    uid: str
    role: str  # "user" | "admin"


def _bad_credentials() -> DFShaError:
    return DFShaError(401, "UNAUTHENTICATED", "credenciales inválidas")


# ---------------------------------------------------------------------------- cuentas
async def create_user(st: State, uid: str, password: str, role: str = "user") -> None:
    if not UID_RE.match(uid):
        raise DFShaError(400, "INVALID_REQUEST", "usuario inválido (a-z, 0-9, _ y -, 2 a 32 caracteres)")
    if len(password) < 8:
        raise DFShaError(400, "INVALID_REQUEST", "la contraseña debe tener al menos 8 caracteres")
    if role not in ("user", "admin"):
        raise DFShaError(400, "INVALID_REQUEST", "rol inválido")
    hashed = await asyncio.to_thread(_ph.hash, password)  # argon2 es CPU-intensivo: fuera del event loop
    rec = dumps({"hash": hashed, "role": role, "active": True, "created": time.time()})
    res = await st.etcd.txn([c_absent(f"/u/{uid}")], [o_put(f"/u/{uid}", rec)])
    if not res.succeeded:
        raise DFShaError(409, "EXISTS", "el usuario ya existe")


async def bootstrap_admin(st: State) -> None:
    """Crea el administrador inicial (DFSHA_ADMIN_USER/PASSWORD) si aún no existe."""
    try:
        await create_user(st, st.cfg.admin_user, st.cfg.admin_password, "admin")
    except DFShaError as exc:
        if exc.code != "EXISTS":
            raise


async def delete_user(st: State, uid: str) -> None:
    """Elimina la cuenta y marca todos sus archivos como borrados (el GC libera los bloques)."""
    kv = await st.etcd.get(f"/u/{uid}")
    if kv is None:
        raise DFShaError(404, "NOT_FOUND", "usuario inexistente")
    entries = await st.etcd.range_all(f"/ns/{uid}:")
    now = time.time()
    ops = []
    for e in entries:
        meta = e.json
        if meta.get("type") == "file":
            ops.append(o_put(f"/deleted/{meta['file_id']}", dumps({"ts": now, "uid": uid})))
    ops.append(o_del(f"/ns/{uid}:", prefix=True))
    ops.append(o_del(f"/u/{uid}"))
    for i in range(0, len(ops), st.etcd.MAX_OPS):
        await st.etcd.txn([], ops[i : i + st.etcd.MAX_OPS])
    st.user_cache.pop(uid, None)


# ---------------------------------------------------------------------------- login / tokens
def _issue_access(st: State, uid: str, role: str) -> str:
    now = int(time.time())
    claims = {"sub": uid, "role": role, "iat": now, "exp": now + st.cfg.access_ttl_s, "jti": secrets.token_hex(8)}
    return jwt.encode(claims, st.cfg.jwt_secret, algorithm="HS256")


async def _issue_tokens(st: State, uid: str, role: str) -> dict:
    refresh = secrets.token_urlsafe(32)
    key = "/rt/" + hashlib.sha256(refresh.encode()).hexdigest()  # solo guardamos el hash
    await st.etcd.put(key, dumps({"uid": uid, "exp": time.time() + st.cfg.refresh_ttl_s}))
    return {
        "access_token": _issue_access(st, uid, role),
        "refresh_token": refresh,
        "token_type": "Bearer",
        "expires_in": st.cfg.access_ttl_s,
    }


async def login(st: State, uid: str, password: str, ip: str) -> dict:
    st.limiter.hit(f"{uid}|{ip}")
    kv = await st.etcd.get(f"/u/{uid}") if UID_RE.match(uid) else None
    rec = kv.json if kv else None
    stored = rec["hash"] if rec else _DUMMY_HASH
    try:
        await asyncio.to_thread(_ph.verify, stored, password)
        ok = rec is not None and rec.get("active", False)
    except (VerifyMismatchError, InvalidHashError):
        ok = False
    if not ok:
        raise _bad_credentials()
    return await _issue_tokens(st, uid, rec["role"])


async def refresh(st: State, token: str) -> dict:
    """Canjea un refresh token por un par nuevo. El viejo queda inválido (rotación)."""
    key = "/rt/" + hashlib.sha256(token.encode()).hexdigest()
    kv = await st.etcd.get(key)
    if kv is None or kv.json["exp"] < time.time():
        raise _bad_credentials()
    res = await st.etcd.txn([c_mod(key, kv.mod_rev)], [o_del(key)])  # uso único: gana solo una petición
    if not res.succeeded:
        raise _bad_credentials()
    uid = kv.json["uid"]
    ukv = await st.etcd.get(f"/u/{uid}")
    if ukv is None or not ukv.json.get("active", False):
        raise _bad_credentials()
    return await _issue_tokens(st, uid, ukv.json["role"])


# ---------------------------------------------------------------------------- dependencias FastAPI
async def _user_active(st: State, uid: str) -> bool:
    """¿La cuenta sigue existiendo? Caché de 5 s para no leer etcd en cada petición."""
    hit = st.user_cache.get(uid)
    now = time.monotonic()
    if hit and hit[0] > now:
        return hit[1]
    kv = await st.etcd.get(f"/u/{uid}")
    active = bool(kv and kv.json.get("active", False))
    st.user_cache[uid] = (now + 5.0, active)
    return active


async def current_principal(request: Request, authorization: str | None = Header(default=None)) -> Principal:
    """Valida el `Authorization: Bearer <jwt>` y devuelve quién es el usuario."""
    st = get_state(request)
    if not authorization or not authorization.lower().startswith("bearer "):
        raise DFShaError(401, "UNAUTHENTICATED", "falta el token de acceso")
    token = authorization[7:].strip()
    try:
        claims = jwt.decode(token, st.cfg.jwt_secret, algorithms=["HS256"], options={"require": ["exp", "sub"]})
    except jwt.ExpiredSignatureError:
        raise DFShaError(401, "UNAUTHENTICATED", "token vencido")
    except jwt.PyJWTError:
        raise DFShaError(401, "UNAUTHENTICATED", "token inválido")
    if not await _user_active(st, claims["sub"]):
        raise DFShaError(401, "UNAUTHENTICATED", "cuenta inexistente o deshabilitada")
    return Principal(uid=claims["sub"], role=claims.get("role", "user"))


async def require_admin(request: Request, authorization: str | None = Header(default=None)) -> Principal:
    p = await current_principal(request, authorization)
    if p.role != "admin":
        raise DFShaError(403, "FORBIDDEN", "solo el administrador")
    return p
