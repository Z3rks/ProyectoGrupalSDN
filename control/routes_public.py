"""API PÚBLICA del servicio de control (cliente -> control). Sección 4.1 del diseño.

Cada endpoint es delgado: valida la entrada con pydantic, obtiene la identidad con
`current_principal` y delega en los módulos de lógica (auth, namespace, files).
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Header, Query, Request

from common.errors import DFShaError

from . import auth, files, namespace
from .auth import Principal, current_principal, require_admin
from .schemas import (
    AllocateReq, CloseReq, LockReq, LoginReq, MkdirReq, NewUserReq, OpenReq, RefreshReq,
)
from .state import State, get_state

router = APIRouter(prefix="/v1")


@router.post("/auth/login")
async def login(body: LoginReq, request: Request, st: State = Depends(get_state)):
    ip = request.client.host if request.client else "?"
    return await auth.login(st, body.username, body.password, ip)


@router.post("/auth/refresh")
async def refresh(body: RefreshReq, st: State = Depends(get_state)):
    return await auth.refresh(st, body.refresh_token)


# ---------------------------------------------------------------- RF1: namespace
@router.get("/ls")
async def ls(path: str = Query("/"), p: Principal = Depends(current_principal), st: State = Depends(get_state)):
    return {"path": path, "entries": await namespace.ls(st, p.uid, path)}


@router.get("/stat")
async def stat(path: str = Query(...), p: Principal = Depends(current_principal), st: State = Depends(get_state)):
    return await namespace.stat(st, p.uid, path)


@router.post("/mkdir")
async def mkdir(body: MkdirReq, p: Principal = Depends(current_principal), st: State = Depends(get_state)):
    return {"created": await namespace.mkdir(st, p.uid, body.path)}


@router.delete("/dir")
async def rmdir(path: str = Query(...), p: Principal = Depends(current_principal), st: State = Depends(get_state)):
    await namespace.rmdir(st, p.uid, path)
    return {"removed": True}


@router.delete("/file")
async def rm(path: str = Query(...), p: Principal = Depends(current_principal), st: State = Depends(get_state)):
    return {"removed": await namespace.rm(st, p.uid, path)}


# ---------------------------------------------------------------- RF2/RF3: handles
@router.post("/files/open")
async def open_file(
    body: OpenReq,
    idempotency_key: str | None = Header(default=None),
    p: Principal = Depends(current_principal),
    st: State = Depends(get_state),
):
    return await files.open_file(
        st, p, body.path, body.mode, body.create, body.truncate, body.expected_version, body.lock_id, idempotency_key
    )


@router.get("/handles/{hid}/manifest")
async def manifest(
    hid: str,
    from_: int = Query(0, alias="from", ge=0),
    limit: int = Query(1000, ge=1, le=1000),
    p: Principal = Depends(current_principal),
    st: State = Depends(get_state),
):
    return await files.manifest(st, p, hid, from_, limit)


@router.post("/handles/{hid}/allocate")
async def allocate(hid: str, body: AllocateReq, p: Principal = Depends(current_principal), st: State = Depends(get_state)):
    return {"blocks": await files.allocate(st, p, hid, body.indices, body.exclude)}


@router.post("/handles/{hid}/renew")
async def renew(hid: str, p: Principal = Depends(current_principal), st: State = Depends(get_state)):
    return await files.renew_handle(st, p, hid)


@router.post("/handles/{hid}/close")
async def close(
    hid: str,
    body: CloseReq,
    idempotency_key: str | None = Header(default=None),
    p: Principal = Depends(current_principal),
    st: State = Depends(get_state),
):
    return await files.close_handle(st, p, hid, body.commit, body.size, body.sha256_total, idempotency_key)


# ---------------------------------------------------------------- lock() / unlock()
@router.post("/locks")
async def lock(body: LockReq, p: Principal = Depends(current_principal), st: State = Depends(get_state)):
    return await files.lock_file(st, p, body.path)


@router.post("/locks/{lock_id}/renew")
async def renew_lock(lock_id: str, p: Principal = Depends(current_principal), st: State = Depends(get_state)):
    return await files.renew_lock(st, p, lock_id)


@router.delete("/locks/{lock_id}")
async def unlock(lock_id: str, p: Principal = Depends(current_principal), st: State = Depends(get_state)):
    await files.unlock(st, p, lock_id)
    return {"unlocked": True}


# ---------------------------------------------------------------- administración
@router.post("/admin/users", status_code=201)
async def create_user(body: NewUserReq, _: Principal = Depends(require_admin), st: State = Depends(get_state)):
    await auth.create_user(st, body.username, body.password, body.role)
    return {"username": body.username, "role": body.role}


@router.delete("/admin/users/{uid}")
async def delete_user(uid: str, admin: Principal = Depends(require_admin), st: State = Depends(get_state)):
    if uid == admin.uid:
        raise DFShaError(400, "INVALID_REQUEST", "el administrador no puede eliminarse a sí mismo")
    await auth.delete_user(st, uid)
    return {"removed": uid}
