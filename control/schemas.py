"""Cuerpos de petición (validados por pydantic). Un campo inválido -> 400 INVALID_REQUEST."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class LoginReq(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=256)


class RefreshReq(BaseModel):
    refresh_token: str = Field(min_length=10, max_length=200)


class MkdirReq(BaseModel):
    path: str


class OpenReq(BaseModel):
    path: str
    mode: Literal["r", "w", "a"] = "r"
    create: bool = False
    truncate: bool = False          # w + truncate = reemplazar el archivo completo (lo usa `put`)
    expected_version: int | None = None
    lock_id: str | None = None      # reutilizar un lock() explícito previo


class AllocateReq(BaseModel):
    indices: list[int]
    exclude: list[str] = []         # nodos que el cliente vio fallar


class CloseReq(BaseModel):
    commit: bool = False
    size: int | None = None
    sha256_total: str | None = None


class LockReq(BaseModel):
    path: str


class NewUserReq(BaseModel):
    username: str
    password: str
    role: Literal["user", "admin"] = "user"
