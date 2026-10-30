"""Utilidades de hash. SHA-256 se calcula siempre sobre el contenido EN CLARO."""
from __future__ import annotations

import hashlib


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def new_hasher():
    """Hash incremental (para archivos grandes que se leen por bloques)."""
    return hashlib.sha256()
