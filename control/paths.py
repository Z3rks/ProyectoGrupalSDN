"""Rutas lógicas del espacio de cada usuario y su mapeo a claves de etcd.

Clave de un elemento del namespace:   /ns/{uid}:{directorio_padre}|{nombre}

Ejemplo: el archivo /docs/informe.pdf del usuario "ana" es la clave
         /ns/ana:/docs|informe.pdf

¿Por qué ese formato? Porque `ls` de un directorio es UNA lectura por prefijo
(/ns/ana:/docs|) que devuelve solo los hijos directos, sin recorrer subárboles.
Por eso ni el uid ni los nombres pueden contener '|' (se valida aquí).
"""
from __future__ import annotations

import re

from common.errors import DFShaError

_BAD_CHARS = re.compile(r"[\x00-\x1f|]")  # caracteres de control y el separador '|'
MAX_SEGMENT = 255
MAX_PATH = 4096


def normalize(path: str) -> str:
    """Convierte una ruta absoluta en su forma canónica ('/a/b'). Resuelve '.' y '..'.

    Rechaza rutas relativas (el `cd` es estado del cliente, el servidor solo recibe absolutas),
    '..' que escape de la raíz y segmentos inválidos.
    """
    if not isinstance(path, str) or not path.startswith("/") or len(path) > MAX_PATH:
        raise DFShaError(400, "INVALID_REQUEST", "la ruta debe ser absoluta (empezar por '/')")
    parts: list[str] = []
    for seg in path.split("/"):
        if seg in ("", "."):
            continue
        if seg == "..":
            if not parts:
                raise DFShaError(400, "INVALID_REQUEST", "la ruta sale de la raíz")
            parts.pop()
            continue
        if len(seg) > MAX_SEGMENT or _BAD_CHARS.search(seg):
            raise DFShaError(400, "INVALID_REQUEST", "nombre de archivo o directorio inválido")
        parts.append(seg)
    return "/" + "/".join(parts)


def split(path: str) -> tuple[str, str]:
    """('/a/b/c') -> ('/a/b', 'c').  La raíz no tiene padre."""
    if path == "/":
        raise DFShaError(400, "INVALID_REQUEST", "operación no válida sobre la raíz")
    parent, _, name = path.rpartition("/")
    return (parent or "/"), name


def ns_key(uid: str, path: str) -> str:
    parent, name = split(path)
    return f"/ns/{uid}:{parent}|{name}"


def ns_children_prefix(uid: str, directory: str) -> str:
    """Prefijo que agrupa los hijos directos de `directory`."""
    return f"/ns/{uid}:{directory}|"
