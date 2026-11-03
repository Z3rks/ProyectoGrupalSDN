"""FileHandle: primitivas open/read/write/append/close de RF3 con la semántica DECLARADA.

Semántica (sección 5.1 del diseño):
  * read(off, n)   lee SIEMPRE de la versión fijada al abrir (snapshot), aunque otro cliente
                   publique una versión nueva. Solo descarga los bloques que cubren el rango.
  * write(off, b)  modifica un rango (off <= tamaño actual: SIN huecos) y puede extender el archivo.
                   Granularidad lógica de byte, física de bloque: los bloques tocados se reescriben
                   como bloques NUEVOS (lectura-modificación-escritura) y el resto se reutiliza.
                   Nada es visible para otros clientes hasta close().
  * append(b)      escribe al final (modo "a").
  * close()        publica la nueva versión de forma atómica (o falla sin publicar nada).
  * Un handle de escritura mantiene el lock exclusivo del archivo y lo renueva solo (hilo).
  * Las escrituras se acumulan en memoria hasta close(); para archivos grandes use put().
"""
from __future__ import annotations

import math
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from .errors import DFShaClientError

if TYPE_CHECKING:
    from .api import DFShaClient


class FileHandle:
    def __init__(self, client: "DFShaClient", info: dict, truncate: bool = False) -> None:
        self.c = client
        self.id = info["handle_id"]
        self.path = info["path"]
        self.mode = info["mode"]
        self.version = info["base_version"]
        self.block_size = info["block_size"]
        self.sha256_total = info.get("sha256_total")
        self.lock = info.get("lock")
        self._base_size = info["size"]          # tamaño de la versión fijada (0 si se abrió con truncate)
        self.size = info["size"]                # tamaño lógico actual (crece con write/append)
        self._creating_or_replacing = truncate or info["base_version"] == 0
        self._entries: dict[int, dict] = {}     # caché del manifiesto: índice -> {block_id, nodes, ...}
        self._dirty: dict[int, bytearray] = {}  # bloques modificados (aún no subidos)
        self._closed = False
        self._stop = threading.Event()
        # El lease del handle (60 s) y el del lock (30 s) se renuevan solos mientras esté abierto.
        interval = max(info.get("handle_ttl", 60) / 3, 1)
        self._renewer = threading.Thread(target=self._renew_loop, args=(interval,), daemon=True)
        self._renewer.start()

    # ------------------------------------------------------------------ mantenimiento del lease
    def _renew_loop(self, interval: float) -> None:
        while not self._stop.wait(interval):
            try:
                self.c._control("POST", f"/v1/handles/{self.id}/renew", retries=2)
            except DFShaClientError:
                pass   # si el lease se perdió, el close lo dirá (LOCK_LOST / HANDLE_EXPIRED)

    # ------------------------------------------------------------------ lectura
    def _nblocks(self, size: int) -> int:
        return math.ceil(size / self.block_size)

    def _load_entries(self, first: int, last: int) -> None:
        missing = [i for i in range(first, last + 1) if i not in self._entries]
        if not missing:
            return
        page = self.c._manifest(self.id, missing[0], min(max(missing[-1] - missing[0] + 1, 1), 1000))
        for b in page["blocks"]:
            self._entries[b["index"]] = b

    def _base_block(self, idx: int) -> bytes:
        self._load_entries(idx, idx)
        try:
            return self.c._download_block(self._entries[idx])
        except DFShaClientError:
            self._entries.pop(idx, None)        # las capacidades pudieron vencer: renovar y reintentar
            self._load_entries(idx, idx)
            return self.c._download_block(self._entries[idx])

    def read(self, offset: int, length: int) -> bytes:
        """Lee hasta `length` bytes desde `offset` de la versión fijada al abrir."""
        if offset < 0 or length < 0:
            raise ValueError("offset y length deben ser >= 0")
        end = min(offset + length, self._base_size)
        if offset >= end:
            return b""
        bs = self.block_size
        first, last = offset // bs, (end - 1) // bs
        self._load_entries(first, last)
        with ThreadPoolExecutor(self.c.parallel) as pool:
            blocks = list(pool.map(self._base_block, range(first, last + 1)))
        data = b"".join(blocks)
        start = offset - first * bs
        return data[start : start + (end - offset)]

    # ------------------------------------------------------------------ escritura
    def write(self, offset: int, data: bytes) -> int:
        """Escribe `data` en `offset` (<= tamaño actual). Devuelve los bytes escritos."""
        if self.mode == "r":
            raise DFShaClientError(400, "INVALID_REQUEST", "handle abierto en modo lectura")
        if self.mode == "a":
            offset = self.size
        if offset < 0 or offset > self.size:
            raise ValueError("no se admiten escrituras con huecos (offset > tamaño actual)")
        if not data:
            return 0
        bs, end = self.block_size, offset + len(data)
        for idx in range(offset // bs, (end - 1) // bs + 1):
            block_start = idx * bs
            buf = self._dirty.get(idx)
            if buf is None:   # lectura-modificación-escritura: partir del bloque de la versión base
                buf = bytearray(self._base_block(idx)) if block_start < self._base_size else bytearray()
            lo, hi = max(offset, block_start), min(end, block_start + bs)
            if len(buf) < hi - block_start:
                buf.extend(b"\x00" * (hi - block_start - len(buf)))
            buf[lo - block_start : hi - block_start] = data[lo - offset : hi - offset]
            self._dirty[idx] = buf
        self.size = max(self.size, end)
        return len(data)

    def append(self, data: bytes) -> int:
        """Escribe al final del archivo."""
        return self.write(self.size, data)

    # ------------------------------------------------------------------ cierre
    def _commit(self, size: int, sha256_total: str | None) -> dict:
        self._stop.set()
        self._closed = True
        return self.c._control("POST", f"/v1/handles/{self.id}/close",
                               json_body={"commit": True, "size": size, "sha256_total": sha256_total})

    def close(self) -> dict | None:
        """close(): en lectura libera el snapshot; en escritura sube los bloques modificados y publica."""
        if self._closed:
            return None
        if self.mode == "r" or not (self._dirty or self._creating_or_replacing):
            self._stop.set()
            self._closed = True
            return self.c._control("POST", f"/v1/handles/{self.id}/close", json_body={"commit": False})
        try:
            idxs = sorted(self._dirty)
            with ThreadPoolExecutor(self.c.parallel) as pool:
                for start in range(0, len(idxs), 64):
                    chunk = idxs[start : start + 64]
                    allocs = self.c._allocate(self.id, chunk)
                    futs = [pool.submit(self.c._upload_block, self.id, a["index"], bytes(self._dirty[a["index"]]), a) for a in allocs]
                    for f in futs:
                        f.result()
            return self._commit(self.size, None)
        except BaseException:
            self.abort()
            raise

    def abort(self) -> None:
        """Descarta los cambios: cierra sin publicar y libera el lock."""
        self._stop.set()
        self._closed = True
        try:
            self.c._control("POST", f"/v1/handles/{self.id}/close", json_body={"commit": False}, retries=2)
        except DFShaClientError:
            pass

    def __enter__(self) -> "FileHandle":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.close()
        else:
            self.abort()
