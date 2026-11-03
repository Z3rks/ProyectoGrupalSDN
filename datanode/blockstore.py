"""Almacén de bloques en disco: escritura atómica + cifrado en reposo + verificación.

Formato de un archivo de bloque (`<data>/blocks/<xx>/<block_id>.blk`):

    +--------+----------+-------------+----------+-----------------------------+
    | "DFB1" | nonce 12 | sha256 (32) | largo(8) |  ciphertext + etiqueta GCM  |
    +--------+----------+-------------+----------+-----------------------------+
      cabecera EN CLARO (53 bytes)                   contenido cifrado con AES-256-GCM

  * La cabecera guarda el SHA-256 y el largo del contenido en claro: permite hacer inventarios
    y reconciliaciones SIN descifrar (barato) y comparar copias por contenido, no por fecha.
  * El cifrado es AES-256-GCM con la clave maestra del nodo y un nonce aleatorio por bloque.
    Como "datos asociados" (AAD) se autentican block_id + sha256 + largo: si alguien altera el
    cifrado O la cabecera, la etiqueta GCM falla al leer y la copia se declara corrupta.

Estados de un bloque (sección 5.4 del diseño):
  TEMP    archivo en `tmp/`, aún sin validar. Si el nodo muere ahora, se descarta al arrancar.
  STORED  archivo en `blocks/`. Solo se llega aquí tras validar largo y hash, hacer fsync y un
          rename atómico (os.replace). Un lector NUNCA ve un bloque a medias.
"""
from __future__ import annotations

import hashlib
import logging
import os
import secrets
import shutil
import struct
import threading
import time

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

log = logging.getLogger("dfsha.store")

MAGIC = b"DFB1"
_HEADER = struct.Struct(">4s12s32sQ")   # magic, nonce, sha256, largo
HEADER_SIZE = _HEADER.size


class BlockNotFound(Exception):
    pass


class BlockCorrupt(Exception):
    """La etiqueta GCM o el SHA-256 no coinciden: la copia local no es confiable."""


class IntegrityError(Exception):
    """El contenido recibido no coincide con el hash/largo declarados."""


class BlockStore:
    def __init__(self, root: str, master_key: bytes) -> None:
        self.root = root
        self._aes = AESGCM(master_key)
        self.blocks_dir = os.path.join(root, "blocks")
        self.tmp_dir = os.path.join(root, "tmp")
        self.corrupt_dir = os.path.join(root, "corrupt")
        for d in (self.blocks_dir, self.tmp_dir, self.corrupt_dir):
            os.makedirs(d, exist_ok=True)
        self.pending_corrupt: set[str] = set()   # se reportan al control en el próximo latido
        self._lock = threading.Lock()
        self._discard_tmp()

    # ------------------------------------------------------------------ utilidades
    def _discard_tmp(self) -> None:
        """Al arrancar se borran los TEMP: son restos de escrituras interrumpidas (escenario 8.3)."""
        removed = 0
        for name in os.listdir(self.tmp_dir):
            os.unlink(os.path.join(self.tmp_dir, name))
            removed += 1
        if removed:
            log.warning("descartados %d bloques TEMP de escrituras interrumpidas", removed)

    def _path(self, block_id: str) -> str:
        if not block_id.replace("-", "").isalnum():          # evita rutas tipo ../../etc
            raise BlockNotFound(block_id)
        shard = block_id[2:4] or "00"
        return os.path.join(self.blocks_dir, shard, block_id + ".blk")

    @staticmethod
    def _aad(block_id: str, sha: bytes, length: int) -> bytes:
        return block_id.encode() + sha + struct.pack(">Q", length)

    # ------------------------------------------------------------------ escritura
    def put(self, block_id: str, data: bytes, sha256_hex: str) -> bool:
        """Guarda un bloque. Devuelve True si se creó, False si ya existía idéntico (idempotente).

        Falla con IntegrityError si el contenido no coincide con el SHA-256 declarado.
        """
        digest = hashlib.sha256(data).digest()
        if digest.hex() != sha256_hex:
            raise IntegrityError("el SHA-256 del contenido no coincide con el declarado")
        final = self._path(block_id)
        if os.path.exists(final):
            existing = self.header(block_id)
            if existing and existing["sha256"] == sha256_hex:
                return False   # reintento del mismo PUT: ya estaba
        os.makedirs(os.path.dirname(final), exist_ok=True)

        nonce = secrets.token_bytes(12)
        ciphertext = self._aes.encrypt(nonce, data, self._aad(block_id, digest, len(data)))
        tmp = os.path.join(self.tmp_dir, f"{block_id}.{secrets.token_hex(4)}.tmp")   # estado TEMP
        with open(tmp, "wb") as fh:
            fh.write(_HEADER.pack(MAGIC, nonce, digest, len(data)))
            fh.write(ciphertext)
            fh.flush()
            os.fsync(fh.fileno())              # los datos están en disco antes de publicarlos
        os.replace(tmp, final)                 # rename atómico: TEMP -> STORED
        # En POSIX, sincronizar el directorio persiste el rename. Windows no
        # permite abrir directorios con os.open(..., O_RDONLY); el archivo ya
        # fue sincronizado antes del os.replace.
        if os.name != "nt":
            dir_fd = os.open(os.path.dirname(final), os.O_RDONLY)
            try:
                os.fsync(dir_fd)               # y también la entrada de directorio
            finally:
                os.close(dir_fd)
        return True

    # ------------------------------------------------------------------ lectura
    def header(self, block_id: str) -> dict | None:
        """Lee solo la cabecera (sin descifrar). None si el bloque no existe."""
        path = self._path(block_id)
        try:
            with open(path, "rb") as fh:
                raw = fh.read(HEADER_SIZE)
            magic, _nonce, sha, length = _HEADER.unpack(raw)
        except (FileNotFoundError, struct.error):
            return None
        if magic != MAGIC:
            return None
        return {"sha256": sha.hex(), "length": length, "mtime": os.path.getmtime(path)}

    def get(self, block_id: str) -> tuple[bytes, str]:
        """Descifra y verifica. Devuelve (contenido, sha256). Lanza BlockNotFound o BlockCorrupt."""
        path = self._path(block_id)
        try:
            with open(path, "rb") as fh:
                raw = fh.read()
        except FileNotFoundError:
            raise BlockNotFound(block_id)
        try:
            magic, nonce, sha, length = _HEADER.unpack(raw[:HEADER_SIZE])
            if magic != MAGIC:
                raise ValueError("magic")
            data = self._aes.decrypt(nonce, raw[HEADER_SIZE:], self._aad(block_id, sha, length))
            if len(data) != length or hashlib.sha256(data).digest() != sha:
                raise ValueError("hash")
        except (InvalidTag, ValueError, struct.error):
            self.quarantine(block_id)
            raise BlockCorrupt(block_id)
        return data, sha.hex()

    # ------------------------------------------------------------------ mantenimiento
    def quarantine(self, block_id: str) -> None:
        """Aparta una copia corrupta (no se vuelve a servir) y la anuncia al control en el heartbeat."""
        with self._lock:
            src = self._path(block_id)
            if os.path.exists(src):
                shutil.move(src, os.path.join(self.corrupt_dir, block_id + ".blk"))
            self.pending_corrupt.add(block_id)
        log.error("bloque %s CORRUPTO: copia apartada; se reparará desde la otra réplica", block_id)

    def delete(self, block_id: str) -> bool:
        try:
            os.unlink(self._path(block_id))
            return True
        except (FileNotFoundError, BlockNotFound):
            return False

    def inventory(self) -> list[dict]:
        """Lista todos los bloques STORED con su hash, largo y fecha (solo cabeceras)."""
        out = []
        for shard in os.listdir(self.blocks_dir):
            sdir = os.path.join(self.blocks_dir, shard)
            for name in os.listdir(sdir):
                if not name.endswith(".blk"):
                    continue
                block_id = name[:-4]
                h = self.header(block_id)
                if h:
                    out.append({"block_id": block_id, **h})
        return out

    def scrub(self) -> list[str]:
        """Verificación periódica: descifra cada bloque y comprueba etiqueta GCM + hash."""
        bad = []
        for b in self.inventory():
            try:
                self.get(b["block_id"])
            except BlockCorrupt:
                bad.append(b["block_id"])
            except BlockNotFound:
                pass
        return bad

    def usage(self) -> dict:
        total, used, free = shutil.disk_usage(self.root)
        count = sum(len(files) for _, _, files in os.walk(self.blocks_dir))
        return {"free_bytes": free, "used_bytes": used, "block_count": count}

    def free_bytes(self) -> int:
        return shutil.disk_usage(self.root).free
