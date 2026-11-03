"""Fixtures compartidas de las pruebas.

`cluster` levanta UNA VEZ por sesión un clúster real (3 etcd + 3 control + 3 nodos de datos,
con TLS 1.3 y mTLS). Las pruebas que matan procesos deben dejarlo como lo encontraron
(el fixture `restore` lo comprueba al final de cada prueba de fallos).

Las pruebas unitarias (tests/unit) NO necesitan el clúster ni etcd.
"""
from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.local_cluster import NODE_IDS, LocalCluster  # noqa: E402

BLOCK = 256 * 1024   # bloques pequeños en las pruebas: archivos de ~1 MB ya tienen varios bloques


@pytest.fixture(scope="session")
def cluster():
    cl = LocalCluster(str(ROOT / "run" / "tests"), tls=True, n_nodes=3, block_size=BLOCK, base_port=15000)
    cl.start_all()
    yield cl
    cl.stop_all()


@pytest.fixture
def restore(cluster):
    """Tras una prueba que mata/pausa procesos, deja el clúster completo y sano."""
    yield cluster
    import signal
    for name, proc in list(cluster.procs.items()):
        if proc.poll() is None and name in NODE_IDS:
            proc.send_signal(signal.SIGCONT)
    for name in cluster.cmds:
        if not cluster.is_running(name):
            cluster.start(name)
    cluster.wait_all_up(timeout=90)        # cada proceso reiniciado debe estar atendiendo de verdad...
    cluster.wait_nodes(3, timeout=60)      # ...y el control debe verlo sano
    cluster.wait_for(lambda: cluster.leader(), timeout=40, what="un líder de control tras restaurar")


@pytest.fixture
def alice(cluster):
    return cluster.make_user("alice")


@pytest.fixture
def bob(cluster):
    return cluster.make_user("bob")


@pytest.fixture
def tmpfile(tmp_path):
    """Crea un archivo local aleatorio de N bytes y devuelve (ruta, sha256)."""
    def make(size: int, name: str = "in.bin"):
        data = os.urandom(size)
        path = tmp_path / name
        path.write_bytes(data)
        return str(path), hashlib.sha256(data).hexdigest()
    return make


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------- utilidades para inspeccionar el clúster
def uniq(prefix: str = "t") -> str:
    """Nombre único: el clúster es compartido entre pruebas y los usuarios persisten."""
    import uuid
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def blocks_on_disk(cluster) -> dict[str, set[str]]:
    """block_id guardados físicamente en cada nodo de datos (mira los archivos .blk)."""
    out: dict[str, set[str]] = {}
    for nid in NODE_IDS[: cluster.n_nodes]:
        root = cluster.dir / "data" / nid / "blocks"
        out[nid] = {p.stem for p in root.rglob("*.blk")} if root.exists() else set()
    return out


def manifest_of(client, path: str) -> list[dict]:
    """Manifiesto (lista de bloques con sus nodos) de la versión vigente de `path`."""
    h = client.open(path, "r")
    try:
        return client._manifest(h.id, 0, 1000)["blocks"]
    finally:
        h.close()
