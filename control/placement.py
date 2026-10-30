"""Política de colocación de bloques (sección 3.2 del diseño).

Reglas:
  1. Solo se usan nodos HEALTHY.
  2. El primario de cada bloque se reparte cíclicamente (round-robin) entre los nodos.
  3. El punto de partida del ciclo depende del hash del file_id: así los archivos pequeños
     (1 bloque) no cargan siempre al mismo nodo.
  4. La réplica va SIEMPRE en un nodo distinto del primario (el dominio de fallo es el nodo/VM).

Es una función pura (sin E/S) para poder probarla de forma aislada.
"""
from __future__ import annotations

import hashlib


def choose(node_ids: list[str], file_id: str, index: int, replication: int = 2) -> list[str]:
    """Devuelve [primario, réplica, ...] para el bloque `index` del archivo `file_id`.

    `node_ids` debe traer solo nodos sanos. Con menos nodos que `replication` no hay
    colocación posible (el llamador responde 503 antes de llegar aquí).
    """
    nodes = sorted(node_ids)  # orden determinista, independiente de cómo llegue la lista
    n = len(nodes)
    if n < replication:
        raise ValueError("no hay suficientes nodos sanos")
    offset = int(hashlib.sha256(file_id.encode()).hexdigest()[:8], 16)
    first = (offset + index) % n
    return [nodes[(first + k) % n] for k in range(replication)]
