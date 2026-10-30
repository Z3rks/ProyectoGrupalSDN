"""Estado compartido de una instancia del servicio de control.

Se crea una sola vez al arrancar y se comparte entre el listener público y el interno
(`app.state.st`). OJO: "estado" aquí son solo conexiones y cachés locales; los DATOS
(usuarios, namespace, locks, versiones, nodos) viven siempre en etcd.
"""
from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass, field

import httpx
from fastapi import Request

from common.errors import DFShaError
from common.tls import client_ssl_context

from .etcd import EtcdClient
from .settings import Settings


class RateLimiter:
    """Ventana deslizante en memoria: máximo `limit` eventos por `window` segundos por clave.

    Es por instancia de control (no compartido). Suficiente para frenar fuerza bruta de
    contraseñas en el prototipo; se declara como limitación en el informe.
    """

    def __init__(self, limit: int, window: float = 60.0) -> None:
        self.limit, self.window = limit, window
        self._events: dict[str, deque[float]] = defaultdict(deque)

    def hit(self, key: str) -> None:
        now = time.monotonic()
        q = self._events[key]
        while q and now - q[0] > self.window:
            q.popleft()
        if len(q) >= self.limit:
            retry = int(self.window - (now - q[0])) + 1
            raise DFShaError(429, "RATE_LIMITED", "demasiados intentos", retryable=True, retry_after_s=retry)
        q.append(now)


@dataclass
class State:
    cfg: Settings
    etcd: EtcdClient
    etcd_http: httpx.AsyncClient     # conexión a etcd
    node_http: httpx.AsyncClient     # conexión mTLS hacia los nodos de datos (reparación, GC)
    limiter: RateLimiter
    leader: object | None = None     # LeaderService (se asigna al arrancar)
    user_cache: dict = field(default_factory=dict)   # uid -> (expira, activo)
    metrics: dict = field(default_factory=lambda: defaultdict(int))


def build_state(cfg: Settings) -> State:
    """Crea los clientes HTTP (con TLS 1.3 / mTLS si está habilitado) y el cliente de etcd."""
    node_ctx = client_ssl_context(cfg.tls, with_client_cert=True)
    node_http = httpx.AsyncClient(verify=node_ctx, timeout=httpx.Timeout(30.0, connect=3.0))
    # etcd usa su propio certificado de cliente si el despliegue lo exige (misma CA interna).
    etcd_http = httpx.AsyncClient(verify=node_ctx, timeout=httpx.Timeout(5.0, connect=2.0))
    return State(
        cfg=cfg,
        etcd=EtcdClient(cfg.etcd_endpoints, etcd_http),
        etcd_http=etcd_http,
        node_http=node_http,
        limiter=RateLimiter(cfg.login_limit),
    )


def get_state(request: Request) -> State:
    """Dependencia de FastAPI: devuelve el State de esta instancia."""
    return request.app.state.st
