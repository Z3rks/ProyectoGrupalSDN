"""Arranque de varios listeners HTTP(S) dentro de un mismo proceso.

Cada componente (control y nodo de datos) expone DOS puertos:

  * público   -> TLS 1.3 de servidor. Lo usa el cliente.
  * interno   -> mTLS (exige certificado de cliente). Lo usan los demás servidores (S2S).

Se implementa con dos instancias de uvicorn.Server que comparten el mismo proceso y el
mismo estado en memoria (cada una sirve una aplicación FastAPI distinta).
"""
from __future__ import annotations

import asyncio
import contextlib
import ssl
from dataclasses import dataclass

import uvicorn
from fastapi import FastAPI

from .config import TlsSettings


@dataclass
class Listener:
    app: FastAPI
    port: int
    mtls: bool = False  # True: exigir certificado de cliente
    host: str = "0.0.0.0"
    lifespan: bool = False  # solo la app principal ejecuta startup/shutdown


class _Secondary(uvicorn.Server):
    """Servidor secundario: no captura señales (las captura el principal)."""

    def capture_signals(self):  # type: ignore[override]
        return contextlib.nullcontext()


def _build(listener: Listener, tls: TlsSettings, primary: bool) -> uvicorn.Server:
    kwargs: dict = {}
    if tls.enabled:
        kwargs.update(ssl_certfile=tls.cert, ssl_keyfile=tls.key)
        if listener.mtls:
            kwargs.update(ssl_ca_certs=tls.ca, ssl_cert_reqs=ssl.CERT_REQUIRED)
    config = uvicorn.Config(
        listener.app,
        host=listener.host,
        port=listener.port,
        log_level="info",
        lifespan="on" if listener.lifespan else "off",
        # Otros servidores mantienen conexiones keep-alive abiertas; sin este tope, el apagado
        # ordenado esperaría indefinidamente a que las cierren.
        timeout_graceful_shutdown=3,
        timeout_keep_alive=30,   # conexiones keep-alive más duraderas que las del cliente (evita cortes espurios)
        **kwargs,
    )
    config.load()  # construye el SSLContext; así podemos endurecerlo antes de arrancar
    if config.ssl is not None:
        config.ssl.minimum_version = ssl.TLSVersion.TLSv1_3
    return uvicorn.Server(config) if primary else _Secondary(config)


async def run_listeners(listeners: list[Listener], tls: TlsSettings) -> None:
    """Corre todos los listeners; cuando el primero termina (Ctrl+C / SIGTERM) apaga el resto."""
    servers = [_build(lst, tls, primary=(i == 0)) for i, lst in enumerate(listeners)]
    tasks = [asyncio.create_task(s.serve()) for s in servers]
    await tasks[0]
    for s in servers[1:]:
        s.should_exit = True
    await asyncio.gather(*tasks[1:], return_exceptions=True)
