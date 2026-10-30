"""Lectura de configuración desde variables de entorno.

Todos los procesos (control, nodo de datos, cliente) se configuran con variables
`DFSHA_*`. Así el mismo contenedor sirve para pruebas locales y para AWS: solo
cambian las variables (12-factor app).
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def env(name: str, default: str | None = None) -> str | None:
    """Devuelve la variable de entorno `name` o `default` si no existe/está vacía."""
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def env_int(name: str, default: int) -> int:
    return int(env(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(env(name, str(default)))


def env_bool(name: str, default: bool) -> bool:
    value = env(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "on", "yes", "si", "sí")


def env_list(name: str, default: str = "") -> list[str]:
    """Lista separada por comas: 'a, b,c' -> ['a', 'b', 'c']."""
    raw = env(name, default) or ""
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class TlsSettings:
    """Rutas de los certificados de un proceso.

    - ca:   certificado de la CA interna (para verificar al otro extremo).
    - cert/key: identidad propia del proceso (cada proceso tiene la suya).
    Si `enabled` es False todo corre en HTTP plano (SOLO para depuración local).
    """

    enabled: bool = False
    ca: str | None = None
    cert: str | None = None
    key: str | None = None

    @property
    def scheme(self) -> str:
        return "https" if self.enabled else "http"


def tls_from_env() -> TlsSettings:
    """DFSHA_TLS=on|off, DFSHA_TLS_CA, DFSHA_TLS_CERT, DFSHA_TLS_KEY."""
    return TlsSettings(
        enabled=env_bool("DFSHA_TLS", False),
        ca=env("DFSHA_TLS_CA"),
        cert=env("DFSHA_TLS_CERT"),
        key=env("DFSHA_TLS_KEY"),
    )


# Tamaño de bloque por defecto: 4 MiB (decisión del diseño, validada en el experimento E2).
DEFAULT_BLOCK_SIZE = 4 * 1024 * 1024
# Factor de replicación R=2: cada bloque vive en dos nodos distintos.
REPLICATION_FACTOR = 2
