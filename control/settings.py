"""Configuración del servicio de control (todas por variables de entorno DFSHA_*)."""
from __future__ import annotations

import socket
from dataclasses import dataclass

from common.config import (
    DEFAULT_BLOCK_SIZE,
    REPLICATION_FACTOR,
    TlsSettings,
    env,
    env_int,
    env_list,
    tls_from_env,
)


@dataclass(frozen=True)
class Settings:
    instance_id: str
    etcd_endpoints: list[str]
    host: str
    public_port: int          # TLS de servidor: cliente -> control
    internal_port: int        # mTLS: nodos de datos -> control
    jwt_secret: str           # firma de JWT (compartido por las 3 instancias)
    cap_key: bytes            # clave HMAC de capacidades (compartida con los nodos)
    admin_user: str
    admin_password: str
    block_size: int
    replication: int
    access_ttl_s: int         # vida del JWT de acceso (15 min)
    refresh_ttl_s: int        # vida del token de refresco (8 h)
    handle_ttl_s: int         # lease de un handle abierto (60 s)
    lock_ttl_s: int           # lease de un lock (30 s)
    cap_ttl_s: int            # vida de una capacidad (<= 300 s)
    suspect_after_s: float    # sin heartbeat -> SUSPECT (15 s)
    dead_after_s: float       # sin heartbeat -> DEAD (120 s)
    repair_interval_s: float
    gc_interval_s: float
    grace_version_s: float    # espera antes de borrar una versión reemplazada (10 min)
    grace_orphan_s: float     # espera antes de borrar un bloque huérfano (1 h)
    leader_ttl_s: int         # lease del líder (10 s)
    login_limit: int          # intentos de login por minuto
    tls: TlsSettings


def load() -> Settings:
    return Settings(
        instance_id=env("DFSHA_INSTANCE_ID", socket.gethostname()),
        etcd_endpoints=env_list("DFSHA_ETCD", "http://127.0.0.1:2379"),
        host=env("DFSHA_HOST", "0.0.0.0"),
        public_port=env_int("DFSHA_PUBLIC_PORT", 4430),
        internal_port=env_int("DFSHA_INTERNAL_PORT", 4431),
        jwt_secret=env("DFSHA_JWT_SECRET", "cambiar-este-secreto-jwt"),
        cap_key=env("DFSHA_CAP_KEY", "cambiar-esta-clave-de-capacidades").encode(),
        admin_user=env("DFSHA_ADMIN_USER", "admin"),
        admin_password=env("DFSHA_ADMIN_PASSWORD", "admin-cambiar-ya"),
        block_size=env_int("DFSHA_BLOCK_SIZE", DEFAULT_BLOCK_SIZE),
        replication=env_int("DFSHA_REPLICATION", REPLICATION_FACTOR),
        access_ttl_s=env_int("DFSHA_ACCESS_TTL", 900),
        refresh_ttl_s=env_int("DFSHA_REFRESH_TTL", 8 * 3600),
        handle_ttl_s=env_int("DFSHA_HANDLE_TTL", 60),
        lock_ttl_s=env_int("DFSHA_LOCK_TTL", 30),
        cap_ttl_s=env_int("DFSHA_CAP_TTL", 300),
        suspect_after_s=float(env("DFSHA_SUSPECT_AFTER", "15")),
        dead_after_s=float(env("DFSHA_DEAD_AFTER", "120")),
        repair_interval_s=float(env("DFSHA_REPAIR_INTERVAL", "10")),
        gc_interval_s=float(env("DFSHA_GC_INTERVAL", "60")),
        grace_version_s=float(env("DFSHA_GRACE_VERSION", "600")),
        grace_orphan_s=float(env("DFSHA_GRACE_ORPHAN", "3600")),
        leader_ttl_s=env_int("DFSHA_LEADER_TTL", 10),
        login_limit=env_int("DFSHA_LOGIN_LIMIT", 5),
        tls=tls_from_env(),
    )
