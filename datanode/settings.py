"""Configuración del nodo de datos (variables de entorno DFSHA_*)."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

from common.config import TlsSettings, env, env_float, env_int, env_list, tls_from_env


def load_master_key() -> bytes:
    """Clave maestra AES-256 del nodo (32 bytes).

    Se entrega como SECRETO del despliegue y queda FUERA del volumen de datos:
      - DFSHA_MASTER_KEY       64 caracteres hex, o
      - DFSHA_MASTER_KEY_FILE  ruta a un archivo (p. ej. un secreto de Docker) con esos 64 hex.
    Si no hay ninguna el nodo NO arranca: nunca guardamos datos sin cifrar por descuido.
    """
    raw = env("DFSHA_MASTER_KEY")
    path = env("DFSHA_MASTER_KEY_FILE")
    if raw is None and path:
        with open(path, "r", encoding="utf-8") as fh:
            raw = fh.read().strip()
    if raw is None:
        raise SystemExit("Falta DFSHA_MASTER_KEY o DFSHA_MASTER_KEY_FILE (clave maestra de cifrado en reposo)")
    if len(raw) == 64:
        return bytes.fromhex(raw)
    # Permite también una frase: se deriva una clave de 32 bytes (válido para pruebas, no para producción).
    return hashlib.sha256(raw.encode()).digest()


@dataclass(frozen=True)
class Settings:
    node_id: str
    host: str
    public_port: int           # TLS de servidor: cliente -> nodo
    internal_port: int         # mTLS: control/nodos -> nodo
    endpoint: str              # URL pública que se anuncia al control (la usa el cliente)
    internal_endpoint: str     # URL interna que se anuncia al control (la usan otros servidores)
    data_dir: str
    control_internal: list[str]  # URLs internas (mTLS) de las instancias de control
    cap_key: bytes
    master_key: bytes
    max_block: int
    heartbeat_s: float
    scrub_s: float
    replica_timeout_s: float
    tls: TlsSettings


def load() -> Settings:
    node_id = env("DFSHA_NODE_ID", "dn-1")
    public_port = env_int("DFSHA_PUBLIC_PORT", 8443)
    internal_port = env_int("DFSHA_INTERNAL_PORT", 8444)
    host_name = env("DFSHA_ADVERTISE_HOST", "localhost")
    tls = tls_from_env()
    return Settings(
        node_id=node_id,
        host=env("DFSHA_HOST", "0.0.0.0"),
        public_port=public_port,
        internal_port=internal_port,
        endpoint=env("DFSHA_ENDPOINT", f"{tls.scheme}://{host_name}:{public_port}"),
        internal_endpoint=env("DFSHA_INTERNAL_ENDPOINT", f"{tls.scheme}://{host_name}:{internal_port}"),
        data_dir=env("DFSHA_DATA_DIR", f"./data/{node_id}"),
        control_internal=env_list("DFSHA_CONTROL_INTERNAL", f"{tls.scheme}://localhost:4431"),
        cap_key=env("DFSHA_CAP_KEY", "cambiar-esta-clave-de-capacidades").encode(),
        master_key=load_master_key(),
        max_block=env_int("DFSHA_MAX_BLOCK", 64 * 1024 * 1024),
        heartbeat_s=env_float("DFSHA_HEARTBEAT", 5.0),
        scrub_s=env_float("DFSHA_SCRUB_INTERVAL", 300.0),
        replica_timeout_s=env_float("DFSHA_REPLICA_TIMEOUT", 10.0),
        tls=tls,
    )
