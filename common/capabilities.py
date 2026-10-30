"""Capacidades temporales firmadas (sección 4.4 del diseño).

Problema: el cliente habla DIRECTAMENTE con los nodos de datos (así el control no
es cuello de botella), pero un nodo no debe aceptar lecturas/escrituras de cualquiera.

Solución: el servicio de control firma con HMAC-SHA256 un "permiso" que dice
exactamente qué se puede hacer:  usuario, operación (put/get), block_id, node_id,
versión, identificador único (jti) y expiración (máx. 5 min). La clave HMAC solo la
conocen el control y los nodos. El nodo valida el permiso SIN consultar al control.

Formato del token:   base64url(JSON de claims) + "." + base64url(HMAC)
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import secrets
import time

from .errors import DFShaError

log = logging.getLogger("dfsha.cap")
MAX_TTL_S = 300  # una capacidad nunca vive más de 5 minutos


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def sign(key: bytes, claims: dict, ttl_s: int = MAX_TTL_S) -> str:
    """Crea una capacidad firmada. `claims` es el contenido (op, block_id, node_id, ...)."""
    claims = dict(claims)
    claims["exp"] = int(time.time()) + min(ttl_s, MAX_TTL_S)
    claims["jti"] = secrets.token_hex(8)
    payload = _b64(json.dumps(claims, sort_keys=True, separators=(",", ":")).encode())
    sig = _b64(hmac.new(key, payload.encode(), hashlib.sha256).digest())
    return f"{payload}.{sig}"


def verify(key: bytes, token: str, *, op: str, block_id: str, node_id: str) -> dict:
    """Valida firma, vencimiento, operación, bloque y nodo. Devuelve los claims.

    Falla con 401 si está vencida y con 403 en cualquier otro caso (el mensaje es
    genérico a propósito: no le decimos a un atacante cuál comprobación falló).
    """
    try:
        payload, sig = token.split(".", 1)
        expected = _b64(hmac.new(key, payload.encode(), hashlib.sha256).digest())
        if not hmac.compare_digest(sig, expected):  # comparación en tiempo constante
            raise ValueError("firma")
        claims = json.loads(_unb64(payload))
    except Exception:
        log.warning("capacidad rechazada: formato o firma inválidos")
        raise DFShaError(403, "FORBIDDEN", "capacidad inválida")

    if claims.get("exp", 0) < time.time():
        raise DFShaError(401, "UNAUTHENTICATED", "capacidad vencida; vuelva a pedir el manifiesto")
    if claims.get("op") != op or claims.get("block_id") != block_id or claims.get("node_id") != node_id:
        log.warning("capacidad rechazada: no corresponde a esta operación/bloque/nodo")
        raise DFShaError(403, "FORBIDDEN", "capacidad inválida")
    return claims
