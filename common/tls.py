"""Contextos SSL para servidores y clientes (RNF6: cifrado en tránsito).

- Todo exige TLS 1.3 como mínimo.
- Los listeners INTERNOS (control<->nodos, nodo<->nodo) exigen certificado de cliente (mTLS):
  solo procesos con un certificado firmado por la CA interna pueden conectarse.
- Los listeners PÚBLICOS (cliente->control, cliente->nodo) usan TLS de servidor; el cliente
  verifica el certificado del servidor con la CA, el servidor autentica al usuario con JWT
  o con la capacidad firmada.
"""
from __future__ import annotations

import ssl

from .config import TlsSettings


def client_ssl_context(tls: TlsSettings, *, with_client_cert: bool) -> ssl.SSLContext | bool:
    """Contexto para `httpx` cuando ESTE proceso es el cliente de otro.

    with_client_cert=True  -> mTLS (llamadas internas servidor a servidor).
    with_client_cert=False -> solo verifica al servidor (cliente de usuario).
    Si TLS está apagado devuelve True (httpx usa su verificación por defecto; no se usa en http://).
    """
    if not tls.enabled:
        return True
    ctx = ssl.create_default_context(cafile=tls.ca)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    if with_client_cert:
        ctx.load_cert_chain(certfile=tls.cert, keyfile=tls.key)
    return ctx
