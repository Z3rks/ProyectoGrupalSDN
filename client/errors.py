"""Excepción única del cliente: refleja el JSON de error estándar del servidor."""
from __future__ import annotations


class DFShaClientError(Exception):
    """Error devuelto por DFSha (o de red, con status=0).

    Atributos: status (HTTP), code (LOCKED, NOT_FOUND, ...), message, retryable,
    operation_id y `extra` (campos adicionales como retry_after_s o failed_node).
    """

    def __init__(self, status: int, code: str, message: str, retryable: bool = False,
                 operation_id: str | None = None, extra: dict | None = None) -> None:
        super().__init__(f"[{status} {code}] {message}")
        self.status = status
        self.code = code
        self.message = message
        self.retryable = retryable
        self.operation_id = operation_id
        self.extra = extra or {}
