"""Errores de DFSha con un único formato JSON.

Todo error que ve el cliente tiene la forma (ver Anexo B del diseño):

    {"code": "LOCKED", "message": "...", "operation_id": "op-7f3a", "retryable": true}

`retryable` le dice al cliente si tiene sentido reintentar. Los mensajes nunca
incluyen datos sensibles (tokens, contraseñas, rutas internas).
"""
from __future__ import annotations

import logging
import uuid

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

log = logging.getLogger("dfsha.errors")


class DFShaError(Exception):
    """Excepción de negocio: se convierte en una respuesta HTTP con el formato estándar."""

    def __init__(
        self,
        status: int,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        retry_after_s: int | None = None,
        extra: dict | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.retryable = retryable
        self.retry_after_s = retry_after_s
        self.extra = extra or {}   # datos adicionales de ayuda al cliente (p. ej. {"failed_node": "dn-b"})

    def body(self, operation_id: str) -> dict:
        data = {
            "code": self.code,
            "message": self.message,
            "operation_id": operation_id,
            "retryable": self.retryable,
        }
        if self.retry_after_s is not None:
            data["retry_after_s"] = self.retry_after_s
        data.update(self.extra)
        return data


def operation_id(request: Request) -> str:
    """Identificador de la operación: la Idempotency-Key del cliente o uno generado.

    Se cachea en `request.state` para que log y respuesta usen el mismo valor.
    (No usamos un middleware para no frenar las subidas de bloques de 4 MiB.)
    """
    op = getattr(request.state, "operation_id", None)
    if op is None:
        op = request.headers.get("Idempotency-Key") or "op-" + uuid.uuid4().hex[:8]
        request.state.operation_id = op
    return op


def install_error_handlers(app: FastAPI) -> None:
    """Registra los manejadores que convierten excepciones en JSON estándar."""

    @app.exception_handler(DFShaError)
    async def _dfsha(request: Request, exc: DFShaError):
        headers = {}
        if exc.retry_after_s is not None:
            headers["Retry-After"] = str(exc.retry_after_s)
        return JSONResponse(exc.body(operation_id(request)), status_code=exc.status, headers=headers)

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError):
        # Solo indicamos los campos inválidos, nunca el valor recibido (podría ser un secreto).
        fields = ",".join(str(e.get("loc", ["?"])[-1]) for e in exc.errors())
        err = DFShaError(400, "INVALID_REQUEST", f"campos inválidos: {fields}")
        return JSONResponse(err.body(operation_id(request)), status_code=400)

    @app.exception_handler(Exception)
    async def _unexpected(request: Request, exc: Exception):
        log.exception("error inesperado en %s %s", request.method, request.url.path)
        err = DFShaError(500, "INTERNAL", "error interno", retryable=True)
        return JSONResponse(err.body(operation_id(request)), status_code=500)
