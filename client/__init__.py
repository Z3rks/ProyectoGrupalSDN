"""Paquete `client`: librería y CLI de DFSha (PARTE 2).

    from client import DFShaClient
    c = DFShaClient(control=["https://10.0.1.11:4430"], ca="certs/ca.crt")
    c.login("ana", "secreto-123")
    c.put("informe.pdf", "/docs/informe.pdf")
    c.get("/docs/informe.pdf", "copia.pdf")

El cliente solo conoce las direcciones del SERVICIO DE CONTROL. Dónde está cada bloque lo
descubre en cada operación (manifiesto), así que se pueden mover/agregar nodos sin tocarlo (RNF7).
"""
from .api import DFShaClient
from .errors import DFShaClientError
from .handle import FileHandle

__all__ = ["DFShaClient", "DFShaClientError", "FileHandle"]
