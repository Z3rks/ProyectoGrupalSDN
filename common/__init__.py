"""Paquete `common`: código compartido por TODOS los procesos de DFSha.

Contiene el "contrato" entre las dos partes del proyecto:
  - errors.py        formato único de errores JSON (code, message, operation_id, retryable)
  - capabilities.py  capacidades temporales firmadas (HMAC) que el control entrega y el nodo valida
  - hashing.py       utilidades SHA-256
  - config.py        lectura de variables de entorno y configuración TLS
  - tls.py           contextos SSL (TLS 1.3 / mTLS) para servidores y clientes HTTP
  - server.py        arranque de dos listeners (público TLS + interno mTLS) en un mismo proceso

Pertenece a la PARTE 1 (quien la escribe), pero la PARTE 2 la importa.
"""
