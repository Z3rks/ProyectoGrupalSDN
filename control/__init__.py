"""Paquete `control`: plano de control de DFSha (PARTE 1).

El servicio de control NO guarda estado propio ni transporta contenido de archivos:
  - autentica y autoriza (JWT),
  - resuelve rutas (namespace) y administra bloqueos y versiones,
  - decide en qué nodos va cada bloque y firma las capacidades,
  - el líder (elegido con un lease de etcd) detecta nodos caídos, repara réplicas y hace GC.
Todo el estado vive en etcd (Raft), por eso se pueden correr 3 instancias activas.
"""
