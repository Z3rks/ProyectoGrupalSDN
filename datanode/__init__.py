"""Paquete `datanode`: plano de datos de DFSha (PARTE 2).

Un nodo de datos guarda bloques cifrados en disco y los sirve DIRECTAMENTE al cliente (el
control nunca ve el contenido). Responsabilidades:
  - validar la capacidad firmada de cada petición,
  - escribir bloques de forma atómica (TEMP -> STORED) y cifrarlos en reposo (AES-256-GCM),
  - replicar cada bloque al nodo réplica (pipeline síncrono) y avisar al control (confirm),
  - latidos, inventario, reparación, borrado y verificación periódica de integridad (scrubbing).
"""
