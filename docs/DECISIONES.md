# Registro de decisión arquitectónica (ADR) y matriz comparativa

> **Estado:** borrador. Solo se tuvo acceso a la propuesta individual de Gregorio Carvajal. Las columnas de los otros
> integrantes están vacías y deben completarlas ellos antes de entregar el informe.

## 1. Decisión

Se implementa la **Opción 1: cliente/servidor con servicio S2S en red privada**, partiendo del diseño individual de
Gregorio. Plano de control con 3 instancias sin estado local sobre un clúster etcd (Raft) de 3 miembros; plano de
datos con 3 nodos y R = 2.

## 2. Matriz comparativa de propuestas individuales (plantilla)

| Criterio | Gregorio | Integrante 2 | Integrante 3 |
|---|---|---|---|
| Opción de arquitectura | Opción 1 (cliente/servidor + S2S) | _completar_ | _completar_ |
| Metadatos | etcd (Raft), 3 miembros | _completar_ | _completar_ |
| Bloques / replicación | 4 MiB, R = 2 síncrona | _completar_ | _completar_ |
| Consistencia | Commit atómico por versión | _completar_ | _completar_ |
| Concurrencia | Lock exclusivo + versiones inmutables | _completar_ | _completar_ |
| Seguridad | argon2id, JWT, TLS 1.3, mTLS, AES-GCM | _completar_ | _completar_ |
| Fortalezas / debilidades | _completar_ | _completar_ | _completar_ |

## 3. Cambios respecto al diseño individual

| # | Diseño individual | Implementación | Motivo |
|---|---|---|---|
| 1 | Lock por archivo | Lock con clave `uid:path` | Cubre carreras de creación de archivos nuevos |
| 2 | Ubicación dentro del manifiesto | Registro separado `/blk/{block_id}` | Reparar una réplica sin reescribir manifiestos (CoW) |
| 3 | Manifiesto en una sola transacción | Manifiesto en lotes, luego una transacción final | Límite de operaciones por transacción de etcd |
| 4 | Interno por ruta | Segundo listener con mTLS | Separación fuerte entre canal público e interno |
| 5 | — | Bandera `truncate` en `open` | Necesaria para que `put` reemplace un archivo |
| 6 | Hash total siempre | `sha256_total` solo en `put` | Con escrituras por handle no se conoce el flujo completo |
| 7 | Estado del nodo persistido | Estado calculado por edad del latido | Evita escrituras constantes en etcd |
| 8 | etcd por gRPC | etcd por gateway HTTP/JSON (httpx) | Menos dependencias |
| 9 | — | Limitador de intentos en memoria por instancia | Simplicidad; ver limitaciones |
| 10 | — | Timeout de lectura de 3 s en cliente | Evita esperar 30 s a un nodo congelado |

## 4. Consecuencias

- Se prioriza consistencia sobre disponibilidad (CP): sin mayoría de etcd no hay escrituras.
- Un fallo simultáneo de nodo de datos es tolerado; dos pueden dejar bloques sin réplica hasta reparar.
- Los valores de tiempo de las pruebas son más cortos que los por defecto.
