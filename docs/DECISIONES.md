# Arquitectura de consenso: comparación de propuestas y registro de decisiones

Equipo: Gregorio Carvajal Gaviria y Santiago Posso Acevedo. Ambos diseñaron en la Fase I la **Opción 1 (cliente/servidor
con servicio S2S en red privada)**, así que comparten la idea de fondo: plano de control separado del plano de datos,
bloques inmutables con copy-on-write, commit atómico, consistencia fuerte en metadatos (CP) y seguridad por capas.
Difieren en los mecanismos. Este documento compara ambas propuestas, fija la decisión de consenso e indica qué quedó
implementado.

## 1. Decisión

Se implementa la arquitectura de **Gregorio como base** (control sin estado sobre etcd, bloques de 4 MiB, R = 2
síncrona), incorporando de la propuesta de **Santiago** las ideas que encajan con esa base y que se pudieron validar
(sección 4). Lo que no se implementó se declara como limitación o trabajo futuro (sección 5).

Motivo principal: la propuesta de Santiago incluye su propio protocolo Raft para los metadatos, y ella misma prevé en su
sección 12 un plan alternativo "si Raft completo no cabe en la Fase II". Usar **etcd** (Raft ya probado) da elección de
líder, transacciones compare-and-swap y leases sin escribir un consenso propio, que es el componente de mayor riesgo.

## 2. Matriz comparativa de las propuestas individuales

| Criterio | Gregorio | Santiago | Consenso (implementado) |
|---|---|---|---|
| Arquitectura | Opción 1, S2S, 2 planos | Opción 1, S2S, 2 planos | Opción 1, S2S, 2 planos |
| Plano de control | 3 instancias activas **sin estado**, estado en etcd | 3 MetaNodes con estado (SQLite) y un líder; seguidores redirigen (307) | 3 instancias activas sin estado (Gregorio) |
| Consenso de metadatos | etcd (Raft) externo | Raft propio entre MetaNodes | etcd (Raft) |
| Almacén de metadatos | Claves en etcd (una por bloque, manifiesto paginable) | SQLite indexado replicado por log de Raft | Claves en etcd |
| Tamaño de bloque | 4 MiB | 8 MiB | 4 MiB (el benchmark E2 compara tamaños) |
| Réplicas | R = 2 síncronas, 3 nodos de datos | R = 3 con escritura confirmada con W = 2, 4+ nodos | R = 2 síncrona, 3 nodos de datos |
| Cadena de réplicas | Cliente → primario → réplica | Cliente → cadena de 3 nodos | Cliente → primario → réplica |
| Colocación | Round-robin con desplazamiento por archivo | `(hash(file_id)+idx) mod N` y los demás por menor ocupación | Round-robin con desplazamiento por archivo |
| Protocolo de control | REST/HTTPS (FastAPI) | REST/HTTPS (FastAPI) | REST/HTTPS |
| Protocolo de datos | HTTPS binario | gRPC con streams de 1 MiB | HTTPS binario |
| Seguridad servidor-servidor | mTLS con CA interna | mTLS con CA interna | mTLS en un puerto interno separado |
| Concurrencia | Lock exclusivo por **archivo** con lease y fencing | Lock *shared/exclusive* por **rango de bloques** con lease y epoch | Lock exclusivo por archivo (clave `uid:path`) con fencing |
| Commit | Una transacción de etcd cambia la versión vigente | Un comando de Raft cambia la versión vigente | Transacción de etcd; reintento idempotente con `Idempotency-Key` |
| Detección de fallos | Latido 5 s, SUSPECT 15 s, DEAD 120 s | Latido 3 s, SUSPECT tras 3 latidos, DEAD 30 s | Valores de Gregorio, configurables por variables de entorno |
| Reparación | El líder de control repara bloques con una sola copia | El líder encola por menos copias y limita copias concurrentes | El líder de control repara (`repair_pass`) |
| Limpieza (GC) | Gracia de 10 min tras reemplazar una versión | Gracia de 15 min tras borrar | 10 min para versiones, 1 h para bloques huérfanos |
| Autenticación | argon2id, JWT 15 min, refresh 8 h, 5 intentos/min | argon2id, JWT 15 min Ed25519, refresh, 5 intentos/min | argon2id, JWT 15 min **HS256**, refresh 8 h (hash en etcd), 5 intentos/min |
| Acceso a bloques | Capacidad firmada con HMAC, validada por el nodo sin consultar al control | Token de 120 s atado a usuario, bloque y operación, validado por el nodo | Capacidad HMAC-SHA256 (máx. 5 min), validada por el nodo |
| Cifrado en reposo | AES-256-GCM por bloque | AES-256-GCM con clave derivada por HKDF; AAD `block_id` + `user_id` | AES-256-GCM por bloque con clave maestra del nodo; AAD `block_id` + sha + longitud |
| Escritura interrumpida | Archivo temporal, `fsync`, renombre atómico si el hash coincide | Archivo `.part`, `fsync`, renombre atómico si el hash coincide | Igual en ambas |
| Semántica de `write` | Crear, reemplazar, anexar y modificar por bloque (copy-on-write) | Igual | Igual |
| Consistencia | CP: sin mayoría de etcd no hay escrituras | CP: sin mayoría de Raft no se confirma nada | CP |
| Fallos tolerados | 1 nodo de datos y 1 de 3 instancias de control/etcd | 1 MetaNode y 1 DataNode | 1 nodo de datos y 1 instancia de control/etcd |
| Descubrimiento | Nombre del control (3 registros); manifiesto con ubicaciones | Nombre lógico por DNS y `GET /v1/cluster/topology` | El cliente conoce las URL de control; ubicaciones por manifiesto |

### Fortalezas y debilidades de cada propuesta

| | Fortalezas | Debilidades |
|---|---|---|
| Gregorio | Consenso resuelto con etcd, sin código de Raft propio. Control sin estado, fácil de escalar y reiniciar. Replicación simple de razonar. | Lock por archivo: dos escritores en bloques distintos de un mismo archivo se serializan. Con R = 2 hay una sola copia mientras se repara. |
| Santiago | Locks por rango permiten escrituras paralelas en un archivo. R = 3 con W = 2 sigue escribiendo con un nodo caído sin perder la tercera copia. Detalles de seguridad más finos (Ed25519, HKDF, usuario en el AAD). | Raft propio: mucho código y riesgo. Metadatos con estado en cada MetaNode y reconstrucción de ubicaciones tras una elección. Se necesita al menos un cuarto nodo de datos para aprovechar R = 3. |

## 3. Ideas que se tomaron de cada propuesta

| De Gregorio | De Santiago |
|---|---|
| Control sin estado sobre etcd (3 instancias). | Bloques que nunca se modifican y commit atómico de la versión (coincidente). |
| Capacidades firmadas validadas por el nodo. | Escritura al archivo temporal y renombrado atómico solo si el hash coincide, sin ack antes (coincidente). |
| Lock por archivo con fencing de etcd. | El commit solo cuenta réplicas **reportadas por los nodos**, no las que declara el cliente. |
| Lease del líder de control en etcd para reparación y GC. | Reintento del commit sin duplicar versión (`commit_id` → `Idempotency-Key`). |
| Registro dinámico de nodos y bloques por `block-report`. | Archivo ajeno responde 404, sin revelar si existe. |

## 4. Cambios de implementación respecto al diseño base

| # | Diseño base | Implementación | Motivo |
|---|---|---|---|
| 1 | Lock por archivo | Lock con clave `uid:path` | Cubre carreras de creación de archivos nuevos |
| 2 | Ubicación dentro del manifiesto | Registro separado `/blk/{block_id}` | Reparar una réplica sin reescribir manifiestos (copy-on-write) |
| 3 | Manifiesto en una sola transacción | Manifiesto en lotes, luego una transacción final | Límite de operaciones por transacción de etcd |
| 4 | Interno por ruta | Segundo listener con mTLS | Separación fuerte entre canal público e interno |
| 5 | — | Bandera `truncate` en `open` | Necesaria para que `put` reemplace un archivo |
| 6 | Hash total siempre | `sha256_total` solo en `put` | Con escrituras por handle no se conoce el flujo completo |
| 7 | Estado del nodo persistido | Estado calculado por la edad del latido | Evita escrituras constantes en etcd |
| 8 | etcd por gRPC | etcd por gateway HTTP/JSON (httpx) | Menos dependencias |
| 9 | — | Limitador de intentos en memoria por instancia | Simplicidad; ver limitaciones |
| 10 | — | Timeout de lectura de 3 s en el cliente | Evita esperar 30 s a un nodo congelado |

## 5. De la propuesta de Santiago, lo que no se implementó

Se declara en el informe como limitación o trabajo futuro; no debe presentarse como funcionalidad existente.

| Idea | Estado | Qué habría que hacer |
|---|---|---|
| Raft propio entre MetaNodes | Reemplazado por etcd | Nada, es una decisión de diseño |
| Réplicas R = 3 con W = 2 | No implementado (R = 2) | Parámetro de réplicas en `placement` y umbral de confirmación en el commit |
| gRPC con streams para bloques | No implementado (HTTPS binario) | Servicio gRPC en el nodo de datos y cliente generado |
| Locks por rango, *shared/exclusive* | No implementado (lock exclusivo por archivo) | Claves de lock por rango y validación de commit por bloque |
| JWT con Ed25519 | No implementado (HS256 con secreto compartido) | Cambiar el algoritmo y distribuir la clave pública |
| Clave por bloque con HKDF y `user_id` en el AAD | No implementado (clave maestra del nodo) | Derivar la clave con HKDF y añadir `user_id` al AAD |
| `GET /v1/cluster/topology` | No implementado (el cliente prueba las URL de control) | Endpoint que devuelva el líder y los miembros vivos |
| Cola de reparación priorizada por menos copias | No implementado: cada pasada repara hasta 200 bloques, sin ordenar por número de copias | Ordenar los bloques pendientes por copias sanas en `control/repair.py` |

## 6. Consecuencias

- Se prioriza consistencia sobre disponibilidad (CP): sin mayoría de etcd no hay escrituras.
- Un fallo simultáneo de nodo de datos es tolerado; dos pueden dejar bloques sin réplica hasta reparar.
- El control se puede escalar horizontalmente, pero todas las instancias dependen del mismo clúster etcd.
- Los valores de tiempo de las pruebas son más cortos que los por defecto (latido 1 s, SUSPECT 4 s, DEAD 10 s).
