# Pruebas: mapa de las 7 pruebas mínimas del enunciado

**Resultado verificado (30/10/2026, una sola máquina, procesos reales con TLS 1.3 y mTLS):**
`python -m pytest tests -q` → **107 passed en 3 min 24 s** (21 + 10 unitarias, 76 e2e).
Los e2e usan tiempos acortados (latido 1 s, SUSPECT 4 s, DEAD 10 s) y bloques de 256 KiB para que corran rápido.
Las pruebas automáticas corren en una sola máquina; el despliegue real se probó a mano en AWS Academy (resultados al final de este documento).

| # | Prueba mínima del enunciado | Pruebas automáticas | Qué demuestran |
|---|---|---|---|
| 1 | Carga y recuperación con hash | `test_part2_rf2_integrity.py` (`put_get_recupera_el_archivo_identico`, parametrizada por tamaños; particionado con 2 copias; sobrescritura; `rm` libera bloques; descubrimiento dinámico) | SHA-256 idéntico tras `put`/`get`; bloques repartidos entre nodos |
| 2 | RF3: open/read/write/close/lock | `test_part2_rf3_semantics.py` (12), `test_part1_locks_concurrency.py` (locks explícitos y vencimiento) | Snapshot por handle, nada visible hasta `close`, `abort` no publica, copy-on-write por bloque |
| 3 | Concurrencia | `test_part1_locks_concurrency.py` (10) | Segundo escritor recibe `LOCKED`; lectores no se bloquean; fencing impide publicar a un escritor con lock perdido; creación simultánea: gana uno |
| 4 | Replicación y fallos | `test_part2_replication_faults.py` (6), `test_part1_control_ha.py` (5) | `get`/`put` con un nodo caído; reparación a R = 2; segunda caída tolerada; reconciliación al reaparecer; corrupción detectada y reparada; caída del líder de control y de un miembro de etcd |
| 5 | Consistencia | `test_part2_consistency_writes.py` (5) | Bloque incompleto no publica; commit exige bloques confirmados; cliente que muere antes de `close` no publica; `Idempotency-Key` |
| 6 | Seguridad | `test_part1_security_access.py` (10), `test_part2_security_channel.py` (6) | Autenticación/autorización, capacidades inválidas o de otro nodo, TLS 1.3 obligatorio, mTLS en puertos internos, sin texto en claro en red ni en disco |
| 7 | Rendimiento (≥ 2 condiciones) | `test_part2_performance.py` (1 y 4 clientes) y `scripts/benchmark.py` (E1–E7) | Integridad bajo concurrencia; medidas abajo |

Las pruebas unitarias están en `tests/unit/` (`test_blockstore.py`, `test_common_and_control_pure.py`).

## Rendimiento (benchmark `--quick`, local)

`python scripts/benchmark.py --quick` escribe `results/benchmark.json` y `results/benchmark.md`.
Valores medianos de esa corrida (2 CPU compartidas entre todos los procesos, **no representativos de AWS**):

| Experimento | Resultado |
|---|---|
| E1 `put` / `get` de 8 MiB | 33,4 / 43,8 MiB/s (hash correcto) |
| E2 bloque 1 MiB vs 4 MiB, archivo de 64 MiB | `put` 30,9 vs 46,2 MiB/s |
| E7 metadatos (mediana) | `mkdir` 11,6 ms · `ls` 8,6 ms · `stat` 3,9 ms |

Estas cifras son locales. Las mediciones en AWS están en la sección "Resultados en AWS Academy" (una sola ejecución cada una); repetirlas con más corridas queda como trabajo futuro.

## Cómo ejecutar

```bash
python -m pytest tests/unit -q                      # sin clúster
python -m pytest tests/e2e/test_part1_*.py -q       # Parte 1 (necesita también la Parte 2 para el clúster)
python -m pytest tests/e2e/test_part2_*.py -q       # Parte 2
```

Si un `pytest` se interrumpe, pueden quedar procesos `etcd`, `control.main` y `datanode.main` ocupando los puertos
15000+; elimínelos antes de repetir (síntoma: `etcd no arrancó ... address already in use`).

## Resultados en AWS Academy (3/11/2026)

**Entorno:** 4 instancias EC2 t3.medium (2 vCPU, 4 GiB), Ubuntu 24.04, us-east-1b, misma VPC. VM-1 a VM-3: `etcd` + `control`
+ `datanode` en contenedores separados (Docker Compose, red `host`, TLS 1.3 y mTLS). VM-4: cliente. Parámetros por defecto
(bloque de 4 MiB, R = 2, latido 5 s, SUSPECT 15 s, DEAD 120 s). Cada medida es **una sola ejecución** (sin repeticiones);
para el informe conviene repetirlas y reportar mediana y p95.

| # | Prueba | Procedimiento | Resultado observado |
|---|---|---|---|
| 1 | Carga y recuperación con hash | `put` y `get` de 64 MiB aleatorios (67.108.864 B) | `put` 0,842 s (96,0 MiB/s); `get` 1,013 s (76,8 MiB/s); SHA-256 idéntico (`af1fa1e5…caa58dc`) |
| 4a | Fallo de un nodo de datos | `stop datanode` en VM-1; `get` y `put` | `get` 0,954 s (82,9 MiB/s), hash idéntico; `put` terminó sobre los 2 nodos vivos (57,2 MiB/s); `get` de ese archivo (79,8 MiB/s), hash idéntico |
| 4b | Fallo de una instancia de control | `stop control` en VM-1; `put` | `put` 0,875 s (91,7 MiB/s), hash correcto: el cliente pasó a otra instancia |
| 4c | Fallo de un miembro de etcd (2 de 3) | `stop etcd` en VM-2; `put` y `get` | `put` 0,860 s (94,0 MiB/s); `get` 81,1 MiB/s, hash idéntico |
| 4d | Pérdida de la mayoría de etcd (1 de 3) | `stop etcd` en VM-2 y VM-3; `put` | `503 CONTROL_NO_QUORUM`; al restaurar etcd, el `put` volvió a funcionar |
| 3 | Concurrencia | Dos clientes suben archivos distintos (32 MiB) a la misma ruta | Uno terminó; el otro recibió `409` (archivo con bloqueo de escritura vigente). Hash final = el del archivo ganador |
| 7 | Rendimiento, 4 clientes simultáneos | 4 clientes (4 homes) suben y luego bajan un archivo de 64 MiB cada uno (256 MiB en total), desde VM-4 | Subida 2,424 s (105,6 MiB/s en conjunto, 26,4 por cliente); bajada 3,026 s (84,6 MiB/s en conjunto, 21,2 por cliente); los 4 hashes coinciden. Frente a 1 cliente (96,0 / 76,8 MiB/s) el total sube solo ~10 %; hipótesis (no comprobada): el cliente de VM-4 (2 vCPU, `user` 3,8 s en 2,4 s reales) limita la medida |
| 6 | Seguridad | `curl` sin token y con token falso | `401` en ambos |
| 6 | Seguridad | `openssl s_client -tls1_2` / `-tls1_3` contra el puerto 443 | TLS 1.2 rechazado (código 1, `Cipher is (NONE)`); TLS 1.3 aceptado (`TLS_AES_256_GCM_SHA384`) |
| 6 | Seguridad | Puerto interno 4431 sin certificado de cliente | Conexión rechazada (mTLS) |
| 6 | Seguridad | Otro usuario pide un archivo ajeno | `404 NOT_FOUND` |
| 6 | Seguridad | `grep` del texto del archivo en `/data` de los 3 nodos | Sin texto en claro en ningún nodo |

**No ejecutado en AWS (solo en el entorno local, con las pruebas automáticas):** consistencia con réplica desconectada a mitad
de escritura, cliente que muere antes de `close`, reparación de réplicas tras DEAD, corrupción en reposo y reconciliación al
reaparecer un nodo. Esas pruebas pasan en `tests/e2e/` con tiempos acortados (ver arriba); repetirlas a mano en AWS tomaría
más de 2 minutos por escenario por los umbrales por defecto (SUSPECT 15 s, DEAD 120 s).
