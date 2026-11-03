# Pruebas: mapa de las 7 pruebas mínimas del enunciado

**Resultado verificado (30/10/2026, una sola máquina, procesos reales con TLS 1.3 y mTLS):**
`python -m pytest tests -q` → **107 passed en 3 min 24 s** (21 + 10 unitarias, 76 e2e).
Los e2e usan tiempos acortados (latido 1 s, SUSPECT 4 s, DEAD 10 s) y bloques de 256 KiB para que corran rápido.
**No se han ejecutado en las VM de AWS**; hay que repetirlas allí para el informe.

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

Para el informe: repetir en AWS con `--reps 5` (sin `--quick`) e incluir los gráficos.

## Cómo ejecutar

```bash
python -m pytest tests/unit -q                      # sin clúster
python -m pytest tests/e2e/test_part1_*.py -q       # Parte 1 (necesita también la Parte 2 para el clúster)
python -m pytest tests/e2e/test_part2_*.py -q       # Parte 2
```

Si un `pytest` se interrumpe, pueden quedar procesos `etcd`, `control.main` y `datanode.main` ocupando los puertos
15000+; elimínelos antes de repetir (síntoma: `etcd no arrancó ... address already in use`).
