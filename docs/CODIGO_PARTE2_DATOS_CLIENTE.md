# Parte 2 · Nodos de datos, cliente, despliegue y pruebas

## `datanode/`

| Archivo | Qué hace |
|---|---|
| `settings.py` | Configuración del nodo (puertos, rutas, clave maestra, URL del control). |
| `blockstore.py` | `BlockStore`: guarda cada bloque en disco cifrado con AES-256-GCM. Cabecera `DFB1` + nonce + sha256 + longitud; el AAD es `block_id + sha + longitud`, así un bloque copiado a otro nombre no descifra. `put` escribe a un temporal y renombra (atómico); `get` verifica; `quarantine` aparta bloques corruptos; `scrub` los revisa; `inventory` alimenta el informe de bloques. |
| `app.py` | API del nodo. `put_block`: valida la capacidad, comprueba longitud y hash, guarda local, reenvía a la réplica por mTLS (`put_replica`) y solo responde 201 cuando **ambas** copias están guardadas; luego avisa al control (`confirm`). `get_block` / `head_block` sirven lecturas. `repair`, `raw_block`, `delete_block`, `inventory` son internos (mTLS). Bucles: registro, latido, informe de bloques, scrub. |
| `main.py` | Arranque con los dos listeners. |

## `client/`

| Archivo | Qué hace |
|---|---|
| `api.py` | `DFShaClient`. `_control` prueba las 3 instancias con reintentos, renueva el JWT si caduca y envía `Idempotency-Key`. `put`: abre con `truncate`, sube bloques en paralelo (`parallel=8`) con `_upload_block`, cierra. `_upload_block` reasigna con `exclude` si un nodo falla. `get`: descarga en paralelo, verifica hash de cada bloque y del total. Lecturas con timeout de 3 s para saltar nodos congelados. |
| `handle.py` | `FileHandle`: `read/write/append/close/abort` por offset. Escribe con lectura-modificación-escritura de bloques sucios; un hilo renueva el handle. Las escrituras se publican al `close()`. |
| `cli.py`, `__main__.py` | `python -m client login | useradd | ls | cd | mkdir | rmdir | rm | put | get | shell`. |
| `errors.py` | Excepciones del cliente. |

## `deploy/` y `scripts/`

| Archivo | Qué hace |
|---|---|
| `gen_certs.sh` | CA propia y certificados de servidor/cliente. |
| `gen_secrets.sh` | Clave de capacidades, clave maestra de cifrado, secreto JWT. |
| `Dockerfile`, `docker-compose.yml` | Todo el clúster en un host. **No probado** (no hay daemon Docker en el entorno de desarrollo). |
| `aws/docker-compose.vm.yml`, `aws/.env.vm.example` | Plantilla por VM para AWS Academy. **No probada.** |
| `scripts/local_cluster.py` | `LocalCluster`: levanta 3 etcd + 3 control + 3 nodos como procesos reales, con kill/start por componente. Lo usan las pruebas. |
| `scripts/benchmark.py` | Experimentos E1–E7. |
| `scripts/make_parts.py` | Genera los zips por parte. |

## Pruebas de esta parte

`tests/conftest.py` (fixtures: `cluster`, `restore`, `alice`, `bob`), `tests/unit/test_blockstore.py` y
`tests/e2e/test_part2_*.py`: integridad, semántica RF3, replicación con fallos, consistencia de escrituras,
seguridad del canal y rendimiento.
