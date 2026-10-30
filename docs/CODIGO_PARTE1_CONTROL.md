# Parte 1 · Plano de control: qué hace cada archivo y cómo funciona

El servicio de control **nunca toca el contenido de los archivos**. Decide quién eres, dónde viven los bloques, quién
puede escribir y cuál es la versión vigente. No guarda estado local: todo está en etcd, por eso se pueden levantar
3 instancias iguales y el cliente puede hablar con cualquiera.

## `common/` (contrato compartido con la Parte 2)

| Archivo | Qué hace |
|---|---|
| `config.py` | Carga variables de entorno y rutas de certificados. |
| `errors.py` | `DFShaError(status, code, message, retryable, retry_after_s, extra)`: un único formato de error JSON para control, nodos y cliente. |
| `hashing.py` | SHA-256 de bloques y de archivos completos. |
| `capabilities.py` | Capacidades: tokens firmados con HMAC-SHA256 que dicen "este usuario puede hacer `op` sobre este `block_id` en este nodo hasta `exp`" (máx. 300 s). Los nodos las validan **sin llamar al control**. |
| `tls.py` | Contextos TLS: TLS 1.3 mínimo; el listener interno exige certificado de cliente (mTLS). |
| `server.py` | `run_listeners`: levanta dos puertos por proceso, uno público (TLS) y uno interno (mTLS). |

## `control/`

| Archivo | Qué hace |
|---|---|
| `etcd.py` | Cliente de etcd por su gateway HTTP/JSON: lecturas por rango, transacciones con comparaciones (compare-and-swap), leases. Máximo 100 operaciones por transacción. |
| `state.py`, `settings.py` | Objeto compartido con el cliente etcd y la configuración de la instancia. |
| `paths.py` | Normaliza y valida rutas. |
| `auth.py` | Usuarios con argon2id, JWT de 15 min y *refresh token* rotativo (se guarda solo su hash), límite de intentos de login. |
| `nodes.py` | Registro de nodos y latidos. El estado (HEALTHY/SUSPECT/DEAD) se **calcula** por la edad del último latido. |
| `placement.py` | `choose()`: elige las 2 réplicas de cada bloque repartiendo en círculo con desplazamiento por archivo. |
| `blocks.py` | Registro `/blk/{block_id}` con las ubicaciones de cada bloque. |
| `namespace.py` | RF1: `ls`, `mkdir`, `rmdir`, `rm`, `stat`. Cada entrada se cambia con CAS sobre su `mod_revision`. |
| `files.py` | **Núcleo.** Locks, handles y commit atómico (ver abajo). |
| `leader.py` | `LeaderService`: elección por lease en `/leader` (TTL 10 s). Solo el líder ejecuta reparación y GC. |
| `repair.py` | `repair_pass`: busca bloques con menos de 2 copias sanas y ordena copiarlos a otro nodo. |
| `gc.py` | `gc_pass`: mark-and-sweep de bloques huérfanos; lee `/h` antes que `/f` para no borrar bloques de una escritura en curso. |
| `schemas.py` | Modelos de entrada/salida. |
| `routes_public.py`, `routes_internal.py` | Endpoints para clientes (JWT) y para nodos (mTLS). |
| `app.py`, `main.py` | Ensamblado de FastAPI y arranque. |

### `control/files.py`: cómo se escribe un archivo

1. **`open_file`**: adquiere el lock `uid:path` (`_acquire_lock`) con una transacción que solo tiene éxito si la clave no existe. El *fencing token* es la revisión de esa transacción. Crea un *handle* con vencimiento renovable (`renew_handle`).
2. **`allocate`**: por cada índice de bloque pedido, `placement.choose` elige 2 nodos sanos (excluyendo los que el cliente reporta fallidos) y devuelve capacidades firmadas.
3. El cliente sube los bloques a los nodos (Parte 2). Cada nodo llama a **`confirm_block`** cuando tiene sus copias.
4. **`close_handle` → `_commit`**: reúne los bloques nuevos (`/h/{hid}/pend`) y los heredados de la versión anterior, valida longitudes, escribe el manifiesto de la versión n+1 en lotes protegidos por el lock (si el lock se perdió, la transacción falla), y por último **una sola transacción** cambia la entrada del namespace a la versión n+1 y guarda el resultado de la clave de idempotencia. Si algo falla antes de esa transacción, la versión anterior sigue intacta.
5. Los bloques son inmutables (copy-on-write): una escritura parcial reescribe solo los bloques que toca.

### Claves de etcd

| Clave | Contenido |
|---|---|
| `/u/…`, `/rt/…` | Usuarios y *refresh tokens* (hash). |
| `/ns/{uid}:{parent}\|{name}` | Entrada del namespace (archivo o directorio). |
| `/h/{hid}` (+`/alloc`, `/pend`) | Handle abierto y sus bloques pendientes. |
| `/lock/{uid}:{path}`, `/lockid/…` | Lock exclusivo y su índice por id. |
| `/f/{fid}/v{n}/meta`, `/f/{fid}/v{n}/b/{idx}` | Versión inmutable y manifiesto. |
| `/blk/{block_id}` | Ubicaciones del bloque. |
| `/n/{node}` | Nodo registrado y último latido. |
| `/retired`, `/deleted` | Versiones/archivos pendientes de GC. |
| `/idem/…` | Resultado de operaciones con `Idempotency-Key`. |
| `/leader` | Lease del líder. |

## Garantías y dónde se prueban

Las pruebas de esta parte son `tests/unit/test_common_and_control_pure.py` y `tests/e2e/test_part1_*.py`
(namespace, seguridad de acceso, locks y concurrencia, alta disponibilidad del control).
