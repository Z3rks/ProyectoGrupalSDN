# DFSha · Fase II (grupal): sistema de archivos distribuido

Prototipo **ejecutable y distribuido** de DFSha con la **Opción 1: cliente/servidor con servicio distribuido entre
servidores (S2S) en red privada**, resultado del consenso entre los diseños individuales de la Fase I (ver `docs/DECISIONES.md`
para la comparación y los cambios de implementación).

## 1. Qué hace

Cada usuario autenticado tiene un espacio de archivos jerárquico privado. Los archivos se parten en **bloques de 4 MiB**,
se reparten entre varios **nodos de datos** con **2 copias síncronas por bloque (R = 2)** y se reconstruyen en el cliente.
Los metadatos, los bloqueos y las versiones viven en un clúster **etcd (Raft)** al que acceden **3 instancias activas**
del servicio de control. Una escritura se publica de forma **atómica** al cerrar el archivo.

```
                         ┌───────────────────────── PLANO DE CONTROL (sin contenido de archivos) ─────────────────────────┐
  ┌──────────┐  TLS 1.3  │  control-1      control-2      control-3        (3 instancias activas, sin estado local)       │
  │ CLIENTE  │──────────►│      └──────────────┼───────────────┘                                                           │
  │ CLI/lib  │  JWT      │                 etcd-1 ◄─Raft─► etcd-2 ◄─Raft─► etcd-3   (usuarios, namespace, locks, versiones)  │
  └────┬─────┘           └──────────────▲────────────────────────────────────────────────────────────────────────────────┘
       │ capacidad firmada              │ mTLS: registro, latidos, confirm, reparación, GC
       │ (bloques directos)             │
       │            ┌───────────────────┴───────────── PLANO DE DATOS ─────────────────────────────┐
       └───────────►│  nodo-a        nodo-b        nodo-c      bloques cifrados AES-256-GCM        │
          TLS 1.3   │     └── réplica síncrona por mTLS (cliente → primario → réplica) ──┘           │
                    └───────────────────────────────────────────────────────────────────────────────┘
```

| Requisito | Dónde se resuelve |
|---|---|
| RF1 `ls cd mkdir rmdir rm` | `control/namespace.py`, `client/api.py`, `client/cli.py` |
| RF2 `put/get` (send/recv) | `client/api.py` (`put`, `get`), `control/files.py` |
| RF3 `open read write append close lock` | `client/handle.py`, `control/files.py` |
| RNF1/RNF4 particionamiento | `control/placement.py` (reparto round-robin con desplazamiento por archivo) |
| RNF2/RNF3 réplicas y consistencia | `datanode/app.py` (pipeline), `control/files.py` (commit atómico), `control/repair.py` |
| RNF5 concurrencia | lock exclusivo + versiones inmutables (`control/files.py`), transferencia paralela (`client/api.py`) |
| RNF6 seguridad | `control/auth.py`, `common/capabilities.py`, `common/tls.py`, `datanode/blockstore.py` |
| RNF7 transparencia | `control/nodes.py` (registro dinámico), el cliente solo conoce el control |

## 2. Puesta en marcha rápida (una sola máquina, procesos reales)

Requisitos: Python 3.11+, `openssl` y el binario de `etcd` (v3.5; `ETCD_BIN=/ruta/etcd` si no está en el PATH).

```bash
pip install -r requirements.txt
python scripts/local_cluster.py --dir run/demo          # 3 etcd + 3 control + 3 nodos, con TLS 1.3 y mTLS
```

El comando imprime las variables a exportar. En **otra terminal**:

```bash
export DFSHA_CONTROL=https://localhost:14100,https://localhost:14102,https://localhost:14104
export DFSHA_CA=$PWD/run/demo/certs/ca.crt
python -m client login admin                           # contraseña: admin-local-123
python -m client useradd ana --password clave-segura-1
python -m client --home /tmp/ana login ana --password clave-segura-1
python -m client --home /tmp/ana mkdir /docs
python -m client --home /tmp/ana put archivo_grande.iso /docs/archivo_grande.iso
python -m client --home /tmp/ana get /docs/archivo_grande.iso recuperado.iso
python -m client --home /tmp/ana shell                 # modo interactivo (cd, ls, put, get, ...)
```

Con Docker (todo en un host) o en las **4 VM de AWS Academy**: `docs/DESPLIEGUE_AWS.md`.

## 3. Pruebas

```bash
python -m pytest tests/unit -q          # unitarias (sin clúster), ~1 s
python -m pytest tests -q               # TODO: unitarias + e2e contra un clúster real (≈ 7 min)
python scripts/benchmark.py --quick     # rendimiento (E1–E7)
```

`docs/PRUEBAS.md` relaciona cada prueba mínima del enunciado con su prueba automática y su evidencia.

## 4. Estructura del repositorio

```
common/      contrato compartido: errores, capacidades HMAC, TLS, servidor de 2 listeners
control/     servicio de control: auth, namespace, locks, commit, líder, reparación, GC
datanode/    nodo de datos: bloques cifrados, replicación, latidos, reparación
client/      librería y CLI
deploy/      certificados, secretos, Dockerfile, docker-compose, despliegue AWS
scripts/     clúster local, benchmark
tests/       unitarias y e2e (cada archivo indica a qué parte pertenece)
docs/        diseño del código, decisiones, reparto, despliegue, pruebas
```

## 5. Puertos

| Componente | Público (TLS de servidor, clientes) | Interno (mTLS, S2S) |
|---|---|---|
| Control | 4430 (443 en AWS) | 4431 |
| Nodo de datos | 8443 | 8444 |
| etcd | – | 2379 (clientes de etcd) · 2380 (pares) |

## 6. Limitaciones conocidas

- Solo se tolera **un** fallo simultáneo de nodo de datos (R = 2) o de miembro de control/etcd (3 de 3).
- Sin mayoría de etcd el sistema rechaza aperturas y escrituras (decisión CP).
- Las escrituras por `FileHandle` se acumulan en memoria hasta `close()`; para archivos grandes use `put`.
- `truncate`, `rename`, bloqueos de rango y escrituras con huecos **no** están soportados (alcance acotado de RF3).
- El límite de intentos de login es por instancia de control (en memoria).
- El cifrado en reposo no protege ante un nodo comprometido en ejecución; la rotación de claves es manual.
- Las pruebas locales usan tiempos acortados (SUSPECT 4 s, DEAD 10 s); los valores por defecto son 15 s y 120 s.
