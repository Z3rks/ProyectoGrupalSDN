# Despliegue en AWS Academy (4 VM)

> **Estado: sin probar.** Los archivos de `deploy/` no se ejecutaron (el entorno de desarrollo no tenía Docker).
> Lo que sí se probó es el mismo software como procesos reales con TLS en una sola máquina (`scripts/local_cluster.py`).
> Hagan una primera pasada del despliegue con tiempo y corrijan lo que falle antes de grabar el video.

## Topología

| VM | Corre | Puertos |
|---|---|---|
| VM-1, VM-2, VM-3 | `etcd-i` + `control-i` + `dn-i` (3 contenedores separados) | etcd 2379/2380 · control 443 y 4431 · nodo 8443 y 8444 |
| VM-4 | Solo el cliente (CLI) | saliente a 443 y 8443 |

Todas en la misma VPC. Los contenedores usan red `host`, así que usan las IPs privadas de la VM.

## Security groups

- Entre VM-1/2/3: permitir TCP 2379, 2380, 4431, 8444 (tráfico interno; solo desde el SG propio).
- Desde VM-4 hacia VM-1/2/3: TCP 443 y 8443.
- Nada abierto a Internet salvo SSH desde su IP.

## Pasos

1. En cada VM instalar Docker y Docker Compose, y clonar el repositorio (rama con ambas partes fusionadas).
2. **Una vez**, en una sola máquina: `bash deploy/gen_certs.sh certs IP1,IP2,IP3` (las 3 IPs privadas como SAN extra) y copiar `certs/` a las 4 VM **sin** `ca.key` (la clave de la CA no se reparte).
3. En cada VM: `cp deploy/aws/.env.vm.example deploy/aws/.env.vm` y completar:
   - `NODE_INDEX` (1, 2 o 3), `VM_IP`, `IP1`, `IP2`, `IP3`.
   - `DFSHA_JWT_SECRET`, `DFSHA_CAP_KEY`, `DFSHA_ADMIN_PASSWORD`: **iguales** en las tres (`openssl rand -hex 32`).
   - `DFSHA_MASTER_KEY`: **distinta** en cada VM (64 hex).
4. En VM-1, VM-2 y VM-3: `docker compose -f deploy/aws/docker-compose.vm.yml --env-file deploy/aws/.env.vm up -d --build`.
5. Verificar que los 3 miembros de etcd forman clúster (`etcdctl member list`) y que los tres servicios de control aceptan conexiones TLS en el puerto 443.
6. En VM-4: `export DFSHA_CONTROL=https://IP1,https://IP2,https://IP3`, `export DFSHA_CA=<ruta>/ca.crt`, y usar `python -m client ...`.

## Verificación mínima tras desplegar

`login`, `mkdir`, `put` de un archivo de 64 MiB, `get` y comparar `sha256sum`; luego detener el contenedor `datanode` de una VM y repetir `get`.

## Notas

- Las claves nunca se suben a git (`.gitignore` excluye `.env.vm` y `certs/`).
- Los tiempos por defecto (SUSPECT 15 s, DEAD 120 s) aplican en AWS; las pruebas locales usan valores más cortos.
- AWS Academy suele tener sesiones limitadas: guarden las configuraciones antes de que expire el laboratorio.
