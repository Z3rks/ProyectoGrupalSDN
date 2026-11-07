# Despliegue en AWS Academy (4 VM)

> **Estado: probado en AWS Academy (3/11/2026)** con 4 instancias t3.medium (Ubuntu 24.04) en us-east-1: el clúster
> levantó con etcd de 3 miembros, y `put`/`get` de 64 MiB devolvió el mismo SHA-256. Las pruebas de fallos en AWS
> se documentan en `docs/PRUEBAS.md`.

## Topología

| VM | Corre | Puertos |
|---|---|---|
| VM-1, VM-2, VM-3 | `etcd-i` + `control-i` + `dn-i` (3 contenedores separados, red `host`) | etcd 2379/2380 · control 443 y 4431 · nodo 8443 y 8444 |
| VM-4 | Cliente (CLI), generación de certificados y secretos | saliente a 443 y 8443 |

Las cuatro van en la misma VPC y con el mismo security group. La CA (`ca.key`) vive **solo en VM-4**.

## Fase A. Consola de AWS

1. En AWS Academy: *Start Lab*, esperar el círculo verde y pulsar *AWS*. En *AWS Details* descargar la clave `labsuser.pem`.
2. **Security group** (EC2 → Security Groups → Create), nombre `dfsha-sg`, VPC por defecto. Reglas de entrada:
   - SSH (TCP 22) desde *My IP*.
   - SSH (TCP 22) desde `172.31.0.0/16` (necesario para que VM-4 reparta archivos a las otras VM).
   - TCP 443, 2379-2380, 4431 y 8443-8444 con origen `172.31.0.0/16` (el rango de la VPC por defecto).
   - Reglas de salida: dejar *Todo el tráfico* hacia `0.0.0.0/0` (si falta, las VM no pueden instalar Docker).
3. **Lanzar instancias** (EC2 → Launch instances):
   - Nombre `dfsha`, imagen **Ubuntu Server 24.04 LTS**, tipo **t3.medium**, par de claves **vockey**.
   - Red: grupo existente `dfsha-sg`; almacenamiento 20 GiB.
   - *Advanced details → User data*: pegar el contenido de `deploy/aws/user-data.sh`.
   - *Number of instances*: **4**.
4. Renombrar las instancias a `VM-1`, `VM-2`, `VM-3`, `VM-4` y anotar de cada una la **IP pública** (para SSH) y la **IP privada** (para el sistema).
5. Esperar unos 3 minutos. En cada VM, `ls ~` debe mostrar `LISTO.txt` y la carpeta `ProyectoGrupalSDN`.

## Fase B. Certificados y secretos (solo en VM-4)

Con `IP1`, `IP2`, `IP3` = IP privadas de VM-1, VM-2 y VM-3.

```bash
ssh -i labsuser.pem ubuntu@<IP_PUBLICA_VM4>
cd ~/ProyectoGrupalSDN
bash deploy/gen_certs.sh deploy/certs IP1,IP2,IP3
tar czf ~/certs.tgz -C deploy/certs --exclude=ca.key .

IP1=...; IP2=...; IP3=...
JWT=$(openssl rand -hex 32); CAP=$(openssl rand -hex 32); ADMIN=$(openssl rand -base64 18 | tr -d '=+/')
for i in 1 2 3; do
  eval "VMIP=\$IP$i"
  cat > ~/env.vm.$i <<EOF
NODE_INDEX=$i
VM_IP=$VMIP
IP1=$IP1
IP2=$IP2
IP3=$IP3
DFSHA_JWT_SECRET=$JWT
DFSHA_CAP_KEY=$CAP
DFSHA_ADMIN_PASSWORD=$ADMIN
DFSHA_MASTER_KEY=$(openssl rand -hex 32)
EOF
done
echo "$ADMIN" > ~/admin_password.txt && chmod 600 ~/admin_password.txt   # guárdela
```

Para repartir los archivos desde VM-4 hay que copiar allí la clave (borrarla al terminar). Desde su computador:
`scp -i labsuser.pem labsuser.pem ubuntu@<IP_PUBLICA_VM4>:~/`. Luego, en VM-4:

```bash
chmod 400 ~/labsuser.pem
for i in 1 2 3; do
  eval "VMIP=\$IP$i"
  scp -o StrictHostKeyChecking=no -i ~/labsuser.pem ~/certs.tgz ~/env.vm.$i ubuntu@$VMIP:~
done
```

## Fase C. Levantar el sistema (VM-1, VM-2 y VM-3)

Desde VM-4, en paralelo hacia las tres (los 3 etcd necesitan arrancar juntos; tarda de 3 a 8 minutos):

```bash
for i in 1 2 3; do
  eval "VMIP=\$IP$i"
  ssh -o StrictHostKeyChecking=no -i ~/labsuser.pem ubuntu@$VMIP "
    set -e
    cd ~/ProyectoGrupalSDN
    mkdir -p deploy/certs && tar xzf ~/certs.tgz -C deploy/certs
    cp ~/env.vm.$i deploy/aws/.env.vm
    cd deploy/aws
    sudo docker compose -f docker-compose.vm.yml --env-file .env.vm up -d --build
  " > ~/up.$i.log 2>&1 &
done
wait
```

Comprobación (siempre con `--env-file .env.vm`, o compose avisa de variables vacías):

```bash
for i in 1 2 3; do
  eval "VMIP=\$IP$i"; echo "== VM-$i"
  ssh -i ~/labsuser.pem ubuntu@$VMIP "cd ~/ProyectoGrupalSDN/deploy/aws && sudo docker compose -f docker-compose.vm.yml --env-file .env.vm ps"
done
```

Verificación de etcd (en cualquiera de las tres, con `i` y su IP privada):

```bash
sudo docker compose -f docker-compose.vm.yml --env-file .env.vm exec -T etcd etcdctl \
  --endpoints=https://<IP_PRIVADA>:2379 --cacert=/certs/ca.crt \
  --cert=/certs/etcd<i>.crt --key=/certs/etcd<i>.key member list
```

Deben aparecer 3 miembros. Los contenedores `control` pueden reiniciarse un par de veces hasta que etcd tenga quórum;
si siguen reiniciándose, `sudo docker compose -f docker-compose.vm.yml logs control`.

## Fase D. Cliente y prueba (VM-4)

```bash
cd ~/ProyectoGrupalSDN
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
export DFSHA_CONTROL=https://IP1,https://IP2,https://IP3
export DFSHA_CA=$HOME/ProyectoGrupalSDN/deploy/certs/ca.crt
python -m client login admin --password "$(cat ~/admin_password.txt)"
python -m client useradd ana --password clave-segura-1
python -m client --home ~/ana login ana --password clave-segura-1
head -c 64M /dev/urandom > big.bin && sha256sum big.bin
python -m client --home ~/ana put big.bin /big.bin
python -m client --home ~/ana get /big.bin big.out && sha256sum big.out
```

Los dos hashes deben coincidir. Después, en una VM: `sudo docker compose -f docker-compose.vm.yml stop datanode`,
repetir el `get` (debe seguir funcionando) y volver a levantarlo con `start datanode`.

## Fase E. Pruebas y benchmark en AWS

Las 7 pruebas están mapeadas en `docs/PRUEBAS.md`. Las pruebas automáticas levantan su propio clúster local; para
medir el despliegue real hay que repetir a mano las operaciones de la Fase D y los escenarios de fallo (detener
contenedores) y guardar capturas y registros. `scripts/benchmark.py` también levanta clústeres locales: ejecútelo en
VM-4 para obtener cifras de esa máquina, y mida el despliegue real con `put`/`get` cronometrados.

## Notas

- Las claves y los `.env.vm` nunca se suben a git (`.gitignore` los excluye). Borre `~/labsuser.pem` de VM-4 al terminar.
- Al terminar la sesión del laboratorio las instancias se detienen. Al reanudar, inícielas: las IP privadas se
  conservan y los contenedores se reinician solos (`restart: unless-stopped`); si no, repita `up -d` en la Fase C.
- Los tiempos por defecto (SUSPECT 15 s, DEAD 120 s) aplican en AWS; las pruebas locales usan valores más cortos.
- Las IP públicas cambian entre sesiones; las privadas no, y los certificados usan las privadas.
