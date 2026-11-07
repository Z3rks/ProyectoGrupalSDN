#!/bin/bash
# Script de arranque (user data) para las 4 VM de AWS Academy (Ubuntu Server 24.04 LTS).
# Se pega en "Advanced details > User data" al lanzar las instancias; AWS lo ejecuta una vez, como root,
# en el primer arranque. Instala Docker y clona el repositorio en /home/ubuntu/ProyectoGrupalSDN.
# Progreso:  sudo tail -f /var/log/cloud-init-output.log      Fin:  cloud-init status --wait
set -eux
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y ca-certificates curl git openssl docker.io docker-compose-v2 python3-venv python3-pip
systemctl enable --now docker
usermod -aG docker ubuntu
sudo -u ubuntu git clone https://github.com/Z3rks/ProyectoGrupalSDN.git /home/ubuntu/ProyectoGrupalSDN
echo "dfsha-listo" > /home/ubuntu/LISTO.txt
