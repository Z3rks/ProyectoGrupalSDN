#!/usr/bin/env bash
# Genera el archivo .env con los SECRETOS del despliegue (no se sube a git: está en .gitignore).
#
#   DFSHA_JWT_SECRET        firma de los JWT (compartido por las 3 instancias de control)
#   DFSHA_CAP_KEY           clave HMAC de las capacidades (control + nodos de datos)
#   DFSHA_ADMIN_PASSWORD    contraseña del administrador inicial
#   DFSHA_MASTER_KEY_1..3   clave AES-256 de cifrado en reposo de cada nodo de datos
#
# Uso:   deploy/gen_secrets.sh [archivo_salida]     (por defecto deploy/.env)
set -euo pipefail
OUT="${1:-$(dirname "$0")/.env}"
if [ -f "$OUT" ]; then echo "$OUT ya existe; bórrelo si quiere regenerarlo"; exit 0; fi
{
  echo "DFSHA_JWT_SECRET=$(openssl rand -hex 32)"
  echo "DFSHA_CAP_KEY=$(openssl rand -hex 32)"
  echo "DFSHA_ADMIN_PASSWORD=$(openssl rand -base64 18 | tr -d '=+/')"
  for i in 1 2 3; do echo "DFSHA_MASTER_KEY_$i=$(openssl rand -hex 32)"; done
} > "$OUT"
chmod 600 "$OUT"
echo "Secretos escritos en $OUT"
