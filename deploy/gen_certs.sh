#!/usr/bin/env bash
# Genera la CA interna y un certificado POR PROCESO (control, nodos de datos, etcd y cliente).
#
# Uso:   deploy/gen_certs.sh [carpeta_salida] [SANs_extra]
#   carpeta_salida  por defecto ./certs
#   SANs_extra      lista separada por comas con IPs/nombres adicionales por los que se
#                   accederá a los servicios, p. ej. las IPs privadas de las VM de AWS:
#                   deploy/gen_certs.sh certs 10.0.1.11,10.0.1.12,10.0.1.13
#
# Qué produce (en la carpeta de salida):
#   ca.crt / ca.key            CA privada del proyecto (ca.key NO se reparte)
#   <nombre>.crt / <nombre>.key  certificado de cada proceso, válido como servidor Y como cliente
#                               (necesario para mTLS entre servidores)
#
# Todos los certificados llevan los mismos nombres alternativos (SAN) para simplificar el
# prototipo; cada proceso tiene, eso sí, su propia clave privada.
set -euo pipefail

OUT="${1:-certs}"
EXTRA="${2:-}"
mkdir -p "$OUT"
cd "$OUT"

# --- CA -------------------------------------------------------------------------------
if [ ! -f ca.key ]; then
  openssl ecparam -name prime256v1 -genkey -noout -out ca.key
  openssl req -x509 -new -key ca.key -sha256 -days 825 -subj "/CN=DFSha Internal CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -out ca.crt
fi

# --- nombres (SAN) válidos para todos los certificados -----------------------------------
SAN="DNS:localhost,IP:127.0.0.1,DNS:control.dfsha"
NAMES="client"
for i in 1 2 3 4 5; do
  SAN="$SAN,DNS:control$i,DNS:dn$i,DNS:etcd$i"
  NAMES="$NAMES control$i dn$i etcd$i"
done
IFS=',' read -ra EXTRA_LIST <<< "$EXTRA"
for x in "${EXTRA_LIST[@]:-}"; do
  [ -z "$x" ] && continue
  if [[ "$x" =~ ^[0-9.]+$ ]]; then SAN="$SAN,IP:$x"; else SAN="$SAN,DNS:$x"; fi
done

# --- un certificado por proceso ------------------------------------------------------------
for name in $NAMES; do
  [ -f "$name.crt" ] && continue
  openssl ecparam -name prime256v1 -genkey -noout -out "$name.key"
  openssl req -new -key "$name.key" -subj "/CN=$name" -out "$name.csr"
  printf "subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature\n" "$SAN" > "$name.ext"
  openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out "$name.crt" -days 825 -sha256 -extfile "$name.ext" 2>/dev/null
  rm -f "$name.csr" "$name.ext"
done
chmod 600 ./*.key
echo "Certificados generados en $(pwd)"
