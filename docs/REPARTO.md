# Reparto del trabajo en 2 partes (una por integrante)

El proyecto se dividió en dos partes **sin ningún archivo en común**: cada integrante sube las suyas y al juntar
ambas ramas no hay conflictos de merge. El criterio fue que cada parte sea **demostrable por sí sola** y que las dos
tengan carga y dificultad parecidas.

| | **PARTE 1 · Plano de control y consistencia** | **PARTE 2 · Plano de datos, cliente, despliegue y pruebas** |
|---|---|---|
| Idea | "El cerebro": quién eres, dónde está cada cosa, quién puede escribir, qué versión es la vigente. | "Los músculos": guardar/servir bloques cifrados, replicarlos, el cliente, desplegar y medir. |
| Líneas | ≈ 1.800 (código) + pruebas | ≈ 2.100 (código y scripts) + pruebas |
| Rúbrica que cubre | Implementación RF1–RF3 (servidor) · Alta disponibilidad y consistencia · Seguridad de acceso | Distribución y comunicaciones · Replicación y fallos · Seguridad de canal/reposo · Rendimiento · Despliegue |

## Archivos de cada parte

### PARTE 1 — Integrante A

```
common/__init__.py  config.py  errors.py  hashing.py  capabilities.py  tls.py  server.py
control/__init__.py  settings.py  state.py  etcd.py  paths.py  placement.py  auth.py  nodes.py  blocks.py
        namespace.py  files.py  leader.py  repair.py  gc.py  schemas.py  routes_public.py  routes_internal.py
        app.py  main.py
tests/unit/test_common_and_control_pure.py
tests/e2e/test_part1_rf1_namespace.py  test_part1_security_access.py  test_part1_locks_concurrency.py  test_part1_control_ha.py
requirements.txt  pytest.ini  .gitignore  README.md
docs/REPARTO.md  docs/DECISIONES.md  docs/CODIGO_PARTE1_CONTROL.md
```

### PARTE 2 — Integrante B

```
datanode/__init__.py  settings.py  blockstore.py  app.py  main.py
client/__init__.py  errors.py  api.py  handle.py  cli.py  __main__.py
deploy/gen_certs.sh  gen_secrets.sh  Dockerfile  docker-compose.yml  aws/docker-compose.vm.yml  aws/.env.vm.example
scripts/local_cluster.py  benchmark.py  make_parts.py
tests/__init__.py  conftest.py  unit/__init__.py  e2e/__init__.py
tests/unit/test_blockstore.py
tests/e2e/test_part2_rf2_integrity.py  test_part2_rf3_semantics.py  test_part2_replication_faults.py
          test_part2_consistency_writes.py  test_part2_security_channel.py  test_part2_performance.py
docs/CODIGO_PARTE2_DATOS_CLIENTE.md  docs/DESPLIEGUE_AWS.md  docs/PRUEBAS.md
```

## Dependencias entre partes (importante para el orden de subida)

- La Parte 2 **importa** `common/` (Parte 1): el nodo de datos usa los errores, las capacidades y el servidor TLS.
- Las pruebas e2e de la Parte 1 **usan** el lanzador de clúster y el cliente (Parte 2).
- Por eso: **primero se sube la Parte 1** (rama base) y luego la Parte 2. Las pruebas unitarias de cada parte corren
  con solo su parte (más `common/` para la Parte 2); las e2e, con ambas juntas.

## Cómo subir cada parte a git

Los archivos ya están separados. `python scripts/make_parts.py` genera `entrega/parte1_control.zip` y
`entrega/parte2_datos_cliente.zip` con **las mismas rutas relativas** del repositorio; también se pueden copiar a mano
con las listas de arriba.

**Integrante A (Parte 1)** — crea el repositorio y la rama base:

```bash
git clone <url-del-repo> dfsha && cd dfsha
git checkout -b parte-1-control
unzip -o ../parte1_control.zip                       # o copie los archivos de la lista
python -m pytest tests/unit -q                        # debe dar 21 pruebas en verde con solo common/ y control/
git add common control tests/unit/test_common_and_control_pure.py tests/e2e/test_part1_*.py \
        requirements.txt pytest.ini .gitignore README.md docs/REPARTO.md docs/DECISIONES.md docs/CODIGO_PARTE1_CONTROL.md
git commit -m "Parte 1: plano de control (auth, namespace, locks, commit atómico, líder, reparación, GC)"
git push -u origin parte-1-control
# abra un Pull Request hacia main y fusiónelo ANTES que la Parte 2
```

**Integrante B (Parte 2)** — parte de la rama de A (o de `main` ya fusionada):

```bash
git fetch && git checkout -b parte-2-datos-cliente origin/parte-1-control    # (o main, si ya se fusionó)
unzip -o ../parte2_datos_cliente.zip
python -m pytest tests -q                             # ahora sí: unitarias + e2e contra el clúster real
git add datanode client deploy scripts tests/__init__.py tests/conftest.py tests/unit/__init__.py tests/unit/test_blockstore.py \
        tests/e2e/__init__.py tests/e2e/test_part2_*.py docs/CODIGO_PARTE2_DATOS_CLIENTE.md docs/DESPLIEGUE_AWS.md docs/PRUEBAS.md
git commit -m "Parte 2: nodos de datos cifrados, replicación, cliente/CLI, despliegue y pruebas"
git push -u origin parte-2-datos-cliente
```

Los commits quedan **a nombre de cada integrante** (usa su `git config user.name`), que es lo que pide el enunciado
("contribuciones identificables"). No hagan commits con la cuenta del otro.

## Qué debe poder explicar cada uno de la parte del otro (defensa oral)

El profesor puede preguntar a cualquiera sobre **toda** la arquitectura. Mínimo imprescindible:

| Integrante A (Parte 1) debe saber explicar también… | Integrante B (Parte 2) debe saber explicar también… |
|---|---|
| El formato del bloque en disco y por qué el SHA-256 va en la cabecera (`datanode/blockstore.py`). | Qué claves de etcd existen y qué hace cada transacción (`docs/CODIGO_PARTE1_CONTROL.md`, tabla de claves). |
| El pipeline cliente → primario → réplica y cuándo se responde 201 (`datanode/app.py`). | Por qué el commit es atómico y qué es el *fencing* (`control/files.py::_commit`). |
| Cómo el cliente reasigna un bloque cuando un nodo falla (`client/api.py::_upload_block`). | Cómo se elige el líder y qué hace (`control/leader.py`, `repair.py`, `gc.py`). |
| Cómo se despliega en las 4 VM y qué abre cada *security group* (`docs/DESPLIEGUE_AWS.md`). | Qué garantiza cada G1–G6 y qué escenario de las pruebas la demuestra (`docs/PRUEBAS.md`). |

## Contribuciones por parte (para el informe)

Cada integrante documenta en el informe final sus decisiones propias. Sugerencia de reparto del informe:
Parte 1 → secciones *arquitectura, metadatos, consistencia, concurrencia, seguridad de acceso*;
Parte 2 → secciones *plano de datos, replicación y fallos, despliegue, pruebas y rendimiento*.
