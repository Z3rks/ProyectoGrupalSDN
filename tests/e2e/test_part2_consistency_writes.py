"""Consistencia y escrituras interrumpidas (PARTE 2): escenarios 8.2 y 8.3 del diseño.

Prueba mínima del enunciado: "Comportamiento observable cuando una réplica se retrasa o desconecta".
"""
import hashlib
import os
import signal
import threading

import httpx
import pytest

from client import DFShaClientError
from tests.conftest import BLOCK, blocks_on_disk, manifest_of, uniq


def _otro_cliente_de(cluster, user, **kw):
    """Segundo cliente con la MISMA sesión (mismo usuario) pero otros parámetros (p. ej. timeouts)."""
    return cluster.client(home=str(cluster.dir / "home" / user), **kw)


def test_replica_desconectada_durante_escritura_lectores_ven_la_version_anterior_completa(restore, tmpfile, cluster):
    """Escenario 8.2: congelamos un nodo (SIGSTOP) a mitad de una escritura."""
    cluster = restore
    alice = cluster.make_user("alice")
    escritor = _otro_cliente_de(cluster, "alice", io_timeout=6)
    v1, v2 = os.urandom(1_200_000), os.urandom(1_200_000)
    f = "/" + uniq("replica")
    with alice.open(f, "w", create=True) as h:
        h.write(0, v1)

    lector = alice.open(f, "r")                                   # un lector con la v1 abierta
    cluster.procs["dn-b"].send_signal(signal.SIGSTOP)             # dn-b deja de responder (red caída / proceso congelado)
    resultado = {}

    def escribir():
        try:
            with escritor.open(f, "w", truncate=True) as h:
                h.write(0, v2)
            resultado["ok"] = True
        except Exception as exc:                                  # pragma: no cover
            resultado["error"] = exc

    t = threading.Thread(target=escribir); t.start()
    # MIENTRAS la escritura está en curso (esperando timeouts), los lectores solo ven la versión anterior COMPLETA
    en_curso = t.is_alive()
    assert en_curso, "la escritura debía seguir en curso (esperando al nodo congelado)"
    assert lector.read(0, len(v1)) == v1                          # (si lee del nodo congelado, cae a la otra réplica a los 3 s)
    with alice.open(f, "r") as h2:
        # atomicidad (G3): o la v1 completa o la v2 completa, jamás una mezcla de bloques
        assert (h2.version, h2.read(0, len(v1))) in ((1, v1), (2, v2))
    t.join(120)
    assert resultado.get("ok"), resultado

    cluster.procs["dn-b"].send_signal(signal.SIGCONT)
    # la escritura se completó SIN el nodo congelado: la v2 está solo en nodos que sí confirmaron
    for b in manifest_of(alice, f):
        assert {n["node_id"] for n in b["nodes"]} == {"dn-a", "dn-c"}
    with alice.open(f, "r") as h3:
        assert h3.version == 2 and h3.read(0, len(v2)) == v2
    assert lector.read(0, len(v1)) == v1                          # el snapshot antiguo sigue intacto (G5)
    lector.close()


def test_bloque_incompleto_o_corrupto_no_se_guarda_y_el_archivo_no_se_publica(cluster, alice):
    """Escenario 8.3: un nodo recibe solo parte del bloque -> nunca se llega a publicar un archivo incompleto."""
    f = "/" + uniq("parcial")
    h = alice.open(f, "w", create=True)
    alloc = alice._allocate(h.id, [0])[0]
    url = f"{alloc['primary']['endpoint']}/v1/blocks/{alloc['block_id']}"
    cabeceras = {"Authorization": "Capability " + alloc["capability"], "Content-Type": "application/octet-stream"}
    ca = str(cluster.certs / "ca.crt")
    parte = os.urandom(100_000)

    # (a) se declara un largo mayor al realmente recibido (conexión cortada a mitad)
    r = httpx.put(url, content=parte, verify=ca, headers={**cabeceras, "X-Block-Length": str(BLOCK), "X-Block-SHA256": hashlib.sha256(parte).hexdigest()})
    assert r.status_code == 400
    # (b) el largo cuadra pero el hash no (contenido alterado en tránsito)
    r = httpx.put(url, content=parte, verify=ca, headers={**cabeceras, "X-Block-Length": str(len(parte)), "X-Block-SHA256": "0" * 64})
    assert r.status_code == 422 and r.json()["code"] == "INTEGRITY_CHECK_FAILED"

    assert alloc["block_id"] not in set().union(*blocks_on_disk(cluster).values())   # no quedó nada STORED
    with pytest.raises(DFShaClientError) as e:
        alice._control("POST", f"/v1/handles/{h.id}/close", json_body={"commit": True, "size": len(parte)})
    assert e.value.status == 422 and e.value.code == "INCOMPLETE_FILE"
    with pytest.raises(DFShaClientError) as e2:
        alice.stat(f)
    assert e2.value.status == 404                                  # el archivo NO existe: nunca se publicó
    h.abort()


def test_commit_exige_que_todos_los_bloques_esten_confirmados(alice):
    f = "/" + uniq("falta")
    h = alice.open(f, "w", create=True)
    alloc = alice._allocate(h.id, [0, 1])
    data = os.urandom(BLOCK)
    alice._upload_block(h.id, 0, data, alloc[0])                    # sube solo el bloque 0 de 2
    with pytest.raises(DFShaClientError) as e:
        alice._control("POST", f"/v1/handles/{h.id}/close", json_body={"commit": True, "size": 2 * BLOCK})
    assert e.value.code == "INCOMPLETE_FILE"
    h.abort()


def test_cliente_muere_antes_del_close_no_se_publica_nada_y_se_limpian_los_huerfanos(cluster, alice):
    f = "/" + uniq("muerto")
    h = alice.open(f, "w", create=True)
    allocs = alice._allocate(h.id, [0, 1, 2])
    ids = []
    for a in allocs:
        ids.append(alice._upload_block(h.id, a["index"], os.urandom(BLOCK), a))
    assert set(ids) <= set().union(*blocks_on_disk(cluster).values())     # los bloques sí están en disco...
    h._stop.set()                                                          # ...y el cliente "muere": ni renueva ni cierra

    # el lock (8 s) y el handle (20 s) vencen solos; otro cliente puede tomar el archivo
    def libre():
        try:
            alice.open(f, "w", create=True).abort()
            return True
        except DFShaClientError:
            return False
    cluster.wait_for(libre, timeout=60, what="vencimiento del lock del cliente muerto")
    with pytest.raises(DFShaClientError) as e:
        alice.stat(f)
    assert e.value.status == 404                                           # nunca se publicó una versión
    # el GC elimina los bloques huérfanos de los discos
    cluster.wait_for(lambda: not (set(ids) & set().union(*blocks_on_disk(cluster).values())), timeout=120,
                     what="que el GC limpie los bloques de la escritura abandonada")


def test_reintento_del_close_con_la_misma_idempotency_key_devuelve_el_mismo_resultado(alice):
    f = "/" + uniq("idem")
    h = alice.open(f, "w", create=True)
    h.write(0, b"datos")
    h._stop.set()
    # subimos bloques manualmente para poder repetir el close con la MISMA clave
    for a in alice._allocate(h.id, [0]):
        alice._upload_block(h.id, 0, bytes(h._dirty[0]), a)
    hdr = {"Authorization": "Bearer " + alice.session["access_token"], "Idempotency-Key": "op-repetida-123"}
    url = alice.control[0] + f"/v1/handles/{h.id}/close"
    body = {"commit": True, "size": 5}
    r1 = alice.http.post(url, json=body, headers=hdr)
    r2 = alice.http.post(url, json=body, headers=hdr)               # el primero ya publicó; el segundo repite la respuesta
    assert r1.status_code == r2.status_code == 200 and r1.json() == r2.json()
    assert alice.stat(f)["version"] == 1                              # y NO se publicó una versión 2
