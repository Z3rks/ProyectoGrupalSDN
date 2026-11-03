"""Replicación y fallos (PARTE 2).

Prueba mínima del enunciado: "Caída de un nodo y lectura o recuperación según las garantías prometidas".
Todas usan `restore`: al terminar dejan el clúster completo y sano.
"""
import hashlib
import signal

import httpx

from tests.conftest import blocks_on_disk, manifest_of, sha256_file, uniq


def _estado(cluster, node_id):
    return {n["node_id"]: n["state"] for n in cluster.status()["nodes"]}.get(node_id)


def test_get_sigue_funcionando_con_un_nodo_caido(restore, alice, tmpfile, tmp_path):
    cluster = restore
    path, sha = tmpfile(1_500_000)
    f = "/" + uniq("caida-get")
    alice.put(path, f)
    cluster.kill("dn-b")                                          # cae SIN avisar (SIGKILL)
    out = str(tmp_path / "o.bin")
    assert alice.get(f, out)["sha256"] == sha == sha256_file(out)  # cada bloque tiene otra réplica


def test_put_con_un_nodo_caido_reasigna_los_bloques_a_nodos_vivos(restore, alice, tmpfile, tmp_path):
    cluster = restore
    cluster.kill("dn-c")                                          # el control aún lo cree HEALTHY unos segundos
    path, sha = tmpfile(2_000_000)
    f = "/" + uniq("caida-put")
    alice.put(path, f)                                            # el cliente excluye al nodo fallido y reintenta
    for b in manifest_of(alice, f):
        assert {n["node_id"] for n in b["nodes"]} == {"dn-a", "dn-b"}
    out = str(tmp_path / "o.bin")
    assert alice.get(f, out)["sha256"] == sha


def test_sin_dos_nodos_sanos_se_rechaza_escribir_pero_no_se_pierde_lo_confirmado(restore, alice, tmpfile, tmp_path):
    """Decisión CP: prefiere rechazar antes que confirmar con una sola copia."""
    import pytest
    from client import DFShaClientError
    cluster = restore
    path, sha = tmpfile(600_000)
    f = "/" + uniq("cp-datos")
    alice.put(path, f)
    cluster.kill("dn-a"); cluster.kill("dn-b")
    cluster.wait_for(lambda: _estado(cluster, "dn-a") != "HEALTHY" and _estado(cluster, "dn-b") != "HEALTHY", timeout=30,
                     what="que el control detecte los dos nodos como no sanos")
    with pytest.raises(DFShaClientError) as e:
        alice.put(path, "/" + uniq("rechazado"))
    assert e.value.status == 503 and e.value.retryable
    cluster.start("dn-a"); cluster.start("dn-b")
    cluster.wait_nodes(3, timeout=60)
    assert alice.get(f, str(tmp_path / "o.bin"))["sha256"] == sha   # lo confirmado antes sigue intacto


def test_detecta_nodo_muerto_repara_replicas_y_resiste_una_segunda_caida(restore, alice, tmpfile, tmp_path):
    cluster = restore
    path, sha = tmpfile(2_000_000)
    f = "/" + uniq("repara")
    alice.put(path, f)
    cluster.kill("dn-b")

    # HEALTHY -> SUSPECT -> DEAD (tiempos de prueba: 4 s y 10 s; en producción 15 s y 120 s)
    cluster.wait_for(lambda: _estado(cluster, "dn-b") == "SUSPECT", timeout=30, what="estado SUSPECT")
    cluster.wait_for(lambda: _estado(cluster, "dn-b") == "DEAD", timeout=40, what="estado DEAD")

    # el líder repara: todo bloque vuelve a tener 2 copias, ninguna en el nodo muerto
    def reparado():
        return all(len({n["node_id"] for n in b["nodes"]}) == 2 and "dn-b" not in {n["node_id"] for n in b["nodes"]}
                   for b in manifest_of(alice, f))
    cluster.wait_for(reparado, timeout=90, what="que se restablezca R=2 sin usar el nodo muerto")

    # prueba definitiva: ahora puede caer OTRO nodo y el archivo sigue disponible
    cluster.kill("dn-a")
    out = str(tmp_path / "o.bin")
    assert alice.get(f, out)["sha256"] == sha


def test_nodo_que_reaparece_se_reconcilia_y_descarta_copias_excedentes(restore, alice, tmpfile):
    cluster = restore
    path, _ = tmpfile(1_500_000)
    f = "/" + uniq("reaparece")
    alice.put(path, f)
    ids = {b["block_id"] for b in manifest_of(alice, f)}
    cluster.kill("dn-b")
    cluster.wait_for(lambda: _estado(cluster, "dn-b") == "DEAD", timeout=40, what="DEAD")
    cluster.wait_for(lambda: all("dn-b" not in {n["node_id"] for n in b["nodes"]} and len(b["nodes"]) == 2
                                 for b in manifest_of(alice, f)), timeout=90, what="reparación")
    cluster.start("dn-b")                                          # vuelve con su disco intacto
    cluster.wait_nodes(3, timeout=60)
    # su block-report muestra que esas copias ya sobran (R=2 ya se cumple en otros nodos) y las descarta
    cluster.wait_for(lambda: not (ids & blocks_on_disk(cluster)["dn-b"]), timeout=60,
                     what="que dn-b descarte las copias excedentes")


def test_corrupcion_en_reposo_se_detecta_no_se_sirve_y_se_repara(restore, alice, tmpfile, tmp_path):
    cluster = restore
    path, sha = tmpfile(800_000)
    f = "/" + uniq("corrupto")
    alice.put(path, f)
    blk = manifest_of(alice, f)[0]
    victima = blk["nodes"][0]
    archivos = list((cluster.dir / "data" / victima["node_id"] / "blocks").rglob(blk["block_id"] + ".blk"))
    assert len(archivos) == 1
    raw = bytearray(archivos[0].read_bytes()); raw[200] ^= 0xFF; archivos[0].write_bytes(raw)   # un bit corrupto

    # 1) el nodo víctima NO sirve contenido corrupto (la etiqueta GCM falla)
    r = httpx.get(f"{victima['endpoint']}/v1/blocks/{blk['block_id']}", headers={"Authorization": "Capability " + victima["capability"]},
                  verify=str(cluster.certs / "ca.crt"))
    assert r.status_code == 422
    # 2) aun así el cliente recupera el archivo completo y correcto desde la otra réplica
    assert alice.get(f, str(tmp_path / "o.bin"))["sha256"] == sha
    # 3) el nodo lo reporta en su heartbeat y el líder lo repara: vuelven a existir 2 copias VÁLIDAS
    def sano():
        b = [x for x in manifest_of(alice, f) if x["block_id"] == blk["block_id"]][0]
        if len(b["nodes"]) != 2:
            return False
        for n in b["nodes"]:
            resp = httpx.get(f"{n['endpoint']}/v1/blocks/{b['block_id']}", headers={"Authorization": "Capability " + n["capability"]},
                             verify=str(cluster.certs / "ca.crt"))
            if resp.status_code != 200 or hashlib.sha256(resp.content).hexdigest() != b["sha256"]:
                return False
        return True
    cluster.wait_for(sano, timeout=90, what="reparación de la copia corrupta")
