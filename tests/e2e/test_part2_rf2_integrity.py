"""RF2 (PARTE 2): transferencia de archivos completos, particionamiento y recuperación idéntica (hash).

Prueba mínima del enunciado: "Archivo grande particionado en varios nodos; archivo recuperado idéntico (hash)".
"""
import pytest

from tests.conftest import BLOCK, blocks_on_disk, manifest_of, sha256_file, uniq


@pytest.mark.parametrize("size", [0, 1, BLOCK - 1, BLOCK, BLOCK + 1, 1_500_000, 5_000_000])
def test_put_get_recupera_el_archivo_identico(alice, tmpfile, tmp_path, size):
    path, sha = tmpfile(size)
    f = "/" + uniq("rf2")
    r = alice.put(path, f)
    assert r["sha256"] == sha and r["size"] == size and r["version"] == 1
    assert alice.stat(f)["size"] == size
    out = str(tmp_path / "recuperado.bin")
    g = alice.get(f, out)
    assert g["sha256"] == sha == sha256_file(out)


def test_el_archivo_queda_particionado_entre_varios_nodos_con_dos_copias(cluster, alice, tmpfile):
    path, _ = tmpfile(2_000_000)                                  # 8 bloques de 256 KiB
    f = "/" + uniq("part")
    alice.put(path, f)
    blocks = manifest_of(alice, f)
    assert len(blocks) == 8
    usados = set()
    for b in blocks:
        ids = {n["node_id"] for n in b["nodes"]}
        assert len(ids) == 2, "cada bloque debe estar en dos nodos DISTINTOS"
        usados |= ids
    assert usados == {"dn-a", "dn-b", "dn-c"}, "los bloques de un mismo archivo deben repartirse entre todos los nodos"

    en_disco = blocks_on_disk(cluster)                            # evidencia física, mirando los discos
    for b in blocks:
        assert sum(b["block_id"] in en_disco[n] for n in en_disco) == 2


def test_sobrescribir_crea_version_nueva_y_la_anterior_se_libera(cluster, alice, tmpfile, tmp_path):
    p1, _ = tmpfile(900_000, "v1.bin")
    p2, sha2 = tmpfile(700_000, "v2.bin")
    f = "/" + uniq("ver")
    alice.put(p1, f)
    viejos = {b["block_id"] for b in manifest_of(alice, f)}
    assert alice.put(p2, f)["version"] == 2
    out = str(tmp_path / "o.bin")
    assert alice.get(f, out)["sha256"] == sha2
    cluster.wait_for(lambda: not (viejos & set().union(*blocks_on_disk(cluster).values())), timeout=90,
                     what="que el GC libere los bloques de la versión reemplazada")


def test_rm_libera_los_bloques_fisicos(cluster, alice, tmpfile):
    path, _ = tmpfile(1_000_000)
    f = "/" + uniq("gc")
    alice.put(path, f)
    ids = {b["block_id"] for b in manifest_of(alice, f)}
    assert ids & set().union(*blocks_on_disk(cluster).values())
    alice.rm(f)
    cluster.wait_for(lambda: not (ids & set().union(*blocks_on_disk(cluster).values())), timeout=90,
                     what="que el GC elimine los bloques del archivo borrado")


def test_descubrimiento_dinamico_el_cliente_solo_conoce_el_control(cluster, alice, tmpfile):
    """RNF7: las direcciones de los nodos vienen del manifiesto, no de la configuración del cliente."""
    assert alice.control == cluster.control_urls                            # la configuración del cliente = solo el control
    path, _ = tmpfile(300_000)
    f = "/" + uniq("disc")
    alice.put(path, f)
    puertos_nodos = {f"https://localhost:{cluster.node_public(i)}" for i in range(3)}
    vistos = set()
    for b in manifest_of(alice, f):
        for n in b["nodes"]:
            assert n["endpoint"] not in alice.control                       # ninguna dirección de nodo está en el cliente...
            assert n["capability"]
            vistos.add(n["endpoint"])
    assert vistos == puertos_nodos                                          # ...las descubrió en el manifiesto (registro dinámico)
