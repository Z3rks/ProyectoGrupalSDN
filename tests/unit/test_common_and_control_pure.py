"""Pruebas unitarias de funciones puras (PARTE 1): rutas, colocación y capacidades. No requieren clúster."""
import time

import pytest

from common import capabilities
from common.errors import DFShaError
from control import paths, placement


# ---------------------------------------------------------------- rutas
def test_normalize_resuelve_puntos_y_dobles_barras():
    assert paths.normalize("/a/./b/../c//d") == "/a/c/d"
    assert paths.normalize("/") == "/"


@pytest.mark.parametrize("bad", ["relativa/x", "/../escape", "/a/\x00b", "/a|b", "/" + "x" * 5000, "/" + "y" * 300])
def test_normalize_rechaza_rutas_peligrosas(bad):
    with pytest.raises(DFShaError) as e:
        paths.normalize(bad)
    assert e.value.status == 400


def test_clave_del_namespace_agrupa_hijos_por_directorio():
    assert paths.ns_key("ana", "/docs/informe.pdf") == "/ns/ana:/docs|informe.pdf"
    assert paths.ns_key("ana", "/leeme.txt") == "/ns/ana:/|leeme.txt"
    assert paths.ns_children_prefix("ana", "/docs") == "/ns/ana:/docs|"
    # el prefijo de /docs NO incluye los hijos de /docs/sub (cada directorio tiene su propio prefijo)
    assert not paths.ns_key("ana", "/docs/sub/x").startswith(paths.ns_children_prefix("ana", "/docs"))


def test_la_raiz_no_tiene_padre():
    with pytest.raises(DFShaError):
        paths.split("/")


# ---------------------------------------------------------------- colocación
def test_replica_siempre_en_nodo_distinto_del_primario():
    nodes = ["dn-a", "dn-b", "dn-c"]
    for fid in ("f-1", "f-2", "f-3"):
        for idx in range(20):
            p, r = placement.choose(nodes, fid, idx, 2)
            assert p != r


def test_reparto_cclico_usa_todos_los_nodos_como_primario():
    nodes = ["dn-a", "dn-b", "dn-c"]
    primaries = {placement.choose(nodes, "f-x", i, 2)[0] for i in range(6)}
    assert primaries == set(nodes)


def test_archivos_distintos_empiezan_en_nodos_distintos():
    nodes = ["dn-a", "dn-b", "dn-c"]
    firsts = {placement.choose(nodes, f"f-{i}", 0, 2)[0] for i in range(30)}
    assert len(firsts) == 3   # los archivos de 1 bloque no cargan siempre el mismo nodo


def test_colocacion_es_independiente_del_orden_de_la_lista():
    assert placement.choose(["dn-c", "dn-a", "dn-b"], "f-1", 3) == placement.choose(["dn-a", "dn-b", "dn-c"], "f-1", 3)


def test_sin_suficientes_nodos_no_hay_colocacion():
    with pytest.raises(ValueError):
        placement.choose(["dn-a"], "f-1", 0, 2)


# ---------------------------------------------------------------- capacidades
KEY = b"clave-de-prueba"


def _claims(**over):
    base = {"op": "put", "uid": "ana", "block_id": "b-1", "node_id": "dn-a"}
    base.update(over)
    return base


def test_capacidad_valida_se_acepta():
    tok = capabilities.sign(KEY, _claims())
    c = capabilities.verify(KEY, tok, op="put", block_id="b-1", node_id="dn-a")
    assert c["uid"] == "ana" and "jti" in c


@pytest.mark.parametrize("kw", [dict(op="get"), dict(block_id="b-2"), dict(node_id="dn-b")])
def test_capacidad_para_otra_operacion_bloque_o_nodo_se_rechaza(kw):
    tok = capabilities.sign(KEY, _claims())
    args = dict(op="put", block_id="b-1", node_id="dn-a") | kw
    with pytest.raises(DFShaError) as e:
        capabilities.verify(KEY, tok, **args)
    assert e.value.status == 403


def test_capacidad_modificada_o_con_otra_clave_se_rechaza():
    tok = capabilities.sign(KEY, _claims())
    payload, sig = tok.split(".")
    with pytest.raises(DFShaError):
        capabilities.verify(KEY, payload + "." + sig[:-2] + "AA", op="put", block_id="b-1", node_id="dn-a")
    with pytest.raises(DFShaError):
        capabilities.verify(b"otra-clave", tok, op="put", block_id="b-1", node_id="dn-a")


def test_capacidad_vencida_se_rechaza_con_401():
    tok = capabilities.sign(KEY, _claims(), ttl_s=-5)
    with pytest.raises(DFShaError) as e:
        capabilities.verify(KEY, tok, op="put", block_id="b-1", node_id="dn-a")
    assert e.value.status == 401


def test_vida_maxima_de_una_capacidad_es_5_minutos():
    import json, base64
    tok = capabilities.sign(KEY, _claims(), ttl_s=999999)
    payload = tok.split(".")[0]
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    assert claims["exp"] - time.time() <= capabilities.MAX_TTL_S + 1
