"""RF1 (PARTE 1): sistema de archivos jerárquico: ls, cd, mkdir, rmdir, rm. Aislamiento por usuario."""
import pytest

from client import DFShaClientError
from tests.conftest import uniq


def test_mkdir_ls_cd_rmdir(alice):
    d = "/" + uniq("proy")
    assert alice.mkdir(d) is True
    assert alice.mkdir(d) is False                      # idempotente
    alice.mkdir(d + "/sub")
    assert {e["name"]: e["type"] for e in alice.ls(d)} == {"sub": "dir"}

    alice.cd(d)
    assert alice.cwd == d and alice.resolve("sub/../x.txt") == d + "/x.txt"
    assert [e["name"] for e in alice.ls(".")] == ["sub"]    # rutas relativas al cd
    alice.cd("/")

    with pytest.raises(DFShaClientError) as e:
        alice.rmdir(d)                                  # tiene contenido
    assert e.value.code == "NOT_EMPTY" and e.value.status == 409
    alice.rmdir(d + "/sub")
    alice.rmdir(d)
    assert d.strip("/") not in [x["name"] for x in alice.ls("/")]


def test_mkdir_exige_padre_existente_y_no_pisa_archivos(alice, tmpfile):
    with pytest.raises(DFShaClientError) as e:
        alice.mkdir("/no-existe-" + uniq() + "/hijo")
    assert e.value.code == "NOT_FOUND"
    path, _ = tmpfile(1000)
    f = "/" + uniq("f")
    alice.put(path, f)
    with pytest.raises(DFShaClientError) as e:
        alice.mkdir(f)
    assert e.value.code == "EXISTS"
    with pytest.raises(DFShaClientError) as e:
        alice.mkdir(f + "/hijo")                        # el padre es un archivo
    assert e.value.code == "NOT_A_DIRECTORY"
    with pytest.raises(DFShaClientError) as e:
        alice.ls(f)
    assert e.value.code == "NOT_A_DIRECTORY"


def test_rm_es_idempotente_y_el_archivo_deja_de_ser_visible_al_instante(alice, tmpfile):
    path, _ = tmpfile(5000)
    f = "/" + uniq("rm")
    alice.put(path, f)
    assert alice.stat(f)["size"] == 5000
    assert alice.rm(f) is True
    assert alice.rm(f) is False                         # ya no existía
    with pytest.raises(DFShaClientError) as e:
        alice.stat(f)
    assert e.value.code == "NOT_FOUND"
    with pytest.raises(DFShaClientError):
        alice.open(f, "r")


def test_rm_de_un_directorio_se_rechaza(alice):
    d = "/" + uniq("dir")
    alice.mkdir(d)
    with pytest.raises(DFShaClientError) as e:
        alice.rm(d)
    assert e.value.code == "IS_A_DIRECTORY"
    alice.rmdir(d)


def test_put_exige_que_exista_el_directorio_destino(alice, tmpfile):
    path, _ = tmpfile(100)
    with pytest.raises(DFShaClientError) as e:
        alice.put(path, "/" + uniq("nodir") + "/x.bin")
    assert e.value.status == 404


@pytest.mark.parametrize("bad", ["/../x", "relativa", "/a\x00b", "/a|b"])
def test_rutas_invalidas_se_rechazan(alice, bad):
    # Se envía en crudo (sin pasar por alice.resolve) para comprobar la validación DEL SERVIDOR.
    with pytest.raises(DFShaClientError) as e:
        alice._control("POST", "/v1/mkdir", json_body={"path": bad})
    assert e.value.status == 400


def test_ls_de_raiz_lista_solo_hijos_directos(alice):
    base = "/" + uniq("raiz")
    alice.mkdir(base)
    alice.mkdir(base + "/a")
    alice.mkdir(base + "/a/b")                          # nieto: no debe aparecer al listar base
    assert [e["name"] for e in alice.ls(base)] == ["a"]
    assert [e["name"] for e in alice.ls(base + "/a")] == ["b"]
