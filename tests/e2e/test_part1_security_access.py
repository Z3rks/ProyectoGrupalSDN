"""Seguridad de acceso (PARTE 1): autenticación, autorización, aislamiento entre usuarios, rol admin."""
import time

import jwt
import pytest

from client import DFShaClientError
from tests.conftest import uniq


def test_peticion_sin_token_o_con_token_invalido_se_rechaza(cluster):
    c = cluster.client()
    for hdr in (None, "Bearer basura", "Bearer " + jwt.encode({"sub": "alice", "exp": int(time.time()) + 600}, "otro-secreto", "HS256")):
        c.session["access_token"] = hdr[7:] if hdr else None
        with pytest.raises(DFShaClientError) as e:
            c._control("GET", "/v1/ls", params={"path": "/"}, retries=1)
        assert e.value.status == 401


def test_jwt_vencido_se_rechaza(cluster, alice):
    expired = jwt.encode({"sub": "alice", "role": "user", "exp": int(time.time()) - 10}, "secreto-jwt-de-prueba", "HS256")
    c = cluster.client()
    c.session["access_token"] = expired
    with pytest.raises(DFShaClientError) as e:
        c._control("GET", "/v1/ls", params={"path": "/"}, retries=1)
    assert e.value.status == 401 and "vencido" in e.value.message


def test_login_no_distingue_usuario_inexistente_de_clave_incorrecta(cluster, alice):
    c = cluster.client()
    with pytest.raises(DFShaClientError) as e1:
        c.login("alice", "clave-equivocada")
    with pytest.raises(DFShaClientError) as e2:
        c.login("no-existe-" + uniq(), "lo-que-sea-123")
    assert e1.value.status == e2.value.status == 401
    assert e1.value.message == e2.value.message          # no se puede enumerar usuarios


def test_refresh_token_rota_y_no_se_puede_reutilizar(cluster, alice):
    old = alice.session["refresh_token"]
    alice._refresh()
    assert alice.session["refresh_token"] != old
    c2 = cluster.client()
    c2.session["refresh_token"] = old
    with pytest.raises(DFShaClientError) as e:
        c2._refresh()
    assert e.value.status == 401                          # uso único


def test_un_usuario_no_ve_ni_modifica_los_archivos_de_otro(alice, bob, tmpfile):
    path, _ = tmpfile(300_000)
    f = "/" + uniq("privado")
    alice.put(path, f)

    assert f.strip("/") not in [e["name"] for e in bob.ls("/")]
    for op in (lambda: bob.stat(f), lambda: bob.open(f, "r"), lambda: bob.get(f, "/tmp/robado")):
        with pytest.raises(DFShaClientError) as e:
            op()
        assert e.value.status == 404                      # ni siquiera se revela que existe
    assert bob.rm(f) is False                             # rm sobre SU espacio: no toca el de alice
    assert alice.stat(f)["size"] == 300_000

    h = alice.open(f, "r")                                # un handle ajeno tampoco sirve
    with pytest.raises(DFShaClientError) as e:
        bob._control("GET", f"/v1/handles/{h.id}/manifest", retries=1)
    assert e.value.status == 404
    h.close()


def test_mismo_nombre_de_ruta_en_dos_usuarios_son_archivos_distintos(alice, bob, tmp_path):
    name = "/" + uniq("mismo")
    (tmp_path / "a").write_bytes(b"contenido de alice")
    (tmp_path / "b").write_bytes(b"contenido de bob!!")
    alice.put(str(tmp_path / "a"), name)
    bob.put(str(tmp_path / "b"), name)
    with alice.open(name, "r") as h:
        assert h.read(0, 100) == b"contenido de alice"
    with bob.open(name, "r") as h:
        assert h.read(0, 100) == b"contenido de bob!!"


def test_el_admin_gestiona_cuentas_pero_no_ve_archivos_de_usuarios(cluster, alice, tmpfile):
    path, _ = tmpfile(1000)
    f = "/" + uniq("solo-alice")
    alice.put(path, f)
    admin = cluster.admin_client()
    assert f.strip("/") not in [e["name"] for e in admin.ls("/")]
    with pytest.raises(DFShaClientError) as e:
        admin.open(f, "r")
    assert e.value.status == 404


def test_un_usuario_normal_no_puede_administrar_cuentas(alice):
    with pytest.raises(DFShaClientError) as e:
        alice.useradd("intruso-" + uniq()[:6], "clave-larga-123")
    assert e.value.status == 403


def test_borrar_un_usuario_revoca_su_acceso_y_libera_sus_archivos(cluster, tmpfile):
    name = "temp" + uniq()[-6:]
    u = cluster.make_user(name)
    path, _ = tmpfile(2000)
    u.put(path, "/x.bin")
    cluster.admin_client().userdel(name)
    time.sleep(6)                                         # la caché de cuentas del control dura 5 s
    with pytest.raises(DFShaClientError) as e:
        u._control("GET", "/v1/ls", params={"path": "/"}, retries=1)
    assert e.value.status == 401
    with pytest.raises(DFShaClientError):
        cluster.client().login(name, "clave-de-prueba-1")


def test_validacion_de_cuentas(cluster):
    admin = cluster.admin_client()
    with pytest.raises(DFShaClientError) as e:
        admin.useradd("Mayusculas", "clave-larga-123")
    assert e.value.status == 400
    with pytest.raises(DFShaClientError) as e:
        admin.useradd("usuario-ok", "corta")
    assert e.value.status == 400
