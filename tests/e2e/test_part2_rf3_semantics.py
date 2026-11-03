"""RF3 (PARTE 2): open/read/write/append/close con la semántica declarada (sección 5.1 del diseño).

Prueba mínima del enunciado: "Demostración de read()/write() con la semántica declarada".
"""
import os

import pytest

from client import DFShaClientError
from tests.conftest import BLOCK, manifest_of, uniq


def _crear(c, contenido: bytes) -> str:
    f = "/" + uniq("rf3")
    with c.open(f, "w", create=True) as h:
        h.write(0, contenido)
    return f


def _leer_todo(c, f) -> bytes:
    with c.open(f, "r") as h:
        return h.read(0, h.size)


def test_crear_escribir_y_leer(alice):
    f = _crear(alice, b"hola mundo distribuido")
    assert _leer_todo(alice, f) == b"hola mundo distribuido"
    assert alice.stat(f)["version"] == 1


def test_write_reemplaza_un_rango_y_crea_version_nueva(alice):
    f = _crear(alice, b"hola mundo distribuido")
    with alice.open(f, "w") as h:
        h.write(5, b"MUNDO")
    assert _leer_todo(alice, f) == b"hola MUNDO distribuido"
    assert alice.stat(f)["version"] == 2


def test_write_puede_extender_el_archivo_y_append_agrega_al_final(alice):
    f = _crear(alice, b"0123456789")
    with alice.open(f, "w") as h:
        h.write(8, b"ABCDEF")                       # reemplaza 2 bytes y extiende 4
    assert _leer_todo(alice, f) == b"01234567ABCDEF"
    with alice.open(f, "a") as h:
        h.append(b"-fin")
        h.append(b"!")
    assert _leer_todo(alice, f) == b"01234567ABCDEF-fin!"


def test_no_se_admiten_escrituras_con_huecos(alice):
    f = _crear(alice, b"abc")
    h = alice.open(f, "w")
    with pytest.raises(ValueError):
        h.write(10, b"x")                           # offset > tamaño: hueco
    h.abort()


def test_read_de_rangos_que_cruzan_bloques(alice):
    data = os.urandom(3 * BLOCK + 12345)
    f = _crear(alice, data)
    with alice.open(f, "r") as h:
        assert h.size == len(data)
        assert h.read(BLOCK - 10, 20) == data[BLOCK - 10 : BLOCK + 10]           # frontera de bloque
        assert h.read(0, 10) == data[:10]
        assert h.read(2 * BLOCK + 5, 4 * BLOCK) == data[2 * BLOCK + 5 :]          # se recorta al final
        assert h.read(len(data), 10) == b""                                       # más allá del final


def test_write_que_cruza_bloques_reescribe_solo_los_bloques_afectados_copy_on_write(alice):
    data = os.urandom(5 * BLOCK)
    f = _crear(alice, data)
    antes = {b["index"]: b["block_id"] for b in manifest_of(alice, f)}
    parche = b"P" * 100
    with alice.open(f, "w") as h:
        h.write(BLOCK - 50, parche)                 # toca los bloques 0 y 1
    despues = {b["index"]: b["block_id"] for b in manifest_of(alice, f)}
    assert despues[0] != antes[0] and despues[1] != antes[1]                      # reescritos como bloques NUEVOS
    assert all(despues[i] == antes[i] for i in (2, 3, 4))                         # el resto se reutiliza por referencia
    esperado = bytearray(data); esperado[BLOCK - 50 : BLOCK + 50] = parche
    assert _leer_todo(alice, f) == bytes(esperado)


def test_snapshot_un_handle_abierto_sigue_viendo_su_version(alice):
    f = _crear(alice, b"version uno")
    lector = alice.open(f, "r")
    with alice.open(f, "w") as w:
        w.write(0, b"VERSION DOS")
    assert lector.read(0, 100) == b"version uno"      # G5: sigue en su snapshot aunque se publicó v2
    lector.close()
    assert _leer_todo(alice, f) == b"VERSION DOS"     # G4: un open posterior ve la nueva


def test_nada_es_visible_hasta_close(alice, cluster):
    f = _crear(alice, b"original")
    otro = cluster.client(home=str(cluster.dir / "home" / "alice"))
    w = alice.open(f, "w")
    w.write(0, b"CAMBIADO")
    assert _leer_todo(otro, f) == b"original"          # G3: no se ve nada a medias
    w.close()
    assert _leer_todo(otro, f) == b"CAMBIADO"


def test_abort_o_excepcion_no_publica_nada(alice):
    f = _crear(alice, b"intacto")
    h = alice.open(f, "w")
    h.write(0, b"BASURA")
    h.abort()
    assert alice.stat(f)["version"] == 1 and _leer_todo(alice, f) == b"intacto"
    with pytest.raises(RuntimeError):
        with alice.open(f, "w") as h:
            h.write(0, b"BASURA")
            raise RuntimeError("falla en medio de la escritura")
    assert alice.stat(f)["version"] == 1 and _leer_todo(alice, f) == b"intacto"
    alice.open(f, "w").abort()                         # y el lock quedó liberado


def test_truncate_reemplaza_el_contenido_completo(alice):
    f = _crear(alice, os.urandom(2 * BLOCK))
    with alice.open(f, "w", truncate=True) as h:
        h.write(0, b"corto")
    assert _leer_todo(alice, f) == b"corto" and alice.stat(f)["size"] == 5


def test_archivo_vacio_y_modo_lectura_no_escribe(alice):
    f = "/" + uniq("vacio")
    alice.open(f, "w", create=True).close()
    assert alice.stat(f)["size"] == 0
    with alice.open(f, "r") as h:
        assert h.read(0, 10) == b""
        with pytest.raises(DFShaClientError):
            h.write(0, b"x")


def test_abrir_inexistente_sin_create_falla(alice):
    with pytest.raises(DFShaClientError) as e:
        alice.open("/" + uniq("nada"), "w")
    assert e.value.status == 404
