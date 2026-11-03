"""Pruebas unitarias del almacén de bloques (PARTE 2): atomicidad, cifrado en reposo y detección de corrupción."""
import hashlib
import os

import pytest

from datanode.blockstore import BlockCorrupt, BlockNotFound, BlockStore, IntegrityError

KEY = bytes(range(32))


@pytest.fixture
def store(tmp_path):
    return BlockStore(str(tmp_path / "node"), KEY)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def test_guardar_y_leer_devuelve_el_mismo_contenido(store):
    data = os.urandom(100_000)
    assert store.put("b-aaaa1111", data, sha(data)) is True
    got, digest = store.get("b-aaaa1111")
    assert got == data and digest == sha(data)


def test_put_es_idempotente(store):
    data = b"hola" * 1000
    assert store.put("b-aaaa1111", data, sha(data)) is True
    assert store.put("b-aaaa1111", data, sha(data)) is False   # reintento del cliente: no duplica


def test_hash_incorrecto_no_deja_nada_publicado(store):
    with pytest.raises(IntegrityError):
        store.put("b-bbbb2222", b"contenido", sha(b"otro contenido"))
    with pytest.raises(BlockNotFound):
        store.get("b-bbbb2222")
    assert store.inventory() == []


def test_en_disco_no_hay_texto_en_claro(store):
    secreto = b"CONTENIDO-MUY-SECRETO-" * 500
    store.put("b-cccc3333", secreto, sha(secreto))
    raw = open(store._path("b-cccc3333"), "rb").read()
    assert b"CONTENIDO-MUY-SECRETO" not in raw
    assert raw.startswith(b"DFB1")


def test_alterar_el_cifrado_se_detecta_y_aparta_la_copia(store):
    data = os.urandom(50_000)
    store.put("b-dddd4444", data, sha(data))
    path = store._path("b-dddd4444")
    raw = bytearray(open(path, "rb").read())
    raw[100] ^= 0xFF                       # un bit cambiado en el contenido cifrado
    open(path, "wb").write(raw)
    with pytest.raises(BlockCorrupt):
        store.get("b-dddd4444")
    assert "b-dddd4444" in store.pending_corrupt       # se reportará al control en el heartbeat
    with pytest.raises(BlockNotFound):                 # y ya no se sirve
        store.get("b-dddd4444")


def test_alterar_la_cabecera_tambien_se_detecta(store):
    data = os.urandom(10_000)
    store.put("b-eeee5555", data, sha(data))
    path = store._path("b-eeee5555")
    raw = bytearray(open(path, "rb").read())
    raw[20] ^= 0x01                        # byte del SHA-256 de la cabecera (va autenticado como AAD)
    open(path, "wb").write(raw)
    with pytest.raises(BlockCorrupt):
        store.get("b-eeee5555")


def test_otra_clave_maestra_no_puede_leer(tmp_path):
    a = BlockStore(str(tmp_path / "n"), KEY)
    data = b"x" * 1000
    a.put("b-ffff6666", data, sha(data))
    b = BlockStore(str(tmp_path / "n"), bytes(reversed(KEY)))
    with pytest.raises(BlockCorrupt):
        b.get("b-ffff6666")


def test_bloques_temp_de_una_escritura_interrumpida_se_descartan_al_arrancar(tmp_path):
    root = str(tmp_path / "n")
    s = BlockStore(root, KEY)
    huerfano = os.path.join(s.tmp_dir, "b-1234.abcd.tmp")
    open(huerfano, "wb").write(b"medio bloque")          # simula un nodo que murió a mitad de escritura
    BlockStore(root, KEY)                                 # reinicio del nodo
    assert not os.path.exists(huerfano)


def test_inventario_lee_solo_cabeceras_y_scrub_encuentra_corruptos(store):
    for i in range(3):
        d = os.urandom(2000)
        store.put(f"b-inv{i}0000", d, sha(d))
    inv = store.inventory()
    assert len(inv) == 3 and all(len(b["sha256"]) == 64 for b in inv)
    path = store._path("b-inv10000")
    raw = bytearray(open(path, "rb").read()); raw[-1] ^= 0xFF; open(path, "wb").write(raw)
    assert store.scrub() == ["b-inv10000"]


def test_ids_con_rutas_maliciosas_no_escapan_del_directorio(store):
    with pytest.raises(BlockNotFound):
        store.get("../../etc/passwd")
