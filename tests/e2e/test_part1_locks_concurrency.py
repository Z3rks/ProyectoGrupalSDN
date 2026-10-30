"""Concurrencia y exclusión mutua (PARTE 1): lock exclusivo, fencing, versiones, lock() explícito."""
import threading
import time

import pytest

from client import DFShaClientError
from tests.conftest import uniq


def _make_file(c, tmpfile, size=600_000):
    path, _ = tmpfile(size)
    f = "/" + uniq("lk")
    c.put(path, f)
    return f


def test_dos_escritores_el_segundo_recibe_LOCKED_y_luego_puede_escribir(alice, tmpfile):
    f = _make_file(alice, tmpfile)
    a = alice.open(f, "w")
    with pytest.raises(DFShaClientError) as e:
        alice.open(f, "w")
    assert e.value.status == 409 and e.value.code == "LOCKED" and e.value.retryable
    a.write(0, b"A")
    a.close()
    b = alice.open(f, "w")                    # liberado el lock, ahora sí
    b.close()


def test_dos_clientes_modifican_el_mismo_bloque_sin_perder_actualizaciones(alice, tmpfile):
    """Ambos esperan el lock (wait): se SERIALIZAN y el segundo parte de la versión del primero."""
    f = _make_file(alice, tmpfile, 400_000)
    results, errors = [], []

    def worker(offset, payload):
        try:
            with alice.open(f, "w", wait=30) as h:
                h.write(offset, payload)
                time.sleep(0.5)              # mantiene el lock un rato para forzar la contención
            results.append(payload)
        except Exception as exc:             # pragma: no cover
            errors.append(exc)

    t1 = threading.Thread(target=worker, args=(0, b"PRIMERO--"))
    t2 = threading.Thread(target=worker, args=(100, b"SEGUNDO--"))
    t1.start(); t2.start(); t1.join(60); t2.join(60)
    assert not errors and len(results) == 2
    with alice.open(f, "r") as h:
        assert h.read(0, 9) == b"PRIMERO--" and h.read(100, 9) == b"SEGUNDO--"   # ninguna se perdió
        assert h.version == 3                # v1 (put) + 2 commits serializados


def test_lector_no_bloquea_ni_es_bloqueado_por_el_escritor(alice, tmpfile):
    f = _make_file(alice, tmpfile, 300_000)
    w = alice.open(f, "w")
    w.write(0, b"NUEVO")
    with alice.open(f, "r") as r:            # abrir para leer con un escritor activo funciona
        assert r.read(0, 5) != b"NUEVO"      # y no ve cambios sin publicar
        assert r.version == 1
    w.close()


def test_lock_perdido_el_escritor_viejo_no_puede_publicar_fencing(cluster, alice, tmpfile):
    """El dueño del lock deja de renovar; vence; otro cliente escribe; el viejo falla al hacer commit."""
    f = _make_file(alice, tmpfile, 300_000)
    viejo = alice.open(f, "w")
    viejo._stop.set()                         # simula un cliente congelado: ya no renueva
    viejo.write(0, b"VIEJO-VIEJO")
    cluster.wait_for(lambda: _can_open(alice, f), timeout=40, what="vencimiento del lock")
    nuevo = alice.open(f, "w")
    nuevo.write(0, b"NUEVO-NUEVO")
    nuevo.close()                             # el nuevo publica v2
    with pytest.raises(DFShaClientError) as e:
        viejo.close()                         # el viejo intenta publicar sobre la base v1
    assert e.value.status == 409 and e.value.code in ("LOCK_LOST", "VERSION_CONFLICT", "HANDLE_EXPIRED")
    with alice.open(f, "r") as h:
        assert h.read(0, 11) == b"NUEVO-NUEVO" and h.version == 2   # solo hay UNA versión vigente


def _can_open(c, f):
    try:
        h = c.open(f, "w")
        h.abort()
        return True
    except DFShaClientError:
        return False


def test_lock_explicito_bloquea_a_otros_y_se_puede_reutilizar_en_open(alice, tmpfile):
    f = _make_file(alice, tmpfile, 100_000)
    lk = alice.lock(f)
    assert lk["fencing"] > 0
    with pytest.raises(DFShaClientError) as e:
        alice.lock(f)                         # nadie más puede tomarlo
    assert e.value.code == "LOCKED"
    with pytest.raises(DFShaClientError) as e:
        alice.open(f, "w")                    # ni abrir para escribir
    assert e.value.code == "LOCKED"
    with pytest.raises(DFShaClientError) as e:
        alice.rm(f)                           # ni borrar
    assert e.value.code == "LOCKED"

    with alice.open(f, "w", lock_id=lk["lock_id"]) as h:      # el titular sí puede
        h.write(0, b"con lock explicito")
    assert alice.renew_lock(lk["lock_id"])["expires_in"] > 0  # el lock sigue siendo suyo tras el close
    alice.unlock(lk["lock_id"])
    alice.open(f, "w").abort()                                # y ahora está libre


def test_lock_explicito_vence_si_no_se_renueva(cluster, alice, tmpfile):
    f = _make_file(alice, tmpfile, 100_000)
    alice.lock(f)
    cluster.wait_for(lambda: _can_open(alice, f), timeout=40, what="vencimiento del lock explícito")


def test_lock_de_un_archivo_inexistente_o_ajeno(alice, bob, tmpfile):
    f = _make_file(alice, tmpfile, 1000)
    with pytest.raises(DFShaClientError) as e:
        bob.lock(f)
    assert e.value.status == 404
    lk = alice.lock(f)
    with pytest.raises(DFShaClientError) as e:
        bob.unlock(lk["lock_id"])             # un lock ajeno no se puede liberar
    assert e.value.status == 404
    alice.unlock(lk["lock_id"])


def test_creacion_simultanea_del_mismo_archivo_solo_una_gana(alice):
    f = "/" + uniq("nuevo")
    a = alice.open(f, "w", create=True)
    with pytest.raises(DFShaClientError) as e:
        alice.open(f, "w", create=True)
    assert e.value.code == "LOCKED"
    a.write(0, b"hola")
    a.close()
    with alice.open(f, "r") as h:
        assert h.read(0, 10) == b"hola"


def test_expected_version_detecta_cambios_previos(alice, tmpfile):
    f = _make_file(alice, tmpfile, 5000)
    with alice.open(f, "w", expected_version=1) as h:
        h.write(0, b"x")
    with pytest.raises(DFShaClientError) as e:
        alice.open(f, "w", expected_version=1)   # ya está en la v2
    assert e.value.code == "VERSION_CONFLICT"


def test_rm_durante_una_escritura_hace_fallar_el_commit(alice, tmpfile):
    f = _make_file(alice, tmpfile, 5000)
    h = alice.open(f, "w")
    h.write(0, b"zzz")
    with pytest.raises(DFShaClientError) as e:
        alice.rm(f)                            # hay un lock vigente: rm se rechaza
    assert e.value.code == "LOCKED"
    h.close()
