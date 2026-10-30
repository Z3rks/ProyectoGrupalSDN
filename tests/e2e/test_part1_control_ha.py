"""Alta disponibilidad del plano de control (PARTE 1): caída de instancias de control y de miembros de etcd."""
import pytest

from client import DFShaClientError
from tests.conftest import manifest_of, uniq


def test_hay_un_unico_lider_de_control(cluster):
    leaders = [cluster.status(i)["is_leader"] for i in range(3)]
    assert sum(leaders) == 1


def test_cae_una_instancia_de_control_el_cliente_sigue_operando(restore, alice, tmpfile):
    cluster = restore
    path, _ = tmpfile(400_000)
    cluster.kill("control1")                               # la primera de la lista del cliente
    d = "/" + uniq("ha")
    assert alice.mkdir(d) is True                          # failover transparente
    alice.put(path, d + "/x.bin")
    assert alice.stat(d + "/x.bin")["size"] == 400_000


def test_cae_el_lider_y_otra_instancia_toma_el_relevo(restore):
    cluster = restore
    cluster.wait_for(lambda: cluster.leader(), timeout=40, what="líder inicial")
    leader = cluster.leader()
    assert leader
    cluster.kill(leader)
    cluster.wait_for(lambda: cluster.leader() not in (None, leader), timeout=40, what="elección de un nuevo líder")
    assert cluster.leader() != leader


def test_cae_un_miembro_de_etcd_el_sistema_sigue_escribiendo(restore, alice, tmpfile):
    cluster = restore
    cluster.kill("etcd2")                                  # 2 de 3 = hay quórum
    path, sha = tmpfile(500_000)
    f = "/" + uniq("etcd1caido")
    alice.put(path, f)
    assert alice.stat(f)["size"] == 500_000


def test_sin_mayoria_de_etcd_se_rechazan_escrituras_pero_siguen_lecturas_iniciadas(restore, alice, tmpfile):
    """Decisión CP: sin quórum NO se acepta nada nuevo; lo que ya se había abierto puede terminar de leerse."""
    cluster = restore
    path, sha = tmpfile(500_000)
    f = "/" + uniq("cp")
    alice.put(path, f)
    h = alice.open(f, "r")
    manifest = alice._manifest(h.id, 0, 1000)["blocks"]    # capacidades ya emitidas (válidas 5 min)

    cluster.kill("etcd2")
    cluster.kill("etcd3")                                  # 1 de 3: no hay mayoría

    with pytest.raises(DFShaClientError) as e:
        alice._control("POST", "/v1/mkdir", json_body={"path": "/" + uniq("x")}, retries=1)
    assert e.value.status == 503 and e.value.code == "CONTROL_NO_QUORUM" and e.value.retryable

    import hashlib
    data = b"".join(alice._download_block(b) for b in manifest)        # los nodos validan solos las capacidades
    assert hashlib.sha256(data).hexdigest() == sha

    cluster.start("etcd2")
    cluster.start("etcd3")                                 # vuelve el quórum: el servicio se recupera solo
    cluster.wait_for(lambda: alice.mkdir("/" + uniq("tras-recuperar")), timeout=60, what="recuperación del quórum")
