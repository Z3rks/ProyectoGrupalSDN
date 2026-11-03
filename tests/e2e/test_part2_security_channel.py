"""Seguridad del canal, de las capacidades y del almacenamiento (PARTE 2).

Prueba mínima del enunciado: "acceso autorizado y denegado; evidencia de protección del canal y almacenamiento".
"""
import socket
import ssl
import threading

import httpx
import pytest

from common import capabilities
from tests.conftest import manifest_of, uniq

CAP_KEY = b"clave-capacidades-de-prueba"      # la usa el clúster de pruebas (scripts/local_cluster.py)


def _bloque(alice, tmpfile):
    path, _ = tmpfile(400_000)
    f = "/" + uniq("sec")
    alice.put(path, f)
    b = manifest_of(alice, f)[0]
    return b, b["nodes"][0]


def _get(cluster, node, block_id, cap=None):
    headers = {"Authorization": "Capability " + cap} if cap else {}
    return httpx.get(f"{node['endpoint']}/v1/blocks/{block_id}", headers=headers, verify=str(cluster.certs / "ca.crt"))


# ----------------------------------------------------------------------------- capacidades
def test_nodo_de_datos_acepta_solo_capacidades_validas(cluster, alice, tmpfile):
    b, n = _bloque(alice, tmpfile)
    assert _get(cluster, n, b["block_id"], n["capability"]).status_code == 200            # autorizado

    assert _get(cluster, n, b["block_id"]).status_code == 401                            # sin capacidad
    assert _get(cluster, n, "b-otro-bloque", n["capability"]).status_code == 403          # capacidad de OTRO bloque
    caducada = capabilities.sign(CAP_KEY, {"op": "get", "uid": "alice", "block_id": b["block_id"], "node_id": n["node_id"]}, ttl_s=-5)
    assert _get(cluster, n, b["block_id"], caducada).status_code == 401                  # vencida
    payload, sig = n["capability"].split(".")
    assert _get(cluster, n, b["block_id"], payload + "." + sig[:-3] + "AAA").status_code == 403   # alterada
    falsa = capabilities.sign(b"clave-equivocada", {"op": "get", "uid": "alice", "block_id": b["block_id"], "node_id": n["node_id"]})
    assert _get(cluster, n, b["block_id"], falsa).status_code == 403                      # firmada con otra clave


def test_capacidad_de_un_nodo_no_sirve_en_otro_ni_para_otra_operacion(cluster, alice, tmpfile):
    b, n = _bloque(alice, tmpfile)
    otro = [x for x in b["nodes"] if x["node_id"] != n["node_id"]][0]
    assert _get(cluster, otro, b["block_id"], n["capability"]).status_code == 403          # capacidad ligada a SU nodo
    r = httpx.put(f"{n['endpoint']}/v1/blocks/{b['block_id']}", content=b"x", verify=str(cluster.certs / "ca.crt"),
                  headers={"Authorization": "Capability " + n["capability"], "X-Block-Length": "1", "X-Block-SHA256": "0" * 64})
    assert r.status_code == 403                                                            # una capacidad 'get' no permite escribir


# ----------------------------------------------------------------------------- canal
def _puertos(cluster):
    return cluster.node_public(0), cluster.node_internal(0), cluster.control_internal(0)


def test_el_canal_exige_tls_13_y_certificado_valido(cluster):
    pub, _, _ = _puertos(cluster)
    ca = str(cluster.certs / "ca.crt")

    def conectar(ctx):
        with socket.create_connection(("localhost", pub), timeout=5) as raw:
            with ctx.wrap_socket(raw, server_hostname="localhost") as s:
                return s.version()

    ok = ssl.create_default_context(cafile=ca)
    assert conectar(ok) == "TLSv1.3"

    tls12 = ssl.create_default_context(cafile=ca)
    tls12.maximum_version = ssl.TLSVersion.TLSv1_2
    with pytest.raises(ssl.SSLError):
        conectar(tls12)                                                   # TLS 1.2 se rechaza

    sin_ca = ssl.create_default_context()
    with pytest.raises(ssl.SSLError):
        conectar(sin_ca)                                                  # CA desconocida: el cliente no confía en el servidor

    with pytest.raises(httpx.HTTPError):
        httpx.get(f"http://localhost:{pub}/healthz", timeout=5)           # HTTP plano contra el puerto TLS


def test_los_puertos_internos_exigen_certificado_de_cliente_mtls(cluster):
    _, nodo_int, control_int = _puertos(cluster)
    ca = str(cluster.certs / "ca.crt")
    for puerto, ruta in ((nodo_int, "/internal/v1/inventory"), (control_int, "/internal/v1/status")):
        url = f"https://localhost:{puerto}{ruta}"
        with pytest.raises(httpx.HTTPError):
            httpx.get(url, verify=ca, timeout=5)                          # sin certificado de cliente: rechazado
        ctx = ssl.create_default_context(cafile=ca)
        ctx.load_cert_chain(str(cluster.certs / "client.crt"), str(cluster.certs / "client.key"))
        assert httpx.get(url, verify=ctx, timeout=5).status_code == 200   # con certificado de la CA: aceptado

    # un certificado de OTRA CA no sirve
    import subprocess, tempfile, os
    d = tempfile.mkdtemp()
    subprocess.run(["bash", os.path.join(os.path.dirname(__file__), "..", "..", "deploy", "gen_certs.sh"), d], check=True, capture_output=True)
    ctx = ssl.create_default_context(cafile=ca)
    ctx.load_cert_chain(os.path.join(d, "client.crt"), os.path.join(d, "client.key"))
    with pytest.raises(httpx.HTTPError):
        httpx.get(f"https://localhost:{nodo_int}/internal/v1/inventory", verify=ctx, timeout=5)


def test_el_trafico_capturado_no_contiene_el_contenido_del_archivo(cluster, alice):
    """Un 'sniffer' (proxy TCP) entre cliente y nodo solo ve bytes cifrados."""
    marca = b"MARCA-SECRETA-DE-PRUEBA-" * 2000
    f = "/" + uniq("sniff")
    h = alice.open(f, "w", create=True)
    alloc = alice._allocate(h.id, [0])[0]
    nodo_puerto = int(alloc["primary"]["endpoint"].rsplit(":", 1)[1])
    capturado = bytearray()

    srv = socket.socket(); srv.bind(("127.0.0.1", 0)); srv.listen(1)
    puerto_proxy = srv.getsockname()[1]

    def bombear(a, b):
        try:
            while (d := a.recv(65536)):
                capturado.extend(d); b.sendall(d)
        except OSError:
            pass
        finally:
            for s in (a, b):
                try: s.shutdown(socket.SHUT_RDWR)
                except OSError: pass

    def proxy():
        cli, _ = srv.accept()
        up = socket.create_connection(("127.0.0.1", nodo_puerto))
        t1 = threading.Thread(target=bombear, args=(cli, up)); t2 = threading.Thread(target=bombear, args=(up, cli))
        t1.start(); t2.start(); t1.join(); t2.join()
    threading.Thread(target=proxy, daemon=True).start()

    import hashlib
    r = httpx.put(f"https://localhost:{puerto_proxy}/v1/blocks/{alloc['block_id']}", content=marca, timeout=20, verify=str(cluster.certs / "ca.crt"),
                  headers={"Authorization": "Capability " + alloc["capability"], "X-Block-Length": str(len(marca)),
                           "X-Block-SHA256": hashlib.sha256(marca).hexdigest()})
    assert r.status_code == 201
    assert len(capturado) > len(marca)                      # se capturó el tráfico completo...
    assert b"MARCA-SECRETA" not in bytes(capturado)         # ...y no hay ni rastro del contenido en claro
    h.abort()


# ----------------------------------------------------------------------------- reposo
def test_en_los_discos_de_los_nodos_no_hay_contenido_en_claro(cluster, alice, tmp_path):
    marca = b"CONFIDENCIAL-NO-DEBE-VERSE-EN-DISCO-" * 5000
    p = tmp_path / "secreto.bin"; p.write_bytes(marca)
    f = "/" + uniq("reposo")
    alice.put(str(p), f)
    ids = {b["block_id"] for b in manifest_of(alice, f)}
    archivos = [a for a in (cluster.dir / "data").rglob("*.blk") if a.stem in ids]
    assert len(archivos) >= 2 * len(ids)                    # las dos copias de cada bloque
    for a in archivos:
        assert b"CONFIDENCIAL" not in a.read_bytes()        # cifrado AES-256-GCM
    # además, ningún archivo temporal ni de otro tipo guarda el contenido
    for a in (cluster.dir / "data").rglob("*"):
        if a.is_file() and a.suffix != ".blk" and "etcd" not in str(a):
            assert b"CONFIDENCIAL" not in a.read_bytes()
