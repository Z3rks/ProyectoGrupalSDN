"""Clúster DFSha local para pruebas: procesos REALES comunicándose por red (TLS 1.3 + mTLS).

Levanta:  3 × etcd (Raft)  +  3 × servicio de control  +  N × nodo de datos
Todo en localhost con puertos distintos. Cada proceso tiene su propio certificado.

Uso desde Python (así lo hacen las pruebas):
    with LocalCluster("run/demo") as cl:
        c = cl.client()                 # cliente ya autenticado como usuario de prueba
        cl.kill("dn-b")                 # matar un nodo de datos (SIGKILL)
        cl.start("dn-b")                # volver a levantarlo

Uso desde la terminal (deja el clúster corriendo para probar a mano):
    python scripts/local_cluster.py --dir run/demo
"""
from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from client import DFShaClient  # noqa: E402
from common.config import TlsSettings  # noqa: E402
from common.tls import client_ssl_context  # noqa: E402

ETCD_BIN = os.environ.get("ETCD_BIN") or shutil.which("etcd") or "/opt/etcd/etcd"
NODE_IDS = ["dn-a", "dn-b", "dn-c", "dn-d", "dn-e"]


class LocalCluster:
    def __init__(self, workdir: str, *, tls: bool = True, n_nodes: int = 3, block_size: int = 256 * 1024,
                 base_port: int = 14000, extra_env: dict | None = None) -> None:
        self.dir = Path(workdir).resolve()
        self.tls = tls
        self.n_nodes = n_nodes
        self.base = base_port
        self.block_size = block_size
        self.procs: dict[str, subprocess.Popen] = {}
        self.cmds: dict[str, tuple[list[str], dict]] = {}
        self.logs = self.dir / "logs"
        self.certs = self.dir / "certs"
        self.admin_password = "admin-local-123"
        self.extra_env = extra_env or {}
        self.scheme = "https" if tls else "http"

    # ------------------------------------------------------------------ puertos y URLs
    def etcd_client_port(self, i): return self.base + i * 2
    def etcd_peer_port(self, i): return self.base + i * 2 + 1
    def control_public(self, i): return self.base + 100 + i * 2
    def control_internal(self, i): return self.base + 100 + i * 2 + 1
    def node_public(self, i): return self.base + 200 + i * 2
    def node_internal(self, i): return self.base + 200 + i * 2 + 1

    @property
    def control_urls(self): return [f"{self.scheme}://localhost:{self.control_public(i)}" for i in range(3)]

    # ------------------------------------------------------------------ ciclo de vida
    def __enter__(self): self.start_all(); return self
    def __exit__(self, *a): self.stop_all()

    def start_all(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)
        self.logs.mkdir(parents=True)
        if self.tls:
            subprocess.run(["bash", str(ROOT / "deploy" / "gen_certs.sh"), str(self.certs)], check=True, capture_output=True)
        self._define_processes()
        for name in ("etcd1", "etcd2", "etcd3"):
            self.start(name)
        self._wait_etcd()
        for i in range(3):
            self.start(f"control{i + 1}")
        self._wait_control()
        for i in range(self.n_nodes):
            self.start(NODE_IDS[i])
        self.wait_nodes(self.n_nodes)

    def _tls_env(self, who: str) -> dict:
        if not self.tls:
            return {"DFSHA_TLS": "off"}
        c = self.certs
        return {"DFSHA_TLS": "on", "DFSHA_TLS_CA": str(c / "ca.crt"),
                "DFSHA_TLS_CERT": str(c / f"{who}.crt"), "DFSHA_TLS_KEY": str(c / f"{who}.key")}

    def _define_processes(self) -> None:
        env0 = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONUNBUFFERED": "1"}
        s, c = self.scheme, self.certs
        initial = ",".join(f"etcd{i + 1}={s}://127.0.0.1:{self.etcd_peer_port(i)}" for i in range(3))
        etcd_eps = ",".join(f"{s}://127.0.0.1:{self.etcd_client_port(i)}" for i in range(3))
        for i in range(3):
            name = f"etcd{i + 1}"
            cmd = [ETCD_BIN, "--name", name, "--data-dir", str(self.dir / "data" / name),
                   "--listen-client-urls", f"{s}://127.0.0.1:{self.etcd_client_port(i)}",
                   "--advertise-client-urls", f"{s}://127.0.0.1:{self.etcd_client_port(i)}",
                   "--listen-peer-urls", f"{s}://127.0.0.1:{self.etcd_peer_port(i)}",
                   "--initial-advertise-peer-urls", f"{s}://127.0.0.1:{self.etcd_peer_port(i)}",
                   "--initial-cluster", initial, "--initial-cluster-token", "dfsha-local", "--initial-cluster-state", "new"]
            if self.tls:
                cmd += ["--cert-file", str(c / f"{name}.crt"), "--key-file", str(c / f"{name}.key"), "--client-cert-auth",
                        "--trusted-ca-file", str(c / "ca.crt"), "--peer-cert-file", str(c / f"{name}.crt"),
                        "--peer-key-file", str(c / f"{name}.key"), "--peer-client-cert-auth", "--peer-trusted-ca-file", str(c / "ca.crt")]
            self.cmds[name] = (cmd, env0)

        common = {"DFSHA_CAP_KEY": "clave-capacidades-de-prueba", "DFSHA_BLOCK_SIZE": str(self.block_size)}
        control_internal = ",".join(f"{s}://localhost:{self.control_internal(i)}" for i in range(3))
        for i in range(3):
            name = f"control{i + 1}"
            env = {**env0, **common, **self._tls_env(name), "DFSHA_INSTANCE_ID": name, "DFSHA_ETCD": etcd_eps,
                   "DFSHA_PUBLIC_PORT": str(self.control_public(i)), "DFSHA_INTERNAL_PORT": str(self.control_internal(i)),
                   "DFSHA_JWT_SECRET": "secreto-jwt-de-prueba", "DFSHA_ADMIN_PASSWORD": self.admin_password,
                   # tiempos cortos para que las pruebas de fallos duren segundos y no minutos
                   "DFSHA_SUSPECT_AFTER": "4", "DFSHA_DEAD_AFTER": "10", "DFSHA_REPAIR_INTERVAL": "3",
                   "DFSHA_GC_INTERVAL": "4", "DFSHA_GRACE_VERSION": "3", "DFSHA_GRACE_ORPHAN": "6",
                   "DFSHA_LEADER_TTL": "6", "DFSHA_HANDLE_TTL": "20", "DFSHA_LOCK_TTL": "8", "DFSHA_LOGIN_LIMIT": "50",
                   **self.extra_env}
            self.cmds[name] = ([sys.executable, "-m", "control.main"], env)
        for i in range(self.n_nodes):
            nid = NODE_IDS[i]
            master = hashlib.sha256(f"master-{nid}".encode()).hexdigest()
            env = {**env0, **common, **self._tls_env(f"dn{i + 1}"), "DFSHA_NODE_ID": nid, "DFSHA_MASTER_KEY": master,
                   "DFSHA_PUBLIC_PORT": str(self.node_public(i)), "DFSHA_INTERNAL_PORT": str(self.node_internal(i)),
                   "DFSHA_DATA_DIR": str(self.dir / "data" / nid), "DFSHA_CONTROL_INTERNAL": control_internal,
                   "DFSHA_ADVERTISE_HOST": "localhost", "DFSHA_HEARTBEAT": "1", "DFSHA_REPLICA_TIMEOUT": "5",
                   "DFSHA_SCRUB_INTERVAL": "3600"}
            self.cmds[nid] = ([sys.executable, "-m", "datanode.main"], env)

    def start(self, name: str) -> None:
        cmd, env = self.cmds[name]
        log = open(self.logs / f"{name}.log", "ab")
        self.procs[name] = subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT, cwd=ROOT)

    def kill(self, name: str, sig: int | None = None) -> None:
        p = self.procs.get(name)
        if p and p.poll() is None:
            # SIGKILL no existe en Windows. Popen.kill() fuerza la terminación
            # en ambas plataformas; para señales explícitas conservamos send_signal.
            if sig is None:
                p.kill()
            else:
                p.send_signal(sig)
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:   # no terminó con el apagado ordenado: forzar
                p.kill()
                p.wait(timeout=5)

    def stop_all(self) -> None:
        for name in reversed(list(self.procs)):
            self.kill(name, signal.SIGTERM)

    def is_running(self, name: str) -> bool:
        p = self.procs.get(name)
        return bool(p and p.poll() is None)

    # ------------------------------------------------------------------ esperas
    def _http(self) -> httpx.Client:
        ctx = client_ssl_context(TlsSettings(True, str(self.certs / "ca.crt"), str(self.certs / "client.crt"),
                                             str(self.certs / "client.key")), with_client_cert=True) if self.tls else True
        return httpx.Client(verify=ctx, timeout=3.0)

    def _wait_etcd(self, timeout: float = 40) -> None:
        end = time.time() + timeout
        with self._http() as h:
            while time.time() < end:
                try:
                    r = h.post(f"{self.scheme}://127.0.0.1:{self.etcd_client_port(0)}/v3/kv/range", json={"key": "AA=="})
                    if r.status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
                time.sleep(0.5)
        raise RuntimeError("etcd no arrancó; revise " + str(self.logs))

    def _wait_control(self, timeout: float = 40) -> None:
        end = time.time() + timeout
        with self._http() as h:
            for i in range(3):
                while True:
                    try:
                        if h.get(f"{self.control_urls[i]}/healthz").status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    if time.time() > end:
                        raise RuntimeError("control no arrancó; revise " + str(self.logs))
                    time.sleep(0.3)
        # el admin se crea en segundo plano: esperar a que pueda iniciar sesión
        while time.time() < end:
            try:
                c = self.admin_client()
                c.login("admin", self.admin_password)
                return
            except Exception:
                time.sleep(0.5)
        raise RuntimeError("no se pudo iniciar sesión como admin")

    def wait_all_up(self, timeout: float = 60) -> None:
        """Espera a que CADA proceso en ejecución atienda peticiones (no basta mirar los latidos: el último
        latido de un nodo recién reiniciado aún puede parecer reciente mientras el proceso sigue arrancando)."""
        end = time.time() + timeout
        checks = {f"control{i + 1}": f"{self.control_urls[i]}/healthz" for i in range(3)}
        checks.update({NODE_IDS[i]: f"{self.scheme}://localhost:{self.node_public(i)}/healthz" for i in range(self.n_nodes)})
        checks.update({f"etcd{i + 1}": f"{self.scheme}://127.0.0.1:{self.etcd_client_port(i)}/health" for i in range(3)})
        with self._http() as h:
            for name, url in checks.items():
                if not self.is_running(name):
                    continue
                while True:
                    try:
                        if h.get(url).status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    if time.time() > end:
                        raise RuntimeError(f"{name} no atiende peticiones; revise {self.logs}")
                    time.sleep(0.3)

    def status(self, i: int = 0) -> dict:
        """GET /internal/v1/status de la instancia de control i (mTLS)."""
        with self._http() as h:
            return h.get(f"{self.scheme}://localhost:{self.control_internal(i)}/internal/v1/status").json()

    def wait_nodes(self, healthy: int, timeout: float = 60, control: int = 0) -> None:
        end = time.time() + timeout
        while time.time() < end:
            try:
                nodes = self.status(control)["nodes"]
                if sum(1 for n in nodes if n["state"] == "HEALTHY") >= healthy:
                    return
            except Exception:
                pass
            time.sleep(0.5)
        raise RuntimeError(f"no hay {healthy} nodos HEALTHY a tiempo")

    def wait_for(self, predicate, timeout: float = 60, what: str = "condición") -> None:
        end = time.time() + timeout
        while time.time() < end:
            try:
                if predicate():
                    return
            except Exception:
                pass
            time.sleep(0.5)
        raise TimeoutError(f"no se cumplió a tiempo: {what}")

    def leader(self) -> str | None:
        for i in range(3):
            try:
                st = self.status(i)
                if st["is_leader"]:
                    return st["instance"]
            except Exception:
                continue
        return None

    # ------------------------------------------------------------------ clientes
    def client(self, home: str | None = None, **kw) -> DFShaClient:
        home = home or str(self.dir / "home" / f"c{time.time_ns()}")
        return DFShaClient(control=self.control_urls, ca=str(self.certs / "ca.crt") if self.tls else None, home=home, **kw)

    def admin_client(self) -> DFShaClient:
        c = self.client(home=str(self.dir / "home" / "admin"))
        if not c.session.get("access_token"):
            c.login("admin", self.admin_password)
        return c

    def make_user(self, name: str, password: str = "clave-de-prueba-1") -> DFShaClient:
        """Crea el usuario (si no existe) y devuelve un cliente autenticado con una sesión propia."""
        from client import DFShaClientError
        try:
            self.admin_client().useradd(name, password)
        except DFShaClientError as exc:
            if exc.code != "EXISTS":
                raise
        c = self.client(home=str(self.dir / "home" / name))
        c.login(name, password)
        return c


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Clúster DFSha local para pruebas manuales")
    ap.add_argument("--dir", default="run/demo")
    ap.add_argument("--no-tls", action="store_true")
    ap.add_argument("--block-size", type=int, default=4 * 1024 * 1024)
    ap.add_argument("--nodes", type=int, default=3)
    a = ap.parse_args()
    with LocalCluster(a.dir, tls=not a.no_tls, block_size=a.block_size, n_nodes=a.nodes) as cl:
        print("\nClúster listo. Cliente:")
        print(f"  export DFSHA_CONTROL={','.join(cl.control_urls)}")
        if cl.tls:
            print(f"  export DFSHA_CA={cl.certs / 'ca.crt'}")
        print(f"  usuario admin / contraseña {cl.admin_password}")
        print("Ctrl+C para detener todo.")
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            pass
