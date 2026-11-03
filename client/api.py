"""Cliente de DFSha: comunicación con el control y con los nodos de datos.

Dos "mundos" de peticiones:
  * CONTROL  (metadatos, JSON, JWT)       -> `_control()`: con reintentos y failover entre instancias.
  * DATOS    (bloques, binario, capacidad) -> directo al nodo que indica el manifiesto.

Lo que hace el cliente (y por qué el control no es cuello de botella):
  - divide archivos en bloques y los sube/baja EN PARALELO directamente a/desde los nodos,
  - verifica el SHA-256 de cada bloque y el hash total,
  - si un nodo falla: lee de otra réplica (lectura) o pide otra asignación (escritura).
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import ssl
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx

from common.config import TlsSettings, env, env_list
from common.tls import client_ssl_context

from .errors import DFShaClientError
from .handle import FileHandle

TRANSIENT = (500, 502, 503, 504)   # errores que merecen reintento automático


def _parse_error(resp: httpx.Response) -> DFShaClientError:
    try:
        d = resp.json()
        extra = {k: v for k, v in d.items() if k not in ("code", "message", "operation_id", "retryable")}
        return DFShaClientError(resp.status_code, d.get("code", "ERROR"), d.get("message", resp.text[:120]),
                                bool(d.get("retryable")), d.get("operation_id"), extra)
    except ValueError:
        return DFShaClientError(resp.status_code, "ERROR", resp.text[:120])


class DFShaClient:
    def __init__(self, control: list[str] | None = None, ca: str | None = None,
                 home: str | None = None, parallel: int = 8, io_timeout: float = 30.0,
                 read_timeout: float = 3.0) -> None:
        self.home = Path(home or env("DFSHA_HOME") or Path.home() / ".dfsha")
        self.home.mkdir(parents=True, exist_ok=True)
        self.session: dict = self._load_session()
        self.control = [c.rstrip("/") for c in (control or env_list("DFSHA_CONTROL") or self.session.get("control") or ["https://localhost:4430"])]
        self.ca = ca or env("DFSHA_CA") or self.session.get("ca")
        self.parallel = parallel
        self.read_timeout = read_timeout   # inactividad máxima al leer un bloque antes de probar otra réplica (3 s, §6)
        self._ci = 0                       # índice de la última instancia de control que respondió bien
        if self.ca:
            ctx = client_ssl_context(TlsSettings(enabled=True, ca=self.ca), with_client_cert=False)
        else:
            ctx = ssl.create_default_context()
            ctx.minimum_version = ssl.TLSVersion.TLSv1_3
        self.http = httpx.Client(
            verify=ctx,
            timeout=httpx.Timeout(io_timeout, connect=3.0),     # connect de 3 s: detección rápida de nodo caído
            limits=httpx.Limits(max_connections=64, max_keepalive_connections=32, keepalive_expiry=4.0),  # < keep-alive del servidor
        )

    # ------------------------------------------------------------------ sesión local (~/.dfsha)
    @property
    def _session_file(self) -> Path:
        return self.home / "session.json"

    def _load_session(self) -> dict:
        try:
            return json.loads(self._session_file.read_text())
        except (OSError, ValueError):
            return {}

    def _save_session(self) -> None:
        self.session.update(control=self.control, ca=self.ca)
        tmp = self._session_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.session))
        os.chmod(tmp, 0o600)                      # contiene tokens: solo el dueño
        os.replace(tmp, self._session_file)

    @property
    def cwd(self) -> str:
        return self.session.get("cwd", "/")

    def resolve(self, path: str) -> str:
        """Ruta absoluta a partir de la ruta dada y el directorio actual (cd es estado local)."""
        base = path if path.startswith("/") else self.cwd.rstrip("/") + "/" + path
        parts: list[str] = []
        for seg in base.split("/"):
            if seg in ("", "."):
                continue
            if seg == "..":
                if parts:
                    parts.pop()
                continue
            parts.append(seg)
        return "/" + "/".join(parts)

    # ------------------------------------------------------------------ transporte al control
    def _control(self, method: str, path: str, *, json_body=None, params=None, auth: bool = True, retries: int = 5):
        """Petición al servicio de control con failover, backoff y renovación de token.

        - Error de red / 5xx: prueba la siguiente instancia; si todas fallan espera 0,5·2^k s (máx. 4 s).
        - 401 con refresh token: renueva el JWT UNA vez y repite.
        - La misma Idempotency-Key se reutiliza en todos los reintentos de una operación: si la
          primera petición sí llegó (pero perdimos la respuesta), el servidor devuelve el mismo resultado.
        """
        idem = "op-" + uuid.uuid4().hex[:12] if method in ("POST", "DELETE") else None
        refreshed, last = False, None
        for attempt in range(retries):
            n = len(self.control)
            for k in range(n):
                idx = (self._ci + k) % n
                headers = {}
                if auth and self.session.get("access_token"):
                    headers["Authorization"] = "Bearer " + self.session["access_token"]
                if idem:
                    headers["Idempotency-Key"] = idem
                try:
                    resp = self.http.request(method, self.control[idx] + path, json=json_body, params=params, headers=headers)
                except httpx.HTTPError as exc:
                    last = DFShaClientError(0, "NETWORK", f"sin conexión con {self.control[idx]}: {exc}", retryable=True)
                    continue
                if resp.status_code < 400:
                    self._ci = idx
                    return resp.json()
                err = _parse_error(resp)
                if err.status == 401 and auth and not refreshed and self.session.get("refresh_token"):
                    refreshed = True
                    self._refresh()
                    return self._control(method, path, json_body=json_body, params=params, auth=auth, retries=retries)
                if err.status in TRANSIENT:
                    last = err
                    continue
                raise err
            time.sleep(min(0.5 * 2 ** attempt, 4))
        raise last or DFShaClientError(0, "NETWORK", "no hay instancias de control disponibles", retryable=True)

    def _refresh(self) -> None:
        try:
            tokens = self._control("POST", "/v1/auth/refresh", json_body={"refresh_token": self.session["refresh_token"]}, auth=False)
        except DFShaClientError:
            self.session.pop("access_token", None)
            self.session.pop("refresh_token", None)
            self._save_session()
            raise
        self.session.update(access_token=tokens["access_token"], refresh_token=tokens["refresh_token"])
        self._save_session()

    # ------------------------------------------------------------------ cuenta
    def login(self, username: str, password: str) -> dict:
        tokens = self._control("POST", "/v1/auth/login", json_body={"username": username, "password": password}, auth=False)
        self.session.update(username=username, access_token=tokens["access_token"],
                            refresh_token=tokens["refresh_token"], cwd="/")
        self._save_session()
        return tokens

    def logout(self) -> None:
        for k in ("access_token", "refresh_token", "username"):
            self.session.pop(k, None)
        self._save_session()

    def useradd(self, username: str, password: str, role: str = "user") -> dict:
        return self._control("POST", "/v1/admin/users", json_body={"username": username, "password": password, "role": role})

    def userdel(self, username: str) -> dict:
        return self._control("DELETE", f"/v1/admin/users/{username}")

    # ------------------------------------------------------------------ RF1: namespace
    def ls(self, path: str = ".") -> list[dict]:
        return self._control("GET", "/v1/ls", params={"path": self.resolve(path)})["entries"]

    def stat(self, path: str) -> dict:
        return self._control("GET", "/v1/stat", params={"path": self.resolve(path)})

    def mkdir(self, path: str) -> bool:
        return self._control("POST", "/v1/mkdir", json_body={"path": self.resolve(path)})["created"]

    def rmdir(self, path: str) -> None:
        self._control("DELETE", "/v1/dir", params={"path": self.resolve(path)})

    def rm(self, path: str) -> bool:
        return self._control("DELETE", "/v1/file", params={"path": self.resolve(path)})["removed"]

    def cd(self, path: str) -> str:
        target = self.resolve(path)
        if self.stat(target)["type"] != "dir":
            raise DFShaClientError(409, "NOT_A_DIRECTORY", "no es un directorio")
        self.session["cwd"] = target
        self._save_session()
        return target

    # ------------------------------------------------------------------ RF3: handles
    def open(self, path: str, mode: str = "r", create: bool = False, truncate: bool = False,
             expected_version: int | None = None, lock_id: str | None = None, wait: float = 0) -> FileHandle:
        """open(). Con `wait` > 0 espera (con backoff) si otro cliente tiene el lock de escritura."""
        deadline = time.monotonic() + wait
        while True:
            try:
                info = self._control("POST", "/v1/files/open", json_body={
                    "path": self.resolve(path), "mode": mode, "create": create, "truncate": truncate,
                    "expected_version": expected_version, "lock_id": lock_id})
                return FileHandle(self, info, truncate)
            except DFShaClientError as exc:
                if exc.code != "LOCKED" or time.monotonic() >= deadline:
                    raise
                time.sleep(min(exc.extra.get("retry_after_s", 1), 2))

    def lock(self, path: str) -> dict:
        return self._control("POST", "/v1/locks", json_body={"path": self.resolve(path)})

    def renew_lock(self, lock_id: str) -> dict:
        return self._control("POST", f"/v1/locks/{lock_id}/renew")

    def unlock(self, lock_id: str) -> None:
        self._control("DELETE", f"/v1/locks/{lock_id}")

    # ------------------------------------------------------------------ plano de datos
    def _allocate(self, handle_id: str, indices: list[int], exclude: list[str] | None = None) -> list[dict]:
        return self._control("POST", f"/v1/handles/{handle_id}/allocate",
                             json_body={"indices": indices, "exclude": exclude or []})["blocks"]

    def _upload_block(self, handle_id: str, idx: int, data: bytes, alloc: dict) -> str:
        """Sube un bloque al primario (que lo replica). Si falla, pide OTRA asignación y reintenta.

        Es el mecanismo de la sección 8.2: si una réplica está caída, el primario responde 503
        NODE_UNAVAILABLE indicando el nodo fallido; el cliente lo excluye y el control elige otro.
        """
        sha, exclude, last, recycled = hashlib.sha256(data).hexdigest(), [], None, False
        for attempt in range(4):
            primary = alloc["primary"]
            try:
                resp = self.http.put(
                    f"{primary['endpoint']}/v1/blocks/{alloc['block_id']}", content=data,
                    headers={"Authorization": "Capability " + alloc["capability"], "X-Block-SHA256": sha,
                             "X-Block-Length": str(len(data)), "Content-Type": "application/octet-stream"})
            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                # no se pudo conectar o no contesta: el nodo se considera fallido y se pide otra asignación
                exclude.append(primary["node_id"])
                last = DFShaClientError(0, "NETWORK", f"nodo {primary['node_id']} no responde: {exc}", retryable=True)
            except httpx.TransportError as exc:
                # p. ej. "server disconnected": casi siempre una conexión keep-alive reciclada, NO un nodo caído.
                # El PUT de un bloque es idempotente, así que se repite UNA vez igual antes de culpar al nodo.
                last = DFShaClientError(0, "NETWORK", f"conexión interrumpida con {primary['node_id']}: {exc}", retryable=True)
                if not recycled:
                    recycled = True
                    continue
                exclude.append(primary["node_id"])
            else:
                if resp.status_code == 201:
                    return alloc["block_id"]
                err = _parse_error(resp)
                last = err
                if err.code == "NODE_UNAVAILABLE":
                    exclude.append(err.extra.get("failed_node") or primary["node_id"])
                elif err.status in (401, 422, 503, 507):
                    if err.status != 401:
                        exclude.append(primary["node_id"])
                else:
                    raise err
            time.sleep(0.2 * (attempt + 1))
            alloc = self._allocate(handle_id, [idx], exclude)[0]
        raise last

    def _download_block(self, entry: dict) -> bytes:
        """Descarga un bloque probando sus réplicas en orden y verificando el SHA-256."""
        last = None
        for node in entry["nodes"]:
            try:
                resp = self.http.get(f"{node['endpoint']}/v1/blocks/{entry['block_id']}",
                                     headers={"Authorization": "Capability " + node["capability"]},
                                     timeout=httpx.Timeout(max(self.read_timeout, 1.0), connect=3.0))
            except httpx.HTTPError as exc:
                last = DFShaClientError(0, "NETWORK", f"nodo {node['node_id']} no responde: {exc}", retryable=True)
                continue
            if resp.status_code == 200 and hashlib.sha256(resp.content).hexdigest() == entry["sha256"]:
                return resp.content
            last = _parse_error(resp) if resp.status_code != 200 else DFShaClientError(422, "INTEGRITY_CHECK_FAILED", "el hash del bloque no coincide")
        raise last or DFShaClientError(503, "BLOCK_UNAVAILABLE", f"ninguna réplica entrega el bloque {entry['index']}", retryable=True)

    def _manifest(self, handle_id: str, start: int, limit: int) -> dict:
        return self._control("GET", f"/v1/handles/{handle_id}/manifest", params={"from": start, "limit": limit})

    # ------------------------------------------------------------------ RF2: put / get
    def put(self, local: str, remote: str) -> dict:
        """put: sube un archivo completo (= open w + write completo + close). Devuelve {version, size, sha256}.

        Lee el archivo bloque a bloque (memoria acotada), calcula el hash total mientras lee y sube
        hasta `parallel` bloques a la vez. Si algo falla antes del close, NO se publica nada.
        """
        size = os.path.getsize(local)
        h = self.open(remote, "w", create=True, truncate=True)
        bs, total = h.block_size, hashlib.sha256()
        nblocks = math.ceil(size / bs)
        slots = threading.BoundedSemaphore(self.parallel * 2)     # bloques en vuelo (acota la memoria)
        errors: list[Exception] = []
        try:
            with open(local, "rb") as fh, ThreadPoolExecutor(self.parallel) as pool:
                for start in range(0, nblocks, 64):
                    idxs = list(range(start, min(start + 64, nblocks)))
                    for alloc in self._allocate(h.id, idxs):
                        data = fh.read(bs)
                        total.update(data)
                        slots.acquire()
                        if errors:
                            slots.release()
                            break
                        fut = pool.submit(self._upload_block, h.id, alloc["index"], data, alloc)
                        fut.add_done_callback(lambda f: (slots.release(), f.exception() and errors.append(f.exception())))
                    if errors:
                        break
            if errors:
                raise errors[0]
            digest = total.hexdigest()
            result = h._commit(size, digest)
        except BaseException:
            h.abort()
            raise
        return {**result, "sha256": digest}

    def get(self, remote: str, local: str) -> dict:
        """get: descarga un archivo completo en paralelo y verifica el hash. Devuelve {size, sha256}."""
        h = self.open(remote, "r")
        part = local + ".part"
        try:
            with open(part, "wb") as out:
                out.truncate(h.size)
                fd = out.fileno()
                with ThreadPoolExecutor(self.parallel) as pool:
                    start = 0
                    while start is not None:
                        page = self._manifest(h.id, start, 1000)
                        def fetch(entry):
                            os.pwrite(fd, self._download_block(entry), entry["index"] * h.block_size)
                        for _ in pool.map(fetch, page["blocks"]):
                            pass
                        start = page["next"]
            digest = hashlib.sha256()
            with open(part, "rb") as fh:
                for chunk in iter(lambda: fh.read(1 << 20), b""):
                    digest.update(chunk)
            if h.sha256_total and digest.hexdigest() != h.sha256_total:
                raise DFShaClientError(422, "INTEGRITY_CHECK_FAILED", "el hash total del archivo no coincide")
            os.replace(part, local)
        except BaseException:
            if os.path.exists(part):
                os.unlink(part)
            raise
        finally:
            h.close()
        return {"size": h.size, "sha256": digest.hexdigest()}
