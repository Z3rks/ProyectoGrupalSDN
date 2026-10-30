"""Cliente mínimo de etcd v3 usando su API HTTP/JSON (el "gRPC-gateway").

¿Por qué HTTP/JSON y no gRPC? Evita dependencias nativas, funciona con httpx (ya lo usamos)
y se puede depurar con curl. Solo necesitamos 5 capacidades de etcd:

  1. get / range        -> leer claves o prefijos.
  2. put / delete       -> escribir y borrar (opcionalmente atadas a un LEASE).
  3. txn                -> transacciones "si <condiciones> entonces <acciones>" (compare-and-swap).
                           Es la base de los locks, del commit atómico de versiones y del
                           fencing: todo el diseño de consistencia descansa en esto.
  4. lease              -> claves que desaparecen solas si nadie renueva (locks y handles).
  5. múltiples endpoints -> si un miembro de etcd cae, se prueba el siguiente.

Convenciones del gateway: claves y valores viajan en base64 y los enteros de 64 bits como texto.
"""
from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field

import httpx


class EtcdError(Exception):
    """Error devuelto por etcd (petición inválida, lease inexistente, ...)."""


class EtcdUnavailable(EtcdError):
    """No hay quórum o no se pudo contactar a ningún miembro (el control responde 503)."""


class LeaseNotFound(EtcdError):
    """El lease ya expiró o fue revocado."""


def _b64(value: str | bytes) -> str:
    if isinstance(value, str):
        value = value.encode()
    return base64.b64encode(value).decode()


def _unb64(text: str) -> str:
    return base64.b64decode(text).decode()


def prefix_end(prefix: str) -> str:
    """`range_end` (en base64) que cubre todas las claves que empiezan por `prefix`."""
    raw = bytearray(prefix.encode())
    for i in range(len(raw) - 1, -1, -1):
        if raw[i] < 0xFF:
            raw[i] += 1
            return _b64(bytes(raw[: i + 1]))
    return _b64(b"\x00")


def dumps(obj) -> str:
    """JSON compacto y estable: el mismo objeto produce siempre el mismo texto."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


@dataclass
class KV:
    """Una clave de etcd con sus revisiones (create_rev = 0 significa 'no existe')."""

    key: str
    value: str
    create_rev: int = 0
    mod_rev: int = 0
    lease: int = 0

    @property
    def json(self):
        return json.loads(self.value)


def _kv(raw: dict) -> KV:
    return KV(
        key=_unb64(raw["key"]),
        value=_unb64(raw.get("value", "")),
        create_rev=int(raw.get("create_revision", 0)),
        mod_rev=int(raw.get("mod_revision", 0)),
        lease=int(raw.get("lease", 0)),
    )


# --------------------------------------------------------------------------------------
# Constructores de condiciones (compare) y operaciones (ops) para las transacciones
# --------------------------------------------------------------------------------------
def c_absent(key: str) -> dict:
    """Condición: la clave NO existe."""
    return {"key": _b64(key), "target": "CREATE", "result": "EQUAL", "create_revision": "0"}


def c_exists(key: str) -> dict:
    """Condición: la clave existe."""
    return {"key": _b64(key), "target": "CREATE", "result": "GREATER", "create_revision": "0"}


def c_mod(key: str, rev: int) -> dict:
    """Condición: la clave no ha cambiado desde la revisión `rev` (CAS optimista).

    Con rev=0 equivale a 'no existe'.
    """
    return {"key": _b64(key), "target": "MOD", "result": "EQUAL", "mod_revision": str(rev)}


def o_put(key: str, value: str, lease: int = 0) -> dict:
    return {"request_put": {"key": _b64(key), "value": _b64(value), "lease": str(lease)}}


def o_del(key: str, prefix: bool = False) -> dict:
    req = {"key": _b64(key)}
    if prefix:
        req["range_end"] = prefix_end(key)
    return {"request_delete_range": req}


def o_get(key: str) -> dict:
    return {"request_range": {"key": _b64(key)}}


@dataclass
class TxnResult:
    succeeded: bool
    revision: int  # revisión global tras la transacción (la usamos como token de fencing)
    responses: list = field(default_factory=list)

    def kvs(self, i: int) -> list[KV]:
        """KVs devueltos por la operación i (si era un o_get)."""
        rr = self.responses[i].get("response_range", {})
        return [_kv(k) for k in rr.get("kvs", [])]


class EtcdClient:
    """Cliente asíncrono con failover entre los endpoints de etcd."""

    MAX_OPS = 100  # etcd limita 128 operaciones por transacción; dejamos margen

    def __init__(self, endpoints: list[str], http: httpx.AsyncClient) -> None:
        self.endpoints = [e.rstrip("/") for e in endpoints]
        self._http = http
        self._current = 0

    async def _post(self, path: str, body: dict) -> dict:
        """POST con failover: ante error de red o 5xx prueba el siguiente miembro."""
        last: Exception | None = None
        n = len(self.endpoints)
        for i in range(n):
            idx = (self._current + i) % n
            try:
                resp = await self._http.post(self.endpoints[idx] + path, json=body)
            except httpx.HTTPError as exc:
                last = exc
                continue
            if resp.status_code >= 500:
                last = EtcdUnavailable(resp.text[:200])
                continue
            if resp.status_code != 200:
                msg = resp.text[:200]
                if "lease not found" in msg:
                    raise LeaseNotFound(msg)
                raise EtcdError(msg)
            self._current = idx
            return resp.json()
        raise EtcdUnavailable(f"ningún miembro de etcd responde: {last}")

    # ---------------------------------------------------------------- lecturas
    async def get(self, key: str) -> KV | None:
        data = await self._post("/v3/kv/range", {"key": _b64(key)})
        kvs = data.get("kvs", [])
        return _kv(kvs[0]) if kvs else None

    async def range(
        self, prefix: str, *, start: str | None = None, limit: int = 0
    ) -> tuple[list[KV], bool]:
        """Claves con el prefijo dado, ordenadas. `start` permite paginar. Devuelve (kvs, hay_más)."""
        body = {
            "key": _b64(start if start is not None else prefix),
            "range_end": prefix_end(prefix),
            "limit": str(limit),
        }
        data = await self._post("/v3/kv/range", body)
        return [_kv(k) for k in data.get("kvs", [])], bool(data.get("more", False))

    async def range_all(self, prefix: str, page: int = 500) -> list[KV]:
        """Todas las claves de un prefijo, leyendo por páginas."""
        out: list[KV] = []
        start = prefix
        while True:
            kvs, more = await self.range(prefix, start=start, limit=page)
            out.extend(kvs)
            if not more or not kvs:
                return out
            start = kvs[-1].key + "\x00"

    async def get_many(self, keys: list[str]) -> dict[str, KV]:
        """Lee muchas claves con pocas peticiones (lotes de MAX_OPS en una transacción)."""
        found: dict[str, KV] = {}
        for i in range(0, len(keys), self.MAX_OPS):
            chunk = keys[i : i + self.MAX_OPS]
            res = await self.txn([], [o_get(k) for k in chunk])
            for j in range(len(chunk)):
                for kv in res.kvs(j):
                    found[kv.key] = kv
        return found

    # ---------------------------------------------------------------- escrituras
    async def put(self, key: str, value: str, lease: int = 0) -> None:
        await self._post("/v3/kv/put", {"key": _b64(key), "value": _b64(value), "lease": str(lease)})

    async def delete(self, key: str, prefix: bool = False) -> None:
        body = {"key": _b64(key)}
        if prefix:
            body["range_end"] = prefix_end(key)
        await self._post("/v3/kv/deleterange", body)

    async def txn(self, compare: list[dict], success: list[dict], failure: list[dict] | None = None) -> TxnResult:
        """Transacción atómica: si TODAS las condiciones se cumplen aplica `success`, si no `failure`."""
        data = await self._post(
            "/v3/kv/txn", {"compare": compare, "success": success, "failure": failure or []}
        )
        return TxnResult(
            succeeded=bool(data.get("succeeded", False)),
            revision=int(data.get("header", {}).get("revision", 0)),
            responses=data.get("responses", []),
        )

    # ---------------------------------------------------------------- leases
    async def lease_grant(self, ttl: int) -> int:
        data = await self._post("/v3/lease/grant", {"TTL": str(ttl)})
        return int(data["ID"])

    async def lease_keepalive(self, lease_id: int) -> int:
        """Renueva el lease. Devuelve el TTL restante; lanza LeaseNotFound si ya expiró."""
        data = await self._post("/v3/lease/keepalive", {"ID": str(lease_id)})
        ttl = int(data.get("result", {}).get("TTL", 0))
        if ttl <= 0:
            raise LeaseNotFound(f"lease {lease_id} expirado")
        return ttl

    async def lease_revoke(self, lease_id: int) -> None:
        """Revoca el lease (borra todas las claves atadas). Silencioso si ya no existe."""
        try:
            await self._post("/v3/lease/revoke", {"ID": str(lease_id)})
        except LeaseNotFound:
            pass
