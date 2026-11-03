"""Benchmark de DFSha: latencia, throughput, concurrencia, replicación, escalabilidad y fallos.

Cada experimento levanta su propio clúster local (procesos reales con TLS) y mide con tiempos de pared.
Cada medición se repite `--reps` veces tras 1 repetición de calentamiento; se reporta mediana y p95.

Experimentos (numeración del informe de diseño, sección 9):
  E1  integridad + rendimiento base: put/get de 1, 16 y 64 MiB (hash comprobado en cada repetición)
  E2  tamaño de bloque: 1, 4 y 16 MiB con un archivo de 64 MiB
  E3  concurrencia: 1, 2, 4 y 8 clientes simultáneos (cada uno con su archivo)
  E4  replicación: R=1 vs R=2
  E5  fallos: get con un nodo caído, y tiempo de recuperación hasta restablecer R=2
  E6  escalabilidad: 2 vs 3 nodos de datos con 4 clientes
  E7  latencia de operaciones de metadatos (mkdir / ls / stat)

Uso:   python scripts/benchmark.py [--reps 3] [--quick] [--only E1,E3] [--out results]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.local_cluster import LocalCluster  # noqa: E402

MiB = 1024 * 1024


def pct(values, p):
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p / 100 * (len(values) - 1))))]


def summarize(times, size_bytes=None):
    d = {"n": len(times), "median_s": round(statistics.median(times), 3), "p95_s": round(pct(times, 95), 3)}
    if size_bytes:
        d["throughput_MiBps"] = round(size_bytes / MiB / statistics.median(times), 2)
    return d


def make_file(directory, size, name="f.bin"):
    path = os.path.join(directory, name)
    with open(path, "wb") as fh:
        remaining = size
        while remaining:
            n = min(remaining, 8 * MiB)
            fh.write(os.urandom(n))
            remaining -= n
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for c in iter(lambda: fh.read(MiB), b""):
            h.update(c)
    return path, h.hexdigest()


def timed(fn):
    t0 = time.perf_counter()
    r = fn()
    return time.perf_counter() - t0, r


def cluster(tag, **kw):
    return LocalCluster(str(ROOT / "run" / "bench" / tag), base_port=kw.pop("base_port", 17000), **kw)


# ------------------------------------------------------------------------------------------ E1
def e1(reps, sizes):
    out = {}
    with tempfile.TemporaryDirectory() as tmp, cluster("e1", block_size=4 * MiB) as cl:
        c = cl.make_user("bench")
        for mb in sizes:
            path, sha = make_file(tmp, mb * MiB)
            put_t, get_t, ok = [], [], True
            for i in range(reps + 1):
                dt, _ = timed(lambda: c.put(path, f"/e1-{mb}-{i}.bin"))
                dg, g = timed(lambda: c.get(f"/e1-{mb}-{i}.bin", os.path.join(tmp, "o.bin")))
                ok &= g["sha256"] == sha
                if i:   # la repetición 0 es calentamiento
                    put_t.append(dt); get_t.append(dg)
            out[f"{mb}MiB"] = {"put": summarize(put_t, mb * MiB), "get": summarize(get_t, mb * MiB), "hash_ok": ok}
    return out


# ------------------------------------------------------------------------------------------ E2
def e2(reps, size_mb):
    out = {}
    with tempfile.TemporaryDirectory() as tmp:
        path, sha = make_file(tmp, size_mb * MiB)
        for bs in (1, 4, 16):
            with cluster(f"e2-{bs}", block_size=bs * MiB) as cl:
                c = cl.make_user("bench")
                put_t, get_t, ok = [], [], True
                for i in range(reps + 1):
                    dt, _ = timed(lambda: c.put(path, f"/f{i}.bin"))
                    dg, g = timed(lambda: c.get(f"/f{i}.bin", os.path.join(tmp, "o.bin")))
                    ok &= g["sha256"] == sha
                    if i:
                        put_t.append(dt); get_t.append(dg)
                nblocks = -(-size_mb // bs)
                out[f"bloque_{bs}MiB"] = {"bloques": nblocks, "put": summarize(put_t, size_mb * MiB), "get": summarize(get_t, size_mb * MiB), "hash_ok": ok}
    return out


# ------------------------------------------------------------------------------------------ E3 / E6
def run_clients(cl, n_clients, size_mb, reps, tmp):
    """n clientes simultáneos, cada uno sube y baja su propio archivo. Devuelve throughput agregado."""
    clients = [cl.make_user(f"u{i}") for i in range(n_clients)]
    files = [make_file(tmp, size_mb * MiB, f"in{i}.bin") for i in range(n_clients)]
    put_t, get_t, ok = [], [], True
    for rep in range(reps + 1):
        for phase in ("put", "get"):
            results = [None] * n_clients

            def work(i):
                path, sha = files[i]
                if phase == "put":
                    clients[i].put(path, f"/c{rep}.bin")
                else:
                    results[i] = clients[i].get(f"/c{rep}.bin", os.path.join(tmp, f"o{i}.bin"))["sha256"] == sha
            threads = [threading.Thread(target=work, args=(i,)) for i in range(n_clients)]
            t0 = time.perf_counter()
            [t.start() for t in threads]; [t.join() for t in threads]
            dt = time.perf_counter() - t0
            if phase == "get":
                ok &= all(results)
            if rep:
                (put_t if phase == "put" else get_t).append(dt)
    total = n_clients * size_mb * MiB
    return {"clientes": n_clients, "put": summarize(put_t, total), "get": summarize(get_t, total), "hash_ok": ok}


def e3(reps, size_mb, clients_list):
    out = {}
    with tempfile.TemporaryDirectory() as tmp, cluster("e3", block_size=4 * MiB) as cl:
        for n in clients_list:
            out[f"{n}_clientes"] = run_clients(cl, n, size_mb, reps, tmp)
    return out


def e4(reps, size_mb):
    out = {}
    with tempfile.TemporaryDirectory() as tmp:
        path, sha = make_file(tmp, size_mb * MiB)
        for r in (1, 2):
            with cluster(f"e4-{r}", block_size=4 * MiB, extra_env={"DFSHA_REPLICATION": str(r)}) as cl:
                c = cl.make_user("bench")
                put_t, get_t, ok = [], [], True
                for i in range(reps + 1):
                    dt, _ = timed(lambda: c.put(path, f"/f{i}.bin"))
                    dg, g = timed(lambda: c.get(f"/f{i}.bin", os.path.join(tmp, "o.bin")))
                    ok &= g["sha256"] == sha
                    if i:
                        put_t.append(dt); get_t.append(dg)
                phys = sum(p.stat().st_size for p in (cl.dir / "data").rglob("*.blk"))
                out[f"R={r}"] = {"put": summarize(put_t, size_mb * MiB), "get": summarize(get_t, size_mb * MiB),
                                 "bytes_fisicos_sobre_logicos": round(phys / ((reps + 1) * size_mb * MiB), 2), "hash_ok": ok}
    return out


def e5(size_mb, reps):
    out = {}
    with tempfile.TemporaryDirectory() as tmp, cluster("e5", block_size=4 * MiB) as cl:
        c = cl.make_user("bench")
        path, sha = make_file(tmp, size_mb * MiB)
        c.put(path, "/f.bin")
        normal = [timed(lambda: c.get("/f.bin", os.path.join(tmp, "o.bin")))[0] for _ in range(reps)]
        cl.kill("dn-b")
        caido, ok = [], True
        for _ in range(reps):
            dt, g = timed(lambda: c.get("/f.bin", os.path.join(tmp, "o.bin")))
            caido.append(dt); ok &= g["sha256"] == sha
        t_kill = time.perf_counter()
        cl.wait_for(lambda: {n["node_id"]: n["state"] for n in cl.status()["nodes"]}["dn-b"] == "DEAD", timeout=60, what="DEAD")
        t_dead = time.perf_counter() - t_kill

        def reparado():
            h = c.open("/f.bin", "r")
            try:
                bl = c._manifest(h.id, 0, 1000)["blocks"]
            finally:
                h.close()
            return all(len(b["nodes"]) == 2 and all(n["node_id"] != "dn-b" for n in b["nodes"]) for b in bl)
        cl.wait_for(reparado, timeout=120, what="reparación")
        t_rep = time.perf_counter() - t_kill
        out = {"get_normal": summarize(normal, size_mb * MiB), "get_con_nodo_caido": summarize(caido, size_mb * MiB), "hash_ok": ok,
               "deteccion_DEAD_s": round(t_dead, 1), "recuperacion_R2_total_s": round(t_rep, 1),
               "nota": "umbrales de prueba: SUSPECT=4 s, DEAD=10 s (producción: 15 s / 120 s)"}
    return out


def e6(reps, size_mb):
    out = {}
    with tempfile.TemporaryDirectory() as tmp:
        for nodes in (2, 3):
            with cluster(f"e6-{nodes}", block_size=4 * MiB, n_nodes=nodes) as cl:
                out[f"{nodes}_nodos"] = run_clients(cl, 4, size_mb, reps, tmp)
    return out


def e7(n=50):
    with cluster("e7") as cl:
        c = cl.make_user("bench")
        c.mkdir("/d")
        res = {}
        for name, fn in (("mkdir", lambda i: c.mkdir(f"/d/m{i}")), ("ls", lambda i: c.ls("/d")), ("stat", lambda i: c.stat("/d"))):
            ts = [timed(lambda: fn(i))[0] * 1000 for i in range(n)]
            res[name] = {"n": n, "median_ms": round(statistics.median(ts), 1), "p95_ms": round(pct(ts, 95), 1)}
    return res


def to_markdown(results: dict) -> str:
    lines = ["# Resultados del benchmark", "", f"_Generado el {time.strftime('%Y-%m-%d %H:%M')}. Procesos reales en localhost "
             f"({os.cpu_count()} CPU compartidas entre clientes, 3 etcd, 3 control y los nodos de datos): los valores son comparables "
             "entre sí pero **no** representan el rendimiento en AWS._", ""]
    for exp, data in results.items():
        lines += [f"## {exp}", "", "```json", json.dumps(data, indent=2, ensure_ascii=False), "```", ""]
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--quick", action="store_true", help="tamaños reducidos para una prueba rápida")
    ap.add_argument("--only", default="")
    ap.add_argument("--out", default=str(ROOT / "results"))
    a = ap.parse_args()
    wanted = set(filter(None, a.only.upper().split(",")))
    big = 16 if a.quick else 64
    runs = {
        "E1_base": lambda: e1(a.reps, [1, 8] if a.quick else [1, 16, 64]),
        "E2_tamano_bloque": lambda: e2(a.reps, big),
        "E3_concurrencia": lambda: e3(a.reps, 16 if a.quick else 32, [1, 2, 4] if a.quick else [1, 2, 4, 8]),
        "E4_replicacion": lambda: e4(a.reps, big),
        "E5_fallos": lambda: e5(big, a.reps),
        "E6_escalabilidad": lambda: e6(a.reps, 16 if a.quick else 32),
        "E7_metadatos": lambda: e7(),
    }
    results = {}
    for name, fn in runs.items():
        if wanted and name.split("_")[0] not in wanted:
            continue
        print(f"== {name} ...", flush=True)
        t0 = time.time()
        results[name] = fn()
        print(json.dumps(results[name], indent=1, ensure_ascii=False)[:1500], f"\n   ({time.time() - t0:.0f} s)", flush=True)
    os.makedirs(a.out, exist_ok=True)
    Path(a.out, "benchmark.json").write_text(json.dumps(results, indent=2, ensure_ascii=False))
    Path(a.out, "benchmark.md").write_text(to_markdown(results))
    print(f"\nResultados en {a.out}/benchmark.json y benchmark.md")


if __name__ == "__main__":
    main()
