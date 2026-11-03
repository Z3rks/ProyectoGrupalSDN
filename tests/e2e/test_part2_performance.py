"""Rendimiento (PARTE 2): medición rápida bajo DOS condiciones, con integridad comprobada.

Prueba mínima del enunciado: "Mediciones de latencia/throughput y comparación bajo al menos dos condiciones".
(Los experimentos completos E1–E7 están en scripts/benchmark.py.)
"""
import hashlib
import json
import os
import statistics
import threading
import time

from tests.conftest import ROOT


def test_throughput_con_1_y_con_4_clientes_concurrentes_conserva_la_integridad(cluster, tmp_path):
    MiB = 1024 * 1024
    size = 6 * MiB
    data = os.urandom(size)
    sha = hashlib.sha256(data).hexdigest()
    src = tmp_path / "in.bin"; src.write_bytes(data)
    users = [cluster.make_user(f"perf{i}") for i in range(4)]
    resultados = {}

    for n in (1, 4):                                            # condición 1: un cliente; condición 2: cuatro a la vez
        tiempos_put, tiempos_get, ok = [], [], []

        def trabajo(i):
            t0 = time.perf_counter(); users[i].put(str(src), f"/perf-{n}.bin"); tiempos_put.append(time.perf_counter() - t0)
            t0 = time.perf_counter(); g = users[i].get(f"/perf-{n}.bin", str(tmp_path / f"o{n}-{i}.bin")); tiempos_get.append(time.perf_counter() - t0)
            ok.append(g["sha256"] == sha)

        hilos = [threading.Thread(target=trabajo, args=(i,)) for i in range(n)]
        t0 = time.perf_counter(); [h.start() for h in hilos]; [h.join() for h in hilos]; total = time.perf_counter() - t0
        assert all(ok) and len(ok) == n                         # integridad 100 %
        resultados[f"{n}_clientes"] = {
            "latencia_put_mediana_s": round(statistics.median(tiempos_put), 3),
            "latencia_get_mediana_s": round(statistics.median(tiempos_get), 3),
            "throughput_agregado_MiBps": round(2 * n * size / MiB / total, 2),
        }
    out = ROOT / "run" / "results"; out.mkdir(parents=True, exist_ok=True)
    (out / "perf_quick.json").write_text(json.dumps(resultados, indent=2))
    print("\n", json.dumps(resultados, indent=2))
    assert all(r["throughput_agregado_MiBps"] > 0 for r in resultados.values())
