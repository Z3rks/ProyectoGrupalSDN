"""Empaqueta el proyecto en dos zips sin archivos en común (ver docs/REPARTO.md).

Uso:  python scripts/make_parts.py [--out entrega]
Crea  entrega/parte1_control.zip  y  entrega/parte2_datos_cliente.zip  con las rutas relativas del repositorio.
Falla si algún archivo del proyecto no pertenece a ninguna parte o pertenece a las dos.
"""
from __future__ import annotations

import argparse
import fnmatch
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

PART1 = [
    "common/*.py", "control/*.py",
    "tests/unit/test_common_and_control_pure.py", "tests/e2e/test_part1_*.py",
    "requirements.txt", "pytest.ini", ".gitignore", "README.md",
    "docs/REPARTO.md", "docs/DECISIONES.md", "docs/CODIGO_PARTE1_CONTROL.md",
]
PART2 = [
    "datanode/*.py", "client/*.py", "deploy/*", "deploy/aws/*", "scripts/*.py",
    "tests/__init__.py", "tests/conftest.py", "tests/unit/__init__.py", "tests/e2e/__init__.py",
    "tests/unit/test_blockstore.py", "tests/e2e/test_part2_*.py",
    "docs/CODIGO_PARTE2_DATOS_CLIENTE.md", "docs/DESPLIEGUE_AWS.md", "docs/PRUEBAS.md",
]
IGNORE_DIRS = {"__pycache__", "run", "results", "entrega", ".git", ".pytest_cache"}


def project_files() -> list[str]:
    out = []
    for p in ROOT.rglob("*"):
        rel = p.relative_to(ROOT)
        if p.is_file() and not (set(rel.parts) & IGNORE_DIRS) and p.suffix != ".pyc":
            out.append(rel.as_posix())
    return sorted(out)


def belongs(rel: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(rel, pat) for pat in patterns)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="entrega")
    args = ap.parse_args()
    files = project_files()
    p1 = [f for f in files if belongs(f, PART1)]
    p2 = [f for f in files if belongs(f, PART2)]
    both = set(p1) & set(p2)
    none = [f for f in files if f not in set(p1) | set(p2)]
    if both or none:
        raise SystemExit(f"Reparto inconsistente. En ambas: {sorted(both)}  En ninguna: {none}")
    out = ROOT / args.out
    out.mkdir(exist_ok=True)
    for name, group in (("parte1_control.zip", p1), ("parte2_datos_cliente.zip", p2)):
        with zipfile.ZipFile(out / name, "w", zipfile.ZIP_DEFLATED) as z:
            for rel in group:
                z.write(ROOT / rel, rel)
        print(f"{out / name}: {len(group)} archivos")


if __name__ == "__main__":
    main()
