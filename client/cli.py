"""CLI de DFSha:   python -m client <comando> [opciones]

Comandos (RF1, RF2, RF3):
  login / logout / whoami           sesión (el token se guarda en ~/.dfsha/session.json)
  pwd / cd / ls / stat              navegación
  mkdir / rmdir / rm                gestión de directorios y archivos
  put LOCAL REMOTO / get REMOTO LOCAL    transferencia de archivos completos
  cat REMOTO [--offset N --length N]     read():  lee un rango
  write REMOTO --offset N --data TXT     write(): modifica un rango (crea con --create)
  append REMOTO --data TXT               escribe al final
  lock REMOTO [--hold S] / unlock ID     bloqueo explícito
  useradd / userdel                      administración (rol admin)
  shell                                  modo interactivo (permite usar `cd`)

Variables útiles: DFSHA_CONTROL (URLs del control separadas por coma), DFSHA_CA (CA para verificar TLS),
DFSHA_HOME (carpeta de la sesión; permite varios usuarios en la misma máquina).
"""
from __future__ import annotations

import argparse
import getpass
import shlex
import sys
import time
from datetime import datetime

from .api import DFShaClient
from .errors import DFShaClientError


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="dfsha", description="Cliente del sistema de archivos distribuido DFSha")
    p.add_argument("--control", help="URL(s) del servicio de control, separadas por coma")
    p.add_argument("--ca", help="certificado de la CA para verificar TLS")
    p.add_argument("--home", help="carpeta de sesión (por defecto ~/.dfsha)")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("login"); s.add_argument("username"); s.add_argument("--password")
    sub.add_parser("logout"); sub.add_parser("whoami"); sub.add_parser("pwd")
    s = sub.add_parser("cd"); s.add_argument("path")
    s = sub.add_parser("ls"); s.add_argument("path", nargs="?", default=".")
    s = sub.add_parser("stat"); s.add_argument("path")
    s = sub.add_parser("mkdir"); s.add_argument("path")
    s = sub.add_parser("rmdir"); s.add_argument("path")
    s = sub.add_parser("rm"); s.add_argument("path")
    s = sub.add_parser("put"); s.add_argument("local"); s.add_argument("remote")
    s = sub.add_parser("get"); s.add_argument("remote"); s.add_argument("local")
    s = sub.add_parser("cat"); s.add_argument("remote"); s.add_argument("--offset", type=int, default=0); s.add_argument("--length", type=int, default=-1)
    s = sub.add_parser("write"); s.add_argument("remote"); s.add_argument("--offset", type=int, default=0)
    g = s.add_mutually_exclusive_group(required=True); g.add_argument("--data"); g.add_argument("--file")
    s.add_argument("--create", action="store_true"); s.add_argument("--wait", type=float, default=0, help="segundos a esperar si el archivo está bloqueado")
    s = sub.add_parser("append"); s.add_argument("remote"); s.add_argument("--data", required=True)
    s.add_argument("--create", action="store_true"); s.add_argument("--wait", type=float, default=0)
    s = sub.add_parser("lock"); s.add_argument("remote"); s.add_argument("--hold", type=float, default=0, help="mantener el lock N segundos y liberarlo")
    s = sub.add_parser("unlock"); s.add_argument("lock_id")
    s = sub.add_parser("useradd"); s.add_argument("username"); s.add_argument("--password"); s.add_argument("--role", default="user", choices=["user", "admin"])
    s = sub.add_parser("userdel"); s.add_argument("username")
    sub.add_parser("shell")
    return p


def _fmt_entry(e: dict) -> str:
    when = datetime.fromtimestamp(e["mtime"]).strftime("%Y-%m-%d %H:%M") if e["mtime"] else "-"
    if e["type"] == "dir":
        return f"d {'-':>12} {'-':>4} {when:<16} {e['name']}/"
    return f"- {e['size']:>12} v{e['version']:<3} {when:<16} {e['name']}"


def _run(c: DFShaClient, a: argparse.Namespace) -> int:
    cmd = a.cmd
    if cmd == "login":
        c.login(a.username, a.password or getpass.getpass("Contraseña: "))
        print(f"sesión iniciada como {a.username}")
    elif cmd == "logout":
        c.logout(); print("sesión cerrada")
    elif cmd == "whoami":
        print(c.session.get("username", "(sin sesión)"))
    elif cmd == "pwd":
        print(c.cwd)
    elif cmd == "cd":
        print(c.cd(a.path))
    elif cmd == "ls":
        for e in c.ls(a.path):
            print(_fmt_entry(e))
    elif cmd == "stat":
        for k, v in c.stat(a.path).items():
            print(f"{k}: {v}")
    elif cmd == "mkdir":
        print("creado" if c.mkdir(a.path) else "ya existía")
    elif cmd == "rmdir":
        c.rmdir(a.path); print("directorio eliminado")
    elif cmd == "rm":
        print("eliminado" if c.rm(a.path) else "no existía")
    elif cmd == "put":
        t0 = time.monotonic(); r = c.put(a.local, a.remote); dt = time.monotonic() - t0
        print(f"subido {a.remote}: v{r['version']}, {r['size']} bytes, sha256={r['sha256']} ({r['size'] / 1048576 / max(dt, 1e-9):.1f} MiB/s)")
    elif cmd == "get":
        t0 = time.monotonic(); r = c.get(a.remote, a.local); dt = time.monotonic() - t0
        print(f"descargado {a.local}: {r['size']} bytes, sha256={r['sha256']} ({r['size'] / 1048576 / max(dt, 1e-9):.1f} MiB/s)")
    elif cmd == "cat":
        with c.open(a.remote, "r") as h:
            n = (h.size - a.offset) if a.length < 0 else a.length
            sys.stdout.buffer.write(h.read(a.offset, n)); sys.stdout.buffer.flush()
    elif cmd == "write":
        data = a.data.encode() if a.data is not None else open(a.file, "rb").read()
        with c.open(a.remote, "w", create=a.create, wait=a.wait) as h:
            h.write(a.offset, data)
        print(f"escritos {len(data)} bytes en {a.remote} (offset {a.offset})")
    elif cmd == "append":
        with c.open(a.remote, "a", create=a.create, wait=a.wait) as h:
            h.append(a.data.encode())
        print(f"añadidos {len(a.data.encode())} bytes a {a.remote}")
    elif cmd == "lock":
        lk = c.lock(a.remote)
        print(f"lock_id={lk['lock_id']} fencing={lk['fencing']} (vence en {lk['expires_in']} s si no se renueva)")
        if a.hold:
            end = time.monotonic() + a.hold
            while time.monotonic() < end:
                time.sleep(min(10, max(end - time.monotonic(), 0)))
                if time.monotonic() < end:
                    c.renew_lock(lk["lock_id"])
            c.unlock(lk["lock_id"]); print("lock liberado")
    elif cmd == "unlock":
        c.unlock(a.lock_id); print("lock liberado")
    elif cmd == "useradd":
        c.useradd(a.username, a.password or getpass.getpass("Contraseña del nuevo usuario: "), a.role); print(f"usuario {a.username} creado")
    elif cmd == "userdel":
        c.userdel(a.username); print(f"usuario {a.username} eliminado")
    return 0


def _shell(c: DFShaClient, parser: argparse.ArgumentParser) -> None:
    print("DFSha shell. Escriba 'exit' para salir.")
    while True:
        try:
            line = input(f"dfsha:{c.cwd}> ").strip()
        except (EOFError, KeyboardInterrupt):
            print(); return
        if not line:
            continue
        if line in ("exit", "quit"):
            return
        try:
            args = parser.parse_args(shlex.split(line))
            if args.cmd == "shell":
                continue
            _run(c, args)
        except SystemExit:
            pass
        except DFShaClientError as exc:
            print(f"error: {exc}")
        except (OSError, ValueError) as exc:
            print(f"error: {exc}")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    control = [u.strip() for u in args.control.split(",")] if args.control else None
    client = DFShaClient(control=control, ca=args.ca, home=args.home)
    try:
        if args.cmd == "shell":
            _shell(client, parser)
            return 0
        return _run(client, args)
    except DFShaClientError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
