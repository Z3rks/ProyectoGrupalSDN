"""Punto de entrada del nodo de datos:   python -m datanode.main"""
from __future__ import annotations

import asyncio
import logging

from common.server import Listener, run_listeners

from .app import create_apps
from .settings import load


def main() -> None:
    cfg = load()
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s %(levelname)s [{cfg.node_id}] %(name)s: %(message)s",
    )
    public, internal, _ = create_apps(cfg)
    asyncio.run(
        run_listeners(
            [
                Listener(public, cfg.public_port, mtls=False, host=cfg.host, lifespan=True),
                Listener(internal, cfg.internal_port, mtls=True, host=cfg.host),
            ],
            cfg.tls,
        )
    )


if __name__ == "__main__":
    main()
