"""Elección de líder y tareas de fondo (detector de fallos, reparación, GC).

Las 3 instancias de control están activas y atienden peticiones, pero ciertas tareas deben
ejecutarlas UNA sola instancia a la vez (si no, repararían o borrarían por duplicado). Se elige
un líder con el mecanismo estándar de etcd:

    /leader  =  <id de la instancia>    atada a un LEASE de 10 s

  * Todas intentan crear /leader con la transacción "si no existe". Solo una gana.
  * El líder renueva el lease cada ~3 s. Si muere, el lease expira en <= 10 s, la clave
    desaparece y otra instancia toma el relevo (sin que nadie intervenga).
  * Si el líder pierde el contacto con etcd, deja de actuar como líder (is_leader=False)
    antes de que otro pueda ser elegido: evita dos líderes simultáneos.
"""
from __future__ import annotations

import asyncio
import logging

from . import gc as gcmod
from . import nodes as nodesmod
from . import repair as repairmod
from .etcd import EtcdError, EtcdUnavailable, LeaseNotFound, c_absent, o_put
from .state import State

log = logging.getLogger("dfsha.leader")


class LeaderService:
    def __init__(self, st: State) -> None:
        self.st = st
        self.is_leader = False
        self._lease = 0
        self._tasks: list[asyncio.Task] = []
        self._repair_now = asyncio.Event()
        self._prev_states: dict[str, str] = {}

    # ------------------------------------------------------------------ ciclo de vida
    def start(self) -> None:
        self._tasks = [
            asyncio.create_task(self._election_loop(), name="election"),
            asyncio.create_task(self._failure_detector_loop(), name="detector"),
            asyncio.create_task(self._repair_loop(), name="repair"),
            asyncio.create_task(self._gc_loop(), name="gc"),
        ]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._lease:
            try:
                await self.st.etcd.lease_revoke(self._lease)   # cede el liderazgo al apagarse
            except EtcdError:
                pass

    # ------------------------------------------------------------------ elección
    async def _election_loop(self) -> None:
        st = self.st
        while True:
            try:
                await self._election_step()
            except EtcdUnavailable:
                if self.is_leader:
                    log.warning("sin contacto con etcd: se deja el liderazgo")
                self.is_leader = False
            except Exception:
                log.exception("error en la elección de líder")
            await asyncio.sleep(st.cfg.leader_ttl_s / 3)

    async def _election_step(self) -> None:
        st = self.st
        if self._lease:
            try:
                await st.etcd.lease_keepalive(self._lease)
                self.is_leader = True
                return
            except LeaseNotFound:
                self._lease, self.is_leader = 0, False
        lease = await st.etcd.lease_grant(st.cfg.leader_ttl_s)
        res = await st.etcd.txn([c_absent("/leader")], [o_put("/leader", st.cfg.instance_id, lease)])
        if res.succeeded:
            self._lease, self.is_leader = lease, True
            log.info("%s es ahora el LÍDER de control", st.cfg.instance_id)
        else:
            await st.etcd.lease_revoke(lease)
            self.is_leader = False

    # ------------------------------------------------------------------ detector de fallos
    async def _failure_detector_loop(self) -> None:
        """Registra los cambios HEALTHY -> SUSPECT -> DEAD y adelanta la reparación si un nodo muere."""
        while True:
            await asyncio.sleep(2)
            if not self.is_leader:
                continue
            try:
                for n in await nodesmod.list_nodes(self.st):
                    prev = self._prev_states.get(n.node_id)
                    if prev != n.state:
                        log.warning("nodo %s: %s -> %s", n.node_id, prev or "?", n.state)
                        self._prev_states[n.node_id] = n.state
                        if n.state == nodesmod.DEAD:
                            self._repair_now.set()
            except EtcdError:
                pass
            except Exception:
                log.exception("error en el detector de fallos")

    async def _repair_loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._repair_now.wait(), timeout=self.st.cfg.repair_interval_s)
            except asyncio.TimeoutError:
                pass
            self._repair_now.clear()
            if not self.is_leader:
                continue
            try:
                await repairmod.repair_pass(self.st)
            except EtcdError:
                pass
            except Exception:
                log.exception("error en la reparación")

    async def _gc_loop(self) -> None:
        while True:
            await asyncio.sleep(self.st.cfg.gc_interval_s)
            if not self.is_leader:
                continue
            try:
                await gcmod.gc_pass(self.st)
            except EtcdError:
                pass
            except Exception:
                log.exception("error en el GC")
