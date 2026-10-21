"""The web demo: a live map of the vans, the exceptions and their workflows,
the agents, and the approval desk.

    python -m fleet.server

It starts a new day (a clean stream), runs the simulator and the agents under
a supervisor, and tails the positions and events topics straight off the log
for the browser. The person on the desk (the approver) lives here too: the
invoice agent's approval gate lands on this server and waits for a click.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import random
import time
from collections import deque
from pathlib import Path

import laser_sdk as ls
from aiohttp import web

from . import config as C
from . import roads
from . import timeline
from .memory import CustomerMemory
from .core import Fleet, connect, get_json, put_json, reset, scan_json, setup
from .chaos import ChaosMonkey
from .demo import DemoLoop
from .supervisor import Supervisor

STATIC = Path(__file__).resolve().parent / "static"
AUTO_DECIDE = float(os.environ.get("DESK_AUTO_DECIDE", "0") or 0)  # seconds; 0 = wait for a person


class Desk:
    def __init__(self) -> None:
        self.laser: ls.Laser | None = None
        self.fleet: Fleet | None = None
        self.sup: Supervisor | None = None
        self.approver = None
        self.tasks: list[asyncio.Task] = []
        self.clients: set[web.WebSocketResponse] = set()
        self.positions: dict[str, dict] = {}
        self.feed: deque = deque(maxlen=150)
        self.approvals: dict[str, dict] = {}
        self.agents: dict[str, dict] = {}
        self.lock = asyncio.Lock()
        self.demo = DemoLoop(self)
        self.demo_text = ""
        self.chaos: ChaosMonkey | None = None
        self.chaos_on = C.CHAOS
        self.readers: dict = {}
        self.evals: list | None = None
        self.van_count = C.VAN_COUNT  # the vans slider; takes effect on the next day
        self.registry = None
        self.memory: CustomerMemory | None = None
        self.memory_items: list = []

    # ---- the day
    async def start_day(self, fresh: bool = True) -> None:
        # The fleet for today: this server seeds it, and the simulator and the
        # agents get the same size through DESK_VANS.
        C.VANS = C.fleet(self.van_count)
        self.laser = self.laser or await connect()
        if fresh:
            await reset(self.laser)
        await setup(self.laser)
        self.fleet = Fleet(self.laser, "desk")
        if fresh or not await scan_json(self.fleet.routes):
            await self.fleet.seed()
        self.positions, self.approvals, self.readers, self.evals = {}, {}, {}, None
        self.feed.clear()
        self.agents = {name: {"state": "starting", "pid": None, "restarts": 0} for name in C.PROCESSES}
        self.registry = self.laser.agent_registry()
        self.memory = self.memory or CustomerMemory(self.laser)
        self.sup = Supervisor(C.PROCESSES, env={"DESK_VANS": str(self.van_count)}, on_change=self.on_change)
        await self.sup.start()
        self.chaos = ChaosMonkey(self.sup, on_kill=self.on_chaos_kill, may_kill=self.may_kill)
        if self.chaos_on:
            self.chaos.start()
        self.approver = self.laser.spawn_agent(C.APPROVER, C.INBOX, self.on_interrupt,
                                               respond_on=C.INBOX, poll_interval_ms=20)
        await self.approver.ready()
        self.tasks = [asyncio.create_task(self.tail_positions()),
                      asyncio.create_task(self.tail_events()),
                      asyncio.create_task(self.push_state()),
                      asyncio.create_task(self.refresh_readers())]

    async def new_day(self) -> None:
        async with self.lock:
            await self.stop_day()
            await self.start_day(fresh=True)
        await self.broadcast({"t": "newday"})

    async def day_settled(self) -> bool:
        """Every van back and the invoice dealt with."""
        check = await get_json(self.fleet.day, "invoice_check")
        return check is not None and check.get("state") != "waiting"

    async def stop_day(self) -> None:
        if self.chaos is not None:
            self.chaos.stop()
        for task in self.tasks:
            task.cancel()
        self.tasks = []
        for item in self.approvals.values():
            if not item["future"].done():
                item["future"].set_result("declined")
        if self.approver is not None:
            self.approver.abort()
            self.approver = None
        if self.sup is not None:
            await self.sup.stop()
            self.sup = None

    async def on_change(self, name: str, state: str, pid: int, code=None) -> None:
        agent = self.agents.setdefault(name, {"state": "", "pid": None, "restarts": 0})
        agent.update(state=state, pid=pid if state == "started" else None)
        if state == "exited":
            agent["restarts"] += 1
        await self.broadcast({"t": "agent", "name": name, "state": state, "pid": pid})

    # ---- the log, straight to the browser
    async def tail_positions(self) -> None:
        cursor = self.laser.topic(C.POSITIONS_TOPIC).replay()
        while True:
            messages = await cursor.poll()
            if not messages:
                await asyncio.sleep(0.1)
                continue
            for message in messages:
                record = message.json()
                self.positions[record["van"]] = record
            await self.broadcast({"t": "pos", "vans": list(self.positions.values())})

    async def tail_events(self) -> None:
        cursor = self.laser.topic(C.EVENTS_TOPIC).replay()
        while True:
            messages = await cursor.poll()
            if not messages:
                await asyncio.sleep(0.1)
                continue
            for message in messages:
                record = message.json()
                self.feed.append(record)
                await self.broadcast({"t": "event", "event": record})

    async def push_state(self) -> None:
        while True:
            try:
                await self.broadcast({"t": "state", "state": await self.state()})
            except ls.LaserError as error:
                print(f"state: {error}", flush=True)
            await asyncio.sleep(1)

    async def broadcast(self, message: dict) -> None:
        text = json.dumps(message)
        for ws in list(self.clients):
            try:
                await ws.send_str(text)
            except (ConnectionError, RuntimeError):
                self.clients.discard(ws)

    async def state(self) -> dict:
        f = self.fleet
        return {
            "clock": await f.minutes(),
            "routes": await scan_json(f.routes),
            "stops": await scan_json(f.stops),
            "incidents": await scan_json(f.incidents),
            "exceptions": await scan_json(f.exceptions),
            "invoice": await get_json(f.day, "carrier_invoice"),
            "check": await get_json(f.day, "invoice_check"),
            "overbill": await get_json(f.day, "overbill"),
            "paths": {v: s.get("path", []) for v, s in (await scan_json(f.vans)).items()},
            "agents": self.agents,
            "demo": {"running": self.demo.running, "text": self.demo_text},
            "chaos": self.chaos_on,
            "readers": self.readers,
            "evals": self.evals,
            "approvals": [{"id": k, "prompt": v["prompt"], "cents": v["cents"]}
                          for k, v in self.approvals.items() if not v["future"].done()],
            "registry": await self.read_registry(),
            "dlq": await scan_json(f.dlq),
            "sms_down": await get_json(f.day, "sms_down"),
            "memory": self.memory_items,
            "van_count": self.van_count,
        }

    async def break_sms(self) -> dict:
        """SMS gateway down: the next notify step fails every attempt until redriven."""
        await put_json(self.fleet.day, "sms_down", {"armed": True, "incident": None})
        await self.fleet.event("injected", agent=C.APPROVER,
                               text="the SMS gateway will reject the next notify step until it is fixed")
        return {"ok": True}

    async def redrive(self, key: str) -> dict:
        """Fix the fault, then redrive the parked task: the runtime re-reads the
        original record and republishes it, and the agent handles it again."""
        f = self.fleet
        entry = await get_json(f.dlq, key)
        if not entry or entry.get("state") != "parked":
            return {"ok": False, "error": "nothing parked under that key"}
        await put_json(f.day, "sms_down", {"armed": False, "incident": None})
        capsule = {"source": bytes.fromhex(entry["source"]), "reason": entry["reason"],
                   "attempts": entry["attempts"], "detail": entry.get("detail") or None,
                   "payload": bytes.fromhex(entry["payload"])}
        await self.laser.redrive_dead_letter(capsule)
        entry["state"] = "redriven"
        await put_json(f.dlq, key, entry)
        await f.event("redriven", van=entry.get("van", ""), incident=entry.get("incident", ""), agent=C.APPROVER,
                      text=f"desk fixed the gateway and redrove {entry['agent']}'s parked task")
        return {"ok": True}

    async def quarantine(self, name: str) -> dict:
        """Take an agent out of routing, or put it back. Quarantine is a fact in
        the registry, so dispatch routes around the agent with no change to it.
        The desk refuses to quarantine the last agent serving a skill."""
        if name not in C.SKILLS or self.registry is None:
            return {"ok": False, "error": f"{name} is not in the registry"}
        folded = self.registry.refresh()
        if inspect.isawaitable(folded):
            await folded
        if self.registry.is_quarantined(name):
            await self.laser.unquarantine(C.APPROVER, name)
            await self.fleet.event("unquarantined", agent=C.APPROVER, text=f"desk reinstated {name}; it is back in routing")
            return {"ok": True, "quarantined": False}
        for skill in C.SKILLS[name]:
            others = [c.agent for c in self.registry.resolve(skill) if c.agent != name]
            if not others:
                return {"ok": False, "error": f"{name} is the only agent serving {skill}"}
        await self.laser.quarantine(C.APPROVER, name)
        await self.fleet.event("quarantined", agent=C.APPROVER,
                               text=f"desk quarantined {name}; new steps route around it")
        return {"ok": True, "quarantined": True}

    # ---- the agent registry: who serves which skill, from the agents' cards
    async def read_registry(self) -> dict:
        if self.registry is None:
            return {}
        try:
            folded = self.registry.refresh()
            if inspect.isawaitable(folded):
                await folded
            now = time.time_ns() // 1000
            cards = []
            for card in self.registry.agents():
                body = card.card if isinstance(card.card, dict) else {}
                skills = [c if isinstance(c, str) else (c.get("id") or c.get("skill_id") or c.get("name"))
                          for c in body.get("capabilities") or [] if isinstance(c, (str, dict))]
                cards.append({"agent": card.agent, "skills": [s for s in skills if s],
                              "fresh": card.is_fresh(now), "age_s": round((now - card.observed_at_micros) / 1e6),
                              "quarantined": self.registry.is_quarantined(card.agent)})
            serves = {skill: [c.agent for c in self.registry.resolve(skill, now)]
                      for skills in C.SKILLS.values() for skill in skills}
            return {"cards": sorted(cards, key=lambda c: c["agent"]), "serves": serves}
        except Exception as error:  # noqa: BLE001 - the panel says so instead of breaking the push
            return {"error": f"{type(error).__name__}: {error}"}

    # ---- the person on the desk
    async def on_interrupt(self, ctx, message) -> None:
        """The invoice agent's approval gate: hold until someone clicks."""
        p = message.provenance
        if p.target_agent_id is not None or p.agent != "invoice":
            return
        try:
            body = json.loads(bytes(message.body()))
        except ValueError:
            return
        if not isinstance(body, dict) or body.get("kind") != "invoice":
            return
        key = ls.mint_ulid()
        future = asyncio.get_running_loop().create_future()
        self.approvals[key] = {"prompt": body["prompt"], "cents": body.get("cents", 0), "future": future}
        await self.broadcast({"t": "state", "state": await self.state()})
        delay = C.DEMO_APPROVE_DELAY if self.demo.running else AUTO_DECIDE
        if delay > 0:
            asyncio.get_running_loop().call_later(
                delay, lambda: future.done() or future.set_result("approved"))
        decision = await future
        await ctx.respond_input(C.INBOX, decision.encode())
        await self.fleet.event("decision", agent=C.APPROVER, text=f"desk {decision} the dispute")

    def decide(self, key: str, decision: str) -> bool:
        item = self.approvals.get(key)
        if item is None or item["future"].done():
            return False
        item["future"].set_result(decision)
        return True

    # ---- chaos
    def set_chaos(self, on: bool) -> None:
        self.chaos_on = on
        if self.chaos is not None:
            self.chaos.start() if on else self.chaos.stop()

    async def may_kill(self) -> bool:
        """In the demo loop, only kill while something is in flight, so every kill
        lands on work and shows a recovery."""
        if not self.demo.running:
            return True
        if any(e.get("state") == "open" for e in (await scan_json(self.fleet.exceptions)).values()):
            return True
        return await get_json(self.fleet.day, "invoice_started") is not None and not await self.day_settled()

    async def on_chaos_kill(self, name: str, pid: int) -> None:
        await self.fleet.event("killed", agent=name, by="chaos", text=f"chaos monkey killed {name} pid {pid} (SIGKILL)")

    # ---- readers: managed queries over the views, and the evals
    async def refresh_readers(self) -> None:
        while True:
            try:
                self.readers = await self.read_views()
                if self.evals is None and await self.day_settled():
                    self.evals = await self.run_evals()
            except ls.LaserError as error:
                print(f"readers: {error}", flush=True)
            try:
                if self.memory is not None:
                    self.memory_items = await self.memory.recent()
            except ls.LaserError as error:
                print(f"memory: {error}", flush=True)
            await asyncio.sleep(3)

    async def count_by(self, field: str, **where) -> dict:
        q = self.laser.query(C.EVENTS_INDEX)
        for key, value in where.items():
            q = q.where_eq(key, value)
        rows = await q.count().group_by([field]).fetch()
        return {rows.value_text(r, field): int(rows.value_text(r, "count")) for r in rows.rows}

    async def read_views(self) -> dict:
        rows = await self.laser.query(C.POSITIONS_INDEX).max("metres").group_by(["van"]).fetch()
        km = {rows.value_text(r, "van"): round(float(rows.value_text(r, "max") or 0) / 1000, 1) for r in rows.rows}
        everything = list((await scan_json(self.fleet.exceptions)).values())
        orders = [e for e in everything if e["kind"] == "new_order"]
        exceptions = [e for e in everything if e["kind"] != "new_order"]
        closed = [e["closed"] - e["opened"] for e in exceptions if e.get("closed")]
        return {
            "orders": len(orders),
            "orders_placed": sum(1 for e in orders if e.get("state") == "resolved"),
            "drops": await self.count_by("van", kind="delivered"),
            "failed": sum((await self.count_by("van", kind="failed")).values()),
            "steps": await self.count_by("agent", kind="step", state="done"),
            "kills": await self.count_by("agent", kind="killed"),
            "replays": sum((await self.count_by("agent", kind="replay")).values()),
            "km": km,
            "exceptions": len(exceptions),
            "resolve_minutes": round(sum(closed) / len(closed), 1) if closed else None,
        }

    async def run_evals(self) -> list[dict]:
        """Checks from the log and KV once the day is settled."""
        f, out = self.fleet, []

        def check(name: str, ok: bool, detail: str) -> None:
            out.append({"name": name, "ok": bool(ok), "detail": detail})
        stops = await scan_json(f.stops)
        left = [s for s, v in stops.items() if v.get("status") != "delivered"]
        check("Every parcel delivered", not left, f"{len(stops) - len(left)} of {len(stops)}")
        per_stop = await self.count_by("stop", kind="delivered")
        twice = [s for s, n in per_stop.items() if n > 1]
        check("No parcel delivered twice", not twice, ", ".join(twice) or "each stop once, from the events view")
        everything = list((await scan_json(f.exceptions)).values())
        exceptions = [e for e in everything if e["kind"] != "new_order"]
        open_ = [e for e in exceptions if e.get("state") != "resolved"]
        check("Every exception resolved", not open_, f"{len(exceptions) - len(open_)} of {len(exceptions)}")
        orders = [e for e in everything if e["kind"] == "new_order"]
        book = await get_json(f.day, "orders") or {"issued": []}
        unplaced = [e["stop"] for e in orders if e.get("state") != "resolved"]
        check("Every order placed on a van", not unplaced and len(orders) == len(book["issued"]),
              ", ".join(unplaced) or f"{len(orders)} of {len(book['issued'])} ordered")
        missed = []
        for e in everything:
            replan = (await get_json(f.results, f"{e['id']}:replan")
                      or await get_json(f.results, f"{e['id']}:assign") or {})
            notify = await get_json(f.results, f"{e['id']}:notify") or {}
            told = {m["stop"] for m in notify.get("messages", [])}
            missed += [s for s in replan.get("affected", []) if s not in told]
        check("Every affected customer told", not missed, ", ".join(missed) or "one message per affected drop")
        kills = sum((await self.count_by("agent", kind="killed")).values())
        failed = sum((await self.count_by("agent", kind="workflow_failed")).values())
        check("Every kill recovered", failed == 0, f"{kills} kill(s), {failed} failed workflow(s)")
        result = await get_json(f.day, "invoice_check") or {}
        overbill = await get_json(f.day, "overbill")
        if overbill:
            expected = 2 * C.RATE_DROP + round(6.0 * C.RATE_KM)
            worst = max(result.get("lines", []), key=lambda line: line["diff"], default={})
            ok = abs(result.get("over", 0) - expected) <= C.RATE_KM and worst.get("van") == overbill
            check("Overbilling caught", ok, f"{overbill} overbilled; check found ${result.get('over', 0) / 100:.2f} on {worst.get('van')}")
        else:
            check("Invoice matches the log", result.get("over", 0) < C.DISPUTE_THRESHOLD,
                  f"${result.get('over', 0) / 100:.2f} over")
        return out

    # ---- scenarios
    async def inject(self, scenario: str, van: str | None = None) -> dict:
        f = self.fleet
        if scenario == "overbilled":
            target = van or random.choice(list(C.VANS))
            await put_json(f.day, "overbill", target)
            await f.event("injected", van=target, text=f"carrier will overbill for {target}")
            return {"ok": True, "van": target}
        kind = C.INCIDENT_KINDS[scenario]
        vans = await scan_json(f.vans)
        routes = await scan_json(f.routes)
        stops = await scan_json(f.stops)
        incidents = await scan_json(f.incidents)
        active = [v for v, r in routes.items() if r.get("status") == "active"]

        def fits(v: str) -> bool:
            s = vans.get(v, {})
            busy = incidents.get(v, {}).get("state") in ("pending", "active")
            left = [x for x in routes[v]["stops"] if stops.get(x, {}).get("status") in ("pending", "retry")]
            if busy or s.get("target", "") in ("", "depot") or s.get("status") not in ("moving", "dwelling"):
                return False
            # Just after a drop the van's target is still the stop it finished.
            if kind == "not_home" and s["target"] not in left:
                return False
            return len(left) >= (2 if kind == "unreachable" else 1) and (kind != "unreachable" or len(active) > 1)
        choices = [v for v in ([van] if van else active) if v in routes and fits(v)]
        if not choices:
            return {"ok": False, "error": "no van can take that incident right now"}
        target = random.choice(choices)
        incident = {"id": ls.mint_ulid(), "kind": kind, "state": "pending",
                    "stop": vans[target]["target"] if kind == "not_home" else "",
                    "at": await f.minutes()}
        await put_json(f.incidents, target, incident)
        await f.event("injected", van=target, incident=incident["id"], text=C.SCENARIOS[scenario])
        return {"ok": True, "van": target, "incident": incident["id"]}


# ---------------------------------------------------------------- HTTP

def routes(desk: Desk) -> web.RouteTableDef:
    r = web.RouteTableDef()

    @r.get("/")
    async def index(_):
        return web.FileResponse(STATIC / "index.html")

    @r.get("/api/config")
    async def config(_):
        return web.json_response({"depot": C.DEPOT, "stops": C.STOPS, "vans": C.VANS,
                                  "van_limits": [C.MIN_VANS, C.MAX_VANS],
                                  "scenarios": C.SCENARIOS, "agents": C.PROCESSES,
                                  "step_pause": C.STEP_PAUSE})

    @r.get("/api/roads")
    async def road_legs(_):
        return web.FileResponse(roads.FILE) if roads.FILE.exists() else web.json_response({"legs": {}})

    @r.get("/api/state")
    async def state(_):
        return web.json_response({**await desk.state(), "positions": list(desk.positions.values()),
                                  "feed": list(desk.feed)})

    @r.post("/api/scenario/{name}")
    async def scenario(request):
        name = request.match_info["name"]
        if name not in C.SCENARIOS:
            raise web.HTTPNotFound()
        body = await request.json() if request.can_read_body else {}
        desk.demo.stop()  # a person at the desk takes over from the demo loop
        return web.json_response(await desk.inject(name, body.get("van")))

    @r.post("/api/demo")
    async def demo(_):
        if desk.demo.running:
            desk.demo.stop()
        else:
            desk.demo.start()
        await asyncio.sleep(0)
        return web.json_response({"running": desk.demo.running})

    @r.post("/api/chaos")
    async def chaos(_):
        desk.set_chaos(not desk.chaos_on)
        await desk.broadcast({"t": "chaos", "on": desk.chaos_on})
        return web.json_response({"on": desk.chaos_on})

    @r.post("/api/kill/{name}")
    async def kill(request):
        name = request.match_info["name"]
        if name not in C.AGENTS:
            raise web.HTTPNotFound()
        pid = await desk.sup.kill(name)
        if pid:
            await desk.fleet.event("killed", agent=name, text=f"{name} pid {pid} killed (SIGKILL)")
        return web.json_response({"ok": bool(pid), "pid": pid})

    @r.post("/api/approval/{key}")
    async def approval(request):
        body = await request.json()
        decision = "approved" if body.get("decision") == "approved" else "declined"
        return web.json_response({"ok": desk.decide(request.match_info["key"], decision)})

    @r.get("/api/timeline")
    async def rewind(_):
        """The day so far, replayed from the log for the time travel slider."""
        try:
            return web.json_response(await timeline.rebuild(desk.laser))
        except ls.LaserError as error:
            return web.json_response({"error": str(error)}, status=503)

    @r.post("/api/vans")
    async def vans(request):
        """Change the size of the fleet: a new day starts with that many vans. The
        demo loop, if it is on, starts again from day 1 with them."""
        body = await request.json() if request.can_read_body else {}
        try:
            count = int(body.get("count"))
        except (TypeError, ValueError):
            raise web.HTTPBadRequest(text="count must be a number")
        desk.van_count = max(C.MIN_VANS, min(C.MAX_VANS, count))
        if desk.demo.running:
            await desk.demo.halt()
            desk.demo.start()
        else:
            await desk.new_day()
        return web.json_response({"count": desk.van_count})

    @r.post("/api/sms-down")
    async def sms_down(_):
        desk.demo.stop()  # a person at the desk takes over from the demo loop
        return web.json_response(await desk.break_sms())

    @r.post("/api/dlq/{key}/redrive")
    async def redrive(request):
        try:
            return web.json_response(await desk.redrive(request.match_info["key"]))
        except ls.LaserError as error:
            return web.json_response({"ok": False, "error": str(error)}, status=503)

    @r.post("/api/quarantine/{name}")
    async def quarantine(request):
        try:
            return web.json_response(await desk.quarantine(request.match_info["name"]))
        except ls.LaserError as error:
            return web.json_response({"ok": False, "error": str(error)}, status=503)

    @r.post("/api/memory/forget")
    async def forget(_):
        gone = await desk.memory.forget_all() if desk.memory else 0
        desk.memory_items = []
        await desk.fleet.event("forgotten", agent=C.APPROVER, text=f"desk forgot {gone} customer fact(s)")
        return web.json_response({"forgotten": gone})

    @r.post("/api/new-day")
    async def new_day(_):
        desk.demo.stop()
        await desk.new_day()
        return web.json_response({"ok": True})

    @r.get("/ws")
    async def ws(request):
        sock = web.WebSocketResponse(heartbeat=20)
        await sock.prepare(request)
        desk.clients.add(sock)
        try:
            async for _ in sock:
                pass
        finally:
            desk.clients.discard(sock)
        return sock

    return r


async def make_app() -> web.Application:
    if C.DEMO:
        # Slow the mock LLM to the pace of a real call, so people can follow each
        # step and the chaos monkey has work in flight to kill.
        os.environ.setdefault("DESK_MOCK_DELAY", "0.8")
    desk = Desk()
    await desk.start_day(fresh=os.environ.get("DESK_KEEP", "0") != "1")
    if C.DEMO:
        desk.demo.start()
    app = web.Application()
    app.add_routes(routes(desk))
    app.router.add_static("/static", STATIC)

    async def close_sockets(_):
        for sock in list(desk.clients):
            await sock.close()

    async def cleanup(_):
        desk.demo.stop()
        await desk.stop_day()
        if desk.laser is not None:
            await desk.laser.close()
    app.on_shutdown.append(close_sockets)
    app.on_cleanup.append(cleanup)
    return app


def main() -> None:
    port = int(os.environ.get("DESK_PORT", "8000"))
    print(f"delivery desk on http://localhost:{port}", flush=True)
    web.run_app(make_app(), port=port, print=None, shutdown_timeout=5)


if __name__ == "__main__":
    main()
