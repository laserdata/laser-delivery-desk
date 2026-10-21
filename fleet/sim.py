"""The world: the vans driving their rounds, reporting where they are.

    python -m fleet.sim

Each tick every van moves towards its next drop and its position goes on the
log. The simulator reads each van's round from KV every tick, so a new route
from the routing agent takes effect on the next tick. It also plays the
incidents the server injects: a closed road, a driver who stops answering,
nobody home. It never fixes them; that is the agents' job. And it plays the
customers who order during the day: each new order goes on the log for
dispatch to give to a van.
"""

from __future__ import annotations

import asyncio
import random
import time

import laser_sdk as ls

from . import config as C
from . import roads
from .core import Fleet, connect, get_json, metres, place, put_json, put_once, scan_json, wait_ready

OPEN = ("pending", "retry")


def start_state() -> dict:
    # "from" is the last place the van was at, "path" the road still ahead of it
    # on this leg and "walked" the road behind it, for turning back.
    return {"lat": C.DEPOT["lat"], "lon": C.DEPOT["lon"], "status": "moving", "target": "",
            "from": "depot", "path": [], "walked": [],
            "dwell": 0, "metres": 0.0, "incident": "", "drops": 0, "failed": 0}


class Sim:
    def __init__(self, fleet: Fleet) -> None:
        self.fleet = fleet
        self.state: dict[str, dict] = {}
        self.minutes = C.DAY_START_MINUTES

    async def load(self) -> None:
        """Pick up where a previous simulator left off, or start at the depot."""
        for van in C.VANS:
            self.state[van] = await get_json(self.fleet.vans, van) or start_state()
        self.minutes = await self.fleet.minutes()

    async def tick(self) -> None:
        f = self.fleet
        self.minutes += C.SIM_MINUTES_PER_TICK
        await put_json(f.day, "clock", self.minutes)
        routes = await scan_json(f.routes)
        stops = await scan_json(f.stops)
        incidents = await scan_json(f.incidents)
        batch = f.laser.topic(C.POSITIONS_TOPIC).publish_batch()
        for van in C.VANS:
            van_state = self.state[van]
            await self.step(van, van_state, routes.get(van) or {}, stops, incidents.get(van))
            batch = batch.add_json({
                "van": van, "lat": round(van_state["lat"], 6), "lon": round(van_state["lon"], 6),
                "status": van_state["status"], "incident": van_state["incident"],
                "target": van_state["target"], "metres": int(van_state["metres"]),
                "clock": self.minutes, "ts": time.time_ns() // 1000})
            await put_json(f.vans, van, van_state)
        await batch.send()
        await self.take_orders(stops)
        await self.maybe_end_day(routes, stops)

    async def step(self, van: str, s: dict, route: dict, stops: dict, incident: dict | None) -> None:
        f = self.fleet
        if route.get("status") == "parked":
            s.update(status="parked", target="", incident="")
            return
        # A closed road or a silent driver halts the van until the routing agent
        # clears the incident.
        if incident and incident["kind"] in ("road_closure", "unreachable"):
            if incident["state"] == "pending" and s["status"] == "moving" and s["target"] not in ("", "depot"):
                incident["state"] = "active"
                await put_json(f.incidents, van, incident)
                s.update(status="stopped" if incident["kind"] == "road_closure" else "unreachable",
                         incident=incident["id"])
                await f.event(incident["kind"], van=van, stop=s["target"], incident=incident["id"])
                return
            if incident["state"] == "active":
                return
        if s["status"] in ("stopped", "unreachable"):
            s.update(status="moving", incident="")
        target = next((x for x in route.get("stops", []) if stops.get(x, {}).get("status") in OPEN), None)
        if s["status"] == "dwelling":
            s["dwell"] -= 1
            if s["dwell"] <= 0:
                await self.drop(van, s, stops, incident)
            return
        if target is None:
            target = "depot"
        if s["status"] == "done" and target == "depot":
            return
        if target != s["target"]:
            # A new leg. Changed mid-leg (a reroute), the van turns back the way
            # it came to the last place it was at, then takes the road from there.
            here = place(s["from"])
            s["path"] = list(reversed(s["walked"])) + [[here["lat"], here["lon"]]] + roads.leg(s["from"], target)[1:]
            s["walked"] = []
        s.update(status="moving", target=target)
        budget = C.SPEED_M
        while budget > 0 and s["path"]:
            lat, lon = s["path"][0]
            gap = metres(s, {"lat": lat, "lon": lon})
            if gap <= budget:
                s.update(lat=lat, lon=lon)
                s["walked"].append(s["path"].pop(0))
                s["metres"] += gap
                budget -= gap
            else:
                frac = budget / gap
                s["lat"] += (lat - s["lat"]) * frac
                s["lon"] += (lon - s["lon"]) * frac
                s["metres"] += budget
                budget = 0
        if not s["path"]:
            s.update({"from": target, "walked": []})
            if target == "depot":
                s["status"] = "done"
                await f.event("back_at_depot", van=van)
            else:
                s.update(status="dwelling", dwell=C.DWELL_TICKS)

    async def drop(self, van: str, s: dict, stops: dict, incident: dict | None) -> None:
        f, stop = self.fleet, s["target"]
        record = stops.get(stop, {"status": "pending", "attempts": 0})
        record["attempts"] = record.get("attempts", 0) + 1
        s["status"] = "moving"
        if incident and incident["kind"] == "not_home" and incident["state"] == "pending" \
                and incident.get("stop") in (stop, ""):
            incident.update(state="active", stop=stop)
            await put_json(f.incidents, van, incident)
            record["status"] = "failed"
            s["failed"] += 1
            await put_json(f.stops, stop, record)
            await f.event("failed", van=van, stop=stop, incident=incident["id"],
                          text=f"Nobody home at {C.STOPS[stop]['suburb']}")
            return
        record.update(status="delivered", by=van, at=self.minutes)
        s["drops"] += 1
        await put_json(f.stops, stop, record)
        await f.event("delivered", van=van, stop=stop)

    def orders_due(self) -> int:
        """How many orders the day takes in: ORDERS, or fewer if the stops run out."""
        return min(C.ORDERS, len(C.order_pool(C.VANS)))

    async def take_orders(self, stops: dict) -> None:
        """A customer orders: the stop gets a record ("new") and the order goes on
        the log. What has been ordered so far is kept in KV, so a restarted
        simulator carries on from there."""
        f = self.fleet
        book = await get_json(f.day, "orders") or {
            "issued": [], "next": C.DAY_START_MINUTES + round(C.ORDER_FIRST / C.TICK) * C.SIM_MINUTES_PER_TICK}
        if len(book["issued"]) >= self.orders_due() or self.minutes < book["next"]:
            return
        left = [x for x in C.order_pool(C.VANS) if x not in book["issued"] and x not in stops]
        if not left:
            return
        stop, order = random.choice(left), ls.mint_ulid()
        await put_json(f.stops, stop, {"status": "new", "attempts": 0, "ordered": self.minutes})
        spec = C.STOPS[stop]
        await f.event("order", stop=stop, incident=order, text=f"{spec['customer']} in {spec['suburb']} ordered a parcel")
        book["issued"].append(stop)
        book["next"] = self.minutes + max(1, round(C.ORDER_EVERY / C.TICK)) * C.SIM_MINUTES_PER_TICK
        await put_json(f.day, "orders", book)

    async def maybe_end_day(self, routes: dict, stops: dict) -> None:
        """Every order in, every van home or parked, nothing left to deliver: the
        carrier sends its invoice."""
        book = await get_json(self.fleet.day, "orders") or {"issued": []}
        if len(book["issued"]) < self.orders_due() or any(v.get("status") == "new" for v in stops.values()):
            return
        if any(s["status"] not in ("done", "parked") for s in self.state.values()):
            return
        if any(r.get("status") == "active" and any(stops.get(x, {}).get("status") in OPEN + ("failed",)
                                                      for x in r.get("stops", []))
               for r in routes.values()):
            return
        overbill = await get_json(self.fleet.day, "overbill")
        lines = []
        for van, s in self.state.items():
            drops, failed, km = s["drops"], s["failed"], round(s["metres"] / 1000, 1)
            if van == overbill:
                drops, km = drops + 2, round(km + 6.0, 1)
            lines.append({"van": van, "drops": drops, "failed": failed, "km": km,
                          "cents": drops * C.RATE_DROP + failed * C.RATE_FAILED + round(km * C.RATE_KM)})
        invoice = {"carrier": "Southern Cross Couriers", "lines": lines,
                   "cents": sum(line["cents"] for line in lines)}
        if await put_once(self.fleet.day, "carrier_invoice", invoice):
            await self.fleet.event("day_done", agent="sim", cents=invoice["cents"])


async def main() -> None:
    laser = await connect()
    await wait_ready(laser)
    sim = Sim(Fleet(laser, "sim"))
    await sim.load()
    next_tick = time.monotonic()
    while True:
        await sim.tick()
        next_tick += C.TICK
        await asyncio.sleep(max(0.0, next_tick - time.monotonic()))


if __name__ == "__main__":
    asyncio.run(main())
