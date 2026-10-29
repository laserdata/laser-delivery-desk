"""The agents, one per process:

    python -m fleet.agents dispatch|routing|customer|invoice

dispatch watches the views of the log for trouble and runs a workflow for each
exception it finds. routing and customer are the workflow's steps. invoice
checks the carrier's bill against the log at the end of the day and asks a
person before disputing it. They share nothing but the log and KV.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import traceback

import laser_sdk as ls

from . import config as C
from . import plan
from . import telemetry
from .core import (Fleet, body_json, cas_update, clock, connect, get_json, order_session, place, put_json,
                   put_once, scan_json, wait_ready)
from .llm import choose_llm, is_mock
from .memory import CustomerMemory, describe, fact, follow_up

OPEN = ("pending", "retry")


def dollars(cents: int) -> str:
    return f"${cents / 100:,.2f}"


def addressed(message, agent: str) -> dict | None:
    """Every agent reads the one sessions topic; each acts only on what is routed to it."""
    if message.provenance.target_agent_id != agent:
        return None
    return body_json(message)


async def respond(ctx, value: dict) -> None:
    await ctx.respond(json.dumps(value).encode())


async def writes(llm, prompt: str, fallback: str) -> str:
    """Text from the LLM, or the plain fallback when it is the mock or fails."""
    text = await llm.complete(prompt + "\nReply with the text only, no preamble.")
    return fallback if is_mock(text) else " ".join(text.split())


# ---------------------------------------------------------------- routing

class Routing:
    """Assesses an exception and replans the rounds it touches. Two of these run
    (routing and routing-2); either can take any step, because the step's result
    and saved plan live in KV, not in the agent."""

    def __init__(self, fleet: Fleet, llm) -> None:
        self.fleet = fleet
        self.llm = llm

    async def handle(self, ctx, message) -> None:
        task = addressed(message, self.fleet.name)
        if task is None:
            return
        exc, op = task["exception"], task["op"]
        key = f"{exc['id']}:{op}"
        done = await get_json(self.fleet.results, key)
        if done is not None:
            # A redelivery, or a step the workflow re-dispatched after dispatch
            # restarted: the work is already done, so answer with it again.
            await self.fleet.event("replay", van=exc["van"], incident=exc["id"], step=op,
                                   text=f"{op} already done, answering from KV")
            await respond(ctx, done)
            return
        await self.fleet.event("step", van=exc["van"], incident=exc["id"], step=op, state="working")
        with telemetry.span(f"routing.{op}", parent=exc.get("trace"), van=exc["van"], kind=exc["kind"]):
            work = {"assess": self.assess, "replan": self.replan, "assign": self.assign}[op]
            result = await work(exc)
        await put_json(self.fleet.results, key, result)
        await self.fleet.event("step", van=exc["van"], incident=exc["id"], step=op, state="done",
                               text=result["summary"])
        await respond(ctx, result)

    async def where(self) -> dict:
        vans = await scan_json(self.fleet.vans)
        return {v: {"lat": s["lat"], "lon": s["lon"]} for v, s in vans.items()}

    async def open_stops(self, van: str) -> list[str]:
        route = await get_json(self.fleet.routes, van) or {"stops": []}
        stops = await scan_json(self.fleet.stops)
        return [s for s in route["stops"] if stops.get(s, {}).get("status") in OPEN + ("failed",)]

    async def assess(self, exc: dict) -> dict:
        van, kind = exc["van"], exc["kind"]
        here = (await self.where()).get(van, C.DEPOT)
        left = await self.open_stops(van)
        now = await self.fleet.minutes()
        late = plan.etas(now, here, left)
        stop = exc.get("stop")
        where = C.STOPS[stop]["suburb"] if stop in C.STOPS else "its next drop"
        fallback = {
            "road_closure": f"Road to {where} closed. {len(left)} drop(s) left on {van}; "
                            f"last one now due {clock(max(late.values(), default=now))}.",
            "unreachable": f"Driver of {van} not answering. {len(left)} drop(s) need another van.",
            "not_home": f"Nobody home at {where}. One parcel back on {van} for a second attempt.",
        }[kind]
        summary = await writes(self.llm, (
            f"You are the routing agent at a parcel delivery desk. Van {van} has an exception: {kind}"
            f" near {where}. Drops left: {[C.STOPS[s]['suburb'] for s in left]}. "
            "Say in one short sentence what is affected."), fallback)
        return {"summary": summary, "left": left, "etas": late}

    async def replan(self, exc: dict) -> dict:
        """Work out the new rounds, save the plan, then apply it. Killed part way,
        the redelivered step finds the saved plan and applies it again; every
        write is idempotent, so applying twice is the same as once."""
        f, van, kind = self.fleet, exc["van"], exc["kind"]
        plan_key = f"{exc['id']}:plan"
        saved = await get_json(f.results, plan_key)
        if saved is None:
            saved = await self.work_out(exc)
            await put_json(f.results, plan_key, saved)
            await f.event("checkpoint", van=van, incident=exc["id"], step="replan",
                          text="new rounds saved to KV before applying them")
        else:
            await f.event("replay", van=van, incident=exc["id"], step="replan",
                          text="found the saved plan, applying it")
        if C.STEP_PAUSE > 0:
            await f.event("holding", van=van, incident=exc["id"], step="replan",
                          text=f"holding {C.STEP_PAUSE:g}s before committing: kill routing now")
            await asyncio.sleep(C.STEP_PAUSE)
        await self.apply_routes(saved)
        for stop, status in saved.get("stops", {}).items():
            record = await get_json(f.stops, stop) or {"attempts": 0}
            record["status"] = status
            await put_json(f.stops, stop, record)
        incident = await get_json(f.incidents, van)
        if incident and incident["id"] == exc["id"]:
            incident["state"] = "cleared"
            await put_json(f.incidents, van, incident)
        return {k: v for k, v in saved.items() if k != "stops"}

    async def apply_routes(self, saved: dict) -> None:
        """Write the planned rounds. A new order may have gone onto one of these
        vans after the plan was worked out, so keep any open stop the plan does
        not know about: on the same van, or on the receiver if the van is parked."""
        f = self.fleet
        stops = await scan_json(f.stops)
        planned = {s for r in saved["routes"].values() for s in r["stops"]}
        extra: list[str] = []

        def merge(route: dict):
            def change(current):
                newer = [s for s in (current or {}).get("stops", [])
                         if s not in planned and stops.get(s, {}).get("status") in OPEN]
                if route["status"] == "parked":
                    extra.extend(x for x in newer if x not in extra)
                    return dict(route)
                return {**route, "stops": route["stops"] + [x for x in newer if x not in route["stops"]]}
            return change
        order = sorted(saved["routes"], key=lambda v: saved["routes"][v]["status"] != "parked")
        for v in order:
            await cas_update(f.routes, v, merge(saved["routes"][v]))
        receiver = saved.get("receiver")
        if extra and receiver:
            await cas_update(f.routes, receiver, lambda r: {**r, "stops": r["stops"] + [x for x in extra if x not in r["stops"]]})

    async def assign(self, exc: dict) -> dict:
        """A new order: fit the stop into the van that can best take it. The
        choice is saved before it is applied, and applying it is idempotent, so a
        redelivered step finishes the same assignment."""
        f, stop = self.fleet, exc["stop"]
        plan_key = f"{exc['id']}:plan"
        saved = await get_json(f.results, plan_key)
        tried: set[str] = set()
        while True:
            if saved is None:
                saved = await self.choose_van(stop, tried)
                if saved is None:
                    return {"van": "", "affected": [], "etas": {}, "summary": "No van can take the order."}
                await put_json(f.results, plan_key, saved)
            van = saved["van"]

            def insert(route):
                if not route or route.get("status") != "active":
                    return None
                if stop in route["stops"]:
                    return None
                # The round still lists drops already made, so place it by the
                # open drop it goes before, not by an index.
                stops = list(route["stops"])
                before = saved.get("before")
                stops.insert(stops.index(before) if before in stops else len(stops), stop)
                return {**route, "stops": stops}
            route = await cas_update(f.routes, van, insert)
            if route and route.get("status") == "active" and stop in route["stops"]:
                break
            tried.add(van)  # parked since it was chosen: choose again
            saved = None
        record = await get_json(f.stops, stop) or {"attempts": 0}
        if record.get("status") == "new":
            record["status"] = "pending"
            await put_json(f.stops, stop, record)
        here = (await self.where()).get(van, C.DEPOT)
        left = await self.open_stops(van)
        spec = C.STOPS[stop]
        return {"van": van, "affected": [stop], "etas": plan.etas(await f.minutes(), here, left),
                "summary": f"{spec['customer']} in {spec['suburb']} goes on {van}, drop "
                           f"{left.index(stop) + 1 if stop in left else len(left)} of {len(left)}."}

    async def choose_van(self, stop: str, tried: set[str]) -> dict | None:
        f = self.fleet
        routes = await scan_json(f.routes)
        incidents = await scan_json(f.incidents)
        positions = await self.where()
        rounds = {}
        for v, r in routes.items():
            silent = incidents.get(v, {}).get("kind") == "unreachable" and incidents[v].get("state") != "cleared"
            if r.get("status") == "active" and v not in tried and not silent:
                rounds[v] = await self.open_stops(v)
        picked = plan.pick_van(stop, rounds, positions)
        if picked is None:
            return None
        van, at, extra = picked
        before = rounds[van][at] if at < len(rounds[van]) else None
        return {"van": van, "before": before, "extra_m": round(extra)}

    async def work_out(self, exc: dict) -> dict:
        f, van, kind = self.fleet, exc["van"], exc["kind"]
        positions = await self.where()
        here = positions.get(van, C.DEPOT)
        now = await f.minutes()
        left = await self.open_stops(van)
        if kind == "road_closure":
            blocked = exc["stop"]
            order = plan.detour(here, left, blocked)
            where = C.STOPS[blocked]["suburb"]
            return {"routes": {van: {"stops": order, "status": "active"}},
                    "affected": [blocked], "etas": plan.etas(now, here, order),
                    "summary": f"{van} goes round the closure: {where} moved to the end of the round."}
        if kind == "not_home":
            failed = exc["stop"]
            order = plan.retry_last(left, failed)
            return {"routes": {van: {"stops": order, "status": "active"}}, "stops": {failed: "retry"},
                    "affected": [failed], "etas": plan.etas(now, here, order),
                    "summary": f"Second attempt at {C.STOPS[failed]['suburb']} at the end of {van}'s round."}
        routes = await scan_json(f.routes)
        counts = {v: len(await self.open_stops(v)) for v in routes}
        receiver = plan.pick_receiver(van, routes, positions, counts)
        if receiver is None:
            return {"routes": {}, "affected": [], "etas": {},
                    "summary": f"No van free to take {van}'s drops."}
        order = plan.reassign(positions.get(receiver, C.DEPOT), await self.open_stops(receiver), left)
        return {"routes": {van: {"stops": [], "status": "parked"},
                           receiver: {"stops": order, "status": "active"}},
                "affected": left, "receiver": receiver,
                "etas": plan.etas(now, positions.get(receiver, C.DEPOT), order),
                "summary": f"{van} parked. {len(left)} drop(s) moved to {receiver}."}


# ---------------------------------------------------------------- customer

class Customer:
    """Tells each affected customer what is happening and when to expect the
    parcel. It recalls what happened to the customer on earlier days first, and
    remembers today's trouble for next time."""

    def __init__(self, fleet: Fleet, llm) -> None:
        self.fleet = fleet
        self.llm = llm
        self.memory = CustomerMemory(fleet.laser)

    async def handle(self, ctx, message) -> None:
        task = addressed(message, "customer")
        if task is None:
            return
        exc = task["exception"]
        replan = task.get("replan") or task.get("assign") or {}
        key = f"{exc['id']}:notify"
        done = await get_json(self.fleet.results, key)
        if done is not None:
            await self.fleet.event("replay", van=exc["van"], incident=exc["id"], step="notify",
                                   text="messages already sent, answering from KV")
            await respond(ctx, done)
            return
        await self.sms_gateway(exc)
        await self.fleet.event("step", van=exc["van"], incident=exc["id"], step="notify", state="working")
        with telemetry.span("customer.notify", parent=exc.get("trace"), van=exc["van"]):
            messages = await self.write_all(exc, replan)
        result = {"messages": messages, "summary": f"{len(messages)} customer(s) told"}
        await put_json(self.fleet.results, key, result)
        await self.fleet.event("step", van=exc["van"], incident=exc["id"], step="notify", state="done",
                               text=result["summary"])
        await respond(ctx, result)

    async def write_all(self, exc: dict, replan: dict) -> list[dict]:
        messages = []
        for stop in replan.get("affected", []):
            spec = C.STOPS[stop]
            start, end = plan.window(replan["etas"].get(stop, await self.fleet.minutes()))
            if exc["kind"] == "new_order":
                messages.append(await self.confirm(exc, stop, replan.get("van", ""), start, end))
                continue
            reason = {"road_closure": "a road closure on the way to you",
                      "unreachable": "a change of driver",
                      "not_home": "missing you earlier today"}[exc["kind"]]
            history = await self.recall(exc, stop)
            extra = follow_up(exc["kind"], history)
            fallback = (f"Hi {spec['customer'].split()[0]}, your parcel is running late because of {reason}. "
                        f"New delivery window: {clock(start)} to {clock(end)}."
                        + (f" {extra}" if extra else ""))
            before = (f" On earlier days this customer was: {describe(history)}. Acknowledge that briefly."
                      if history else "")
            text = await writes(self.llm, (
                f"Write a two sentence SMS to {spec['customer']} in {spec['suburb']} from a parcel courier. "
                f"Their parcel is late because of {reason}. The new delivery window is {clock(start)} "
                f"to {clock(end)}.{before} Friendly, plain, no apology essay."), fallback)
            messages.append({"stop": stop, "customer": spec["customer"], "text": text,
                             "history": len(history)})
            await self.fleet.event("notice", van=exc["van"], stop=stop, incident=exc["id"], text=text)
            await self.tell_order(stop, text)
            await self.remember(exc, stop)
        return messages

    async def confirm(self, exc: dict, stop: str, van: str, start: int, end: int) -> dict:
        """A new order: tell the customer it is booked and when it is coming. Not
        trouble, so nothing is remembered."""
        spec = C.STOPS[stop]
        fallback = (f"Hi {spec['customer'].split()[0]}, thanks for your order. It's coming today: "
                    f"delivery window {clock(start)} to {clock(end)}.")
        text = await writes(self.llm, (
            f"Write a two sentence SMS to {spec['customer']} in {spec['suburb']} from a parcel courier. "
            f"Their new order is booked for delivery today between {clock(start)} and {clock(end)}. "
            "Friendly and plain."), fallback)
        await self.fleet.event("notice", van=van, stop=stop, incident=exc["id"], text=text)
        await self.tell_order(stop, text)
        return {"stop": stop, "customer": spec["customer"], "text": text, "history": 0}

    async def tell_order(self, stop: str, text: str) -> None:
        """Put the SMS on the order's session, if the stop is an order, so the
        session holds everything its customer was told."""
        order = (await get_json(self.fleet.stops, stop) or {}).get("order")
        if not order:
            return
        session = order_session(self.fleet.laser, order)
        await self.fleet.laser.sessions().open(session).as_agent(self.fleet.name).append(ls.event_envelope(
            ls.new_conversation_id(), session, self.fleet.name, text.encode(), operation="sms"))

    async def sms_gateway(self, exc: dict) -> None:
        """The SMS gateway down button arms a fault: the next notify step claims it and
        fails every attempt (the SMS gateway rejects it) until the desk clears it.
        After RETRY_ATTEMPTS the runtime dead-letters the task."""
        day = self.fleet.day
        flag = await get_json(day, "sms_down")
        if not flag or not flag.get("armed"):
            return
        flag = await cas_update(day, "sms_down", lambda v: {**v, "incident": exc["id"]}
                                if v and v.get("armed") and not v.get("incident") else None)
        if flag and flag.get("armed") and flag.get("incident") == exc["id"]:
            await self.fleet.event("rejected", van=exc["van"], incident=exc["id"], step="notify",
                                   text="SMS gateway rejected the batch")
            raise RuntimeError("SMS gateway rejected the batch")

    async def recall(self, exc: dict, stop: str) -> list[dict]:
        """This customer's earlier trouble, from memory. Today's incident is left
        out, so a redelivered step does not count itself."""
        try:
            history = [h for h in await self.memory.recall(stop) if h.get("incident") != exc["id"]]
        except ls.LaserError as error:
            await self.fleet.event("memory", van=exc["van"], stop=stop, incident=exc["id"],
                                   text=f"memory unavailable: {error}")
            return []
        if history:
            await self.fleet.event("recalled", van=exc["van"], stop=stop, incident=exc["id"],
                                   text=f"{C.STOPS[stop]['customer']} before: {describe(history)}")
        return history

    async def remember(self, exc: dict, stop: str) -> None:
        try:
            # The minute the exception opened, not now: a redelivery must make the same fact.
            clock = exc.get("opened") or await self.fleet.minutes()
            await self.memory.remember(fact(stop, exc["kind"], clock, exc["id"]))
        except ls.LaserError as error:
            await self.fleet.event("memory", van=exc["van"], stop=stop, incident=exc["id"],
                                   text=f"could not remember: {error}")


# ---------------------------------------------------------------- invoice

# Speculative rows in the what-if fork go at offsets far past anything the day
# writes, so they add rows to the view instead of replacing real ones.
FORK_OFFSET = 1_000_000_000


class Invoice:
    """Checks the carrier's invoice against what the log shows: drops and failed
    attempts from the events view, kilometres from the positions view. When the
    bill is over, it stages the carrier's version of the day in a fork of the
    views and runs the same check against it, so the desk sees both side by side."""

    def __init__(self, fleet: Fleet, llm) -> None:
        self.fleet = fleet
        self.llm = llm

    async def handle(self, ctx, message) -> None:
        task = addressed(message, "invoice")
        if task is None:
            return
        f = self.fleet
        done = await get_json(f.day, "invoice_check")
        if done is not None and done.get("state") != "waiting":
            await respond(ctx, done)
            return
        invoice = await get_json(f.day, "carrier_invoice")
        if invoice is None:
            await respond(ctx, {"state": "no_invoice", "summary": "No invoice yet"})
            return
        await f.event("step", step="check", state="working", text="matching the invoice against the log")
        logged = await self.from_log()
        lines, over = [], 0
        for line in invoice["lines"]:
            truth = logged.get(line["van"], {"drops": 0, "failed": 0, "km": 0.0})
            expected = truth["drops"] * C.RATE_DROP + truth["failed"] * C.RATE_FAILED + round(truth["km"] * C.RATE_KM)
            diff = line["cents"] - expected
            lines.append({**line, "logged": truth, "expected": expected, "diff": diff})
            over += max(0, diff)
        check = {"state": "matched", "lines": lines, "billed": invoice["cents"], "over": over}
        if over < C.DISPUTE_THRESHOLD:
            check["summary"] = f"Invoice matches the log ({dollars(invoice['cents'])}). Paid."
        else:
            worst = max(lines, key=lambda line: line["diff"])
            prompt = (f"The carrier billed {dollars(invoice['cents'])}. {worst['van']} is billed for "
                      f"{worst['drops']} drops and {worst['km']} km; the log shows {worst['logged']['drops']} "
                      f"drops and {worst['logged']['km']} km. Hold {dollars(over)} and dispute it?")
            check.update(state="waiting", prompt=prompt)
            check["fork"] = await self.what_if(invoice, logged)
            await put_json(f.day, "invoice_check", check)
            await f.event("approval", step="check", text=prompt, cents=over)
            try:
                answer = bytes(await ctx.approval_gate(C.INBOX, json.dumps(
                    {"kind": "invoice", "prompt": prompt, "cents": over}).encode(),
                    timeout_ms=15 * 60_000)).decode()
            except ls.LaserError as error:
                answer = f"no answer ({type(error).__name__})"
            if answer == "approved":
                check.update(state="disputed", summary=f"Holding {dollars(over)}; dispute sent to the carrier.")
            elif answer == "declined":
                check.update(state="paid", summary=f"Paid in full ({dollars(invoice['cents'])}) on the desk's say-so.")
            else:
                check.update(state="on_hold", summary=f"No decision; invoice left on hold ({answer}).")
            await self.drop_fork(check.get("fork"))
        await put_json(f.day, "invoice_check", check)
        await f.event("step", step="check", state="done", text=check["summary"])
        await respond(ctx, check)

    async def what_if(self, invoice: dict, logged: dict) -> dict | None:
        """The day as the carrier billed it: a copy-on-write fork of the two views
        with the drops and kilometres the log does not show added as speculative
        rows. The same queries run against the fork, so if it reproduces the bill
        the gap is exactly those rows. The real views are never touched."""
        f, laser = self.fleet, self.fleet.laser
        try:
            caps = await laser.capabilities()
            if not getattr(caps, "forks", False):
                await f.event("fork", step="check", text="forks are not served here, skipping the what-if")
                return None
            stale = await get_json(f.day, "invoice_fork")
            if stale:  # a killed check left one open: start that again
                await self.drop_fork({"id": stale}, quiet=True)
            fork_id = f"carrier-claim-{ls.mint_ulid()[-8:].lower()}"
            await put_json(f.day, "invoice_fork", fork_id)
            fork = laser.fork(fork_id)
            await fork.create()
            now, rows = str(time.time_ns() // 1000), 0
            for line in invoice["lines"]:
                van, truth = line["van"], logged.get(line["van"], {"drops": 0, "km": 0.0})
                for _ in range(max(0, line["drops"] - truth["drops"])):
                    await (fork.put_row(C.EVENTS_INDEX, 0, FORK_OFFSET + rows)
                           .projection(f"{C.EVENTS_INDEX}.v1", 1)
                           .field("van", van).field("kind", "delivered").field("stop", "")
                           .field("agent", "carrier").field("ts", now).send())
                    rows += 1
                if line["km"] > truth["km"]:
                    await (fork.put_row(C.POSITIONS_INDEX, 0, FORK_OFFSET + rows)
                           .projection(f"{C.POSITIONS_INDEX}.v1", 1)
                           .field("van", van).field("status", "done").field("ts", now)
                           .field("metres", str(round(line["km"] * 1000))).send())
                    rows += 1
            forked = await self.from_log(fork=fork_id)
            billed = {line["van"]: line for line in invoice["lines"]}
            matches = all(forked.get(v, {}).get("drops", 0) == line["drops"]
                          and abs(forked.get(v, {}).get("km", 0.0) - line["km"]) <= 0.1
                          for v, line in billed.items())
            text = (f"Carrier's version staged in fork {fork_id}: {rows} speculative row(s). "
                    + ("Run against the fork, the check reproduces the bill, so the gap is exactly those rows."
                       if matches else "Run against the fork, the check still does not reproduce the bill."))
            await f.event("fork", step="check", text=text)
            return {"id": fork_id, "rows": rows, "matches": matches, "lines": forked, "text": text}
        except ls.LaserError as error:
            await f.event("fork", step="check", text=f"what-if fork failed: {type(error).__name__}: {error}")
            return None

    async def drop_fork(self, fork: dict | None, quiet: bool = False) -> None:
        """The desk has decided: squash the carrier's version. Promoting it would
        write deliveries the log never saw into the real views."""
        if not fork:
            return
        try:
            await self.fleet.laser.fork(fork["id"]).squash()
            await put_json(self.fleet.day, "invoice_fork", "")
            if not quiet:
                await self.fleet.event("fork", step="check", text=f"fork {fork['id']} squashed; the views are as the log has them")
        except ls.LaserError as error:
            await self.fleet.event("fork", step="check", text=f"could not squash fork {fork['id']}: {error}")

    async def from_log(self, fork: str | None = None) -> dict:
        """Drops, failed drops and km per van from the views, or from a fork of them."""
        laser, out = self.fleet.laser, {}

        def query(index: str):
            q = laser.query(index)
            return q.fork(fork) if fork else q
        for kind, field in (("delivered", "drops"), ("failed", "failed")):
            rows = await query(C.EVENTS_INDEX).where_eq("kind", kind).count().group_by(["van"]).fetch()
            for row in rows.rows:
                van = rows.value_text(row, "van")
                out.setdefault(van, {"drops": 0, "failed": 0, "km": 0.0})[field] = int(rows.value_text(row, "count"))
        rows = await query(C.POSITIONS_INDEX).max("metres").group_by(["van"]).fetch()
        for row in rows.rows:
            van = rows.value_text(row, "van")
            metres = int(float(rows.value_text(row, "max") or 0))
            out.setdefault(van, {"drops": 0, "failed": 0, "km": 0.0})["km"] = round(metres / 1000, 1)
        return out


# ---------------------------------------------------------------- dispatch

class Balance:
    """A route policy for the registry: of the agents serving a skill, pick the
    one this dispatch has sent the fewest steps, so the work spreads across the
    routing agents. The registry's default policy takes the first card every time."""

    def __init__(self) -> None:
        self.sent: dict[str, int] = {}

    def select(self, skill: str, candidates) -> int | None:
        if not candidates:
            return None
        pick = min(range(len(candidates)),
                   key=lambda i: (self.sent.get(candidates[i].agent, 0), candidates[i].agent))
        agent = candidates[pick].agent
        self.sent[agent] = self.sent.get(agent, 0) + 1
        return pick


class Dispatch:
    """Watches the two views for trouble and runs one workflow per exception:
    assess and replan (routing), then notify (customer). A restarted dispatch
    resumes every open workflow from its journal."""

    def __init__(self, fleet: Fleet) -> None:
        self.fleet = fleet
        self.laser = fleet.laser
        self.running: dict[str, asyncio.Task] = {}
        self.roots: dict = {}  # incident -> its open trace
        self.balance = Balance()

    async def run(self) -> None:
        f = self.fleet
        for exc in (await scan_json(f.exceptions)).values():
            if exc.get("state") == "open":
                await f.event("resume", van=exc["van"], incident=exc["id"],
                              text=f"resuming workflow {exc['run'][-6:]} from its journal")
                self.start(exc)
        if await get_json(f.day, "invoice_started") and await get_json(f.day, "invoice_check") is None:
            # Killed after the invoice came in but before the check started: ask again.
            self.ask_invoice()
        positions = self.laser.watch(index=C.POSITIONS_INDEX)
        events = self.laser.watch(index=C.EVENTS_INDEX)
        seen_pos = seen_ev = 0
        while True:
            if await positions.poll():
                rows = await (self.laser.query(C.POSITIONS_INDEX)
                              .filter_in("status", ["stopped", "unreachable"])
                              .filter_gt("ts", seen_pos).order_asc("ts").limit(200).fetch())
                for row in rows.rows:
                    seen_pos = max(seen_pos, rows.value_i64(row, "ts") or 0)
                    await self.open(rows.value_text(row, "incident"), rows.value_text(row, "van"))
            if await events.poll():
                rows = await (self.laser.query(C.EVENTS_INDEX)
                              .filter_in("kind", ["failed", "day_done", "redriven", "order", "delivered"])
                              .filter_gt("ts", seen_ev).order_asc("ts").limit(200).fetch())
                for row in rows.rows:
                    seen_ev = max(seen_ev, rows.value_i64(row, "ts") or 0)
                    if rows.value_text(row, "kind") == "day_done":
                        await self.end_of_day()
                    elif rows.value_text(row, "kind") == "redriven":
                        await self.reopen(rows.value_text(row, "incident"))
                    elif rows.value_text(row, "kind") == "order":
                        await self.open_order(rows.value_text(row, "incident"), rows.value_text(row, "stop"))
                    elif rows.value_text(row, "kind") == "delivered":
                        await self.delivered(rows.value_text(row, "stop"), rows.value_text(row, "van"))
                    else:
                        await self.open(rows.value_text(row, "incident"), rows.value_text(row, "van"))
            await asyncio.sleep(0.25)

    async def open(self, incident_id: str | None, van: str | None) -> None:
        if not incident_id or not van:
            return
        incident = await get_json(self.fleet.incidents, van)
        if not incident or incident["id"] != incident_id:
            return
        if await get_json(self.fleet.exceptions, incident_id) is not None:
            return  # already open: a stopped van reports the same incident every tick
        exc = {"id": incident_id, "van": van, "kind": incident["kind"], "stop": incident.get("stop", ""),
               "run": ls.mint_ulid(), "state": "open", "opened": await self.fleet.minutes()}
        if incident["kind"] in ("road_closure", "unreachable"):
            vans = await scan_json(self.fleet.vans)
            exc["stop"] = vans.get(van, {}).get("target", "")
        root = telemetry.start_root("exception", van=van, kind=exc["kind"], incident=incident_id)
        exc.update(trace=telemetry.carrier(root), trace_id=telemetry.trace_id(root))
        # Only the first sighting opens it, even across restarts.
        if not await put_once(self.fleet.exceptions, incident_id, exc):
            telemetry.end_root(root, duplicate=True)
            return
        self.roots[incident_id] = root
        await self.fleet.event("exception", van=van, stop=exc["stop"], incident=incident_id,
                               text=f"{incident['kind'].replace('_', ' ')} on {van}", exc_kind=exc["kind"])
        self.start(exc)

    async def open_order(self, order_id: str | None, stop: str | None) -> None:
        """A customer ordered: open the order's session, which stays open until
        the parcel is delivered, and run the order workflow (assign, then
        confirm) once, as a child session of it."""
        if not order_id or not stop or await get_json(self.fleet.exceptions, order_id) is not None:
            return
        exc = {"id": order_id, "van": "", "kind": "new_order", "stop": stop, "run": ls.mint_ulid(),
               "session": order_session(self.laser, order_id), "state": "open", "opened": await self.fleet.minutes()}
        root = telemetry.start_root("order", stop=stop, incident=order_id)
        exc.update(trace=telemetry.carrier(root), trace_id=telemetry.trace_id(root))
        if not await put_once(self.fleet.exceptions, order_id, exc):
            telemetry.end_root(root, duplicate=True)
            return
        self.roots[order_id] = root
        spec = C.STOPS[stop]
        order, lease = await (self.laser.sessions().create(f"order/{order_id}")
                              .agent(self.fleet.name).tag(f"stop:{stop}").begin())
        await order.state().set("intent", {"op": "deliver-parcel", "stop": stop, "customer": spec["customer"],
                                           "suburb": spec["suburb"], "ordered": clock(exc["opened"])})
        lease.release()
        _, lease = await (self.laser.sessions().start().with_id(exc["run"])
                          .parent(exc["session"], exc["session"]).agent(self.fleet.name).begin())
        lease.release()
        self.start(exc)

    async def delivered(self, stop: str | None, van: str | None) -> None:
        """An ordered parcel is delivered: its order's session ends, once."""
        record = await get_json(self.fleet.stops, stop) if stop else None
        order = (record or {}).get("order")
        if not order or not await put_once(self.fleet.results, f"{order}:delivered", True):
            return
        session = self.laser.sessions().open(order_session(self.laser, order)).as_agent(self.fleet.name)
        await session.state().set("delivered", {"van": van, "at": clock(record.get("at", await self.fleet.minutes()))})
        await session.end()

    async def reopen(self, incident_id: str | None) -> None:
        """A dead letter was redriven. If its workflow gave up meanwhile, run it
        again under the same run id: the journal skips the steps already done."""
        exc = await get_json(self.fleet.exceptions, incident_id) if incident_id else None
        if not exc or exc.get("state") == "resolved" or exc["id"] in self.running:
            return
        exc.update(state="open", summary="")
        await put_json(self.fleet.exceptions, exc["id"], exc)
        await self.fleet.event("resume", van=exc["van"], incident=exc["id"],
                               text=f"redriven: resuming workflow {exc['run'][-6:]} from its journal")
        self.start(exc)

    def start(self, exc: dict) -> None:
        if exc["id"] not in self.running:
            self.running[exc["id"]] = asyncio.create_task(self.workflow(exc))

    async def workflow(self, exc: dict) -> None:
        f = self.fleet

        def task(op: str, extra=None):
            async def build(outputs):
                await f.event("dispatch_step", van=exc["van"], incident=exc["id"], step=op,
                              text=f"step {op} sent to whoever serves '{op}' in the registry")
                body = {"op": op, "exception": exc}
                if extra:
                    body[extra] = json.loads(bytes(outputs[extra]))
                return json.dumps(body).encode()
            return build

        wf = self.laser.workflow("dispatch", fixed_inbox=C.INBOX)
        wf.run_id(exc["run"])  # the same id resumes the same run from its journal
        wf.budget(ls.WorkflowBudget.unlimited().invocations(6).wall_clock(600_000))
        # Each step names a skill, not an agent: the agent registry resolves the
        # agent whose card advertises it (config.SKILLS).
        if exc["kind"] == "new_order":
            plan_step = "assign"
            wf.step("assign", to_capable="assign", policy=self.balance, build=task("assign"))
        else:
            plan_step = "replan"
            wf.step("assess", to_capable="assess", policy=self.balance, build=task("assess"))
            wf.step("replan", to_capable="replan", policy=self.balance, after=["assess"], build=task("replan"))
        wf.step("notify", to_capable="notify", policy=self.balance, after=[plan_step],
                build=task("notify", plan_step))
        try:
            outcome = await wf.run()
            replan = json.loads(bytes(outcome.outputs[plan_step]))
            if exc["kind"] == "new_order":
                exc["van"] = replan.get("van", "")
                order = self.laser.sessions().open(exc["session"]).as_agent(f.name)
                await order.state().set("placed", {"van": exc["van"], "summary": replan["summary"]})
            exc.update(state="resolved", summary=replan["summary"], closed=await f.minutes())
            await put_json(f.exceptions, exc["id"], exc)
            await f.event("resolved", van=exc["van"], incident=exc["id"], text=replan["summary"])
        except Exception as error:  # noqa: BLE001
            exc.update(state="failed", summary=f"{type(error).__name__}: {error}")
            await put_json(f.exceptions, exc["id"], exc)
            await f.event("workflow_failed", van=exc["van"], incident=exc["id"], text=exc["summary"])
        finally:
            self.running.pop(exc["id"], None)
            telemetry.end_root(self.roots.pop(exc["id"], None), state=exc["state"])

    async def end_of_day(self) -> None:
        """The carrier's invoice is in: ask the invoice agent to check it, once."""
        if not await put_once(self.fleet.day, "invoice_started", True):
            return
        await self.fleet.event("dispatch_step", step="check", text="invoice in, sent to the invoice agent")
        self.ask_invoice()

    def ask_invoice(self) -> None:
        async def ask() -> None:
            await self.laser.contract("invoice-check", json.dumps({"op": "check"}).encode(),
                                      source="dispatch", fixed_inbox=C.INBOX, deadline_ms=20 * 60_000)
        self.running["invoice"] = asyncio.create_task(ask())


# ---------------------------------------------------------------- main

async def main(name: str) -> None:
    laser = await connect()
    await wait_ready(laser)
    fleet = Fleet(laser, name)
    telemetry.init(f"fleet-{name}")
    print(f"{name} pid {os.getpid()} up", flush=True)
    if name == "dispatch":
        await Dispatch(fleet).run()
        return
    llm = choose_llm()
    agent = {"routing": Routing, "routing-2": Routing, "customer": Customer, "invoice": Invoice}[name](fleet, llm)

    async def handler(ctx, message):
        try:
            await agent.handle(ctx, message)
        except Exception:
            traceback.print_exc()
            raise
    async def dead_letter(message, capsule, publish_error=None):
        """The runtime gave up on a task: park it in KV for the desk to redrive."""
        body = body_json(message) or {}
        exc = body.get("exception") or {}
        payload = bytes(capsule.get("payload") or b"")
        source = capsule.get("source") or b""
        key = ls.mint_ulid()
        await put_json(fleet.dlq, key, {
            "agent": name, "incident": exc.get("id", ""), "van": exc.get("van", ""),
            "step": body.get("op") or ("notify" if name == "customer" else ""),
            "reason": capsule.get("reason"), "attempts": capsule.get("attempts"),
            "detail": capsule.get("detail") or "", "source": bytes(source).hex(),
            "payload": payload.hex(), "state": "parked",
            "published": publish_error is None})
        await fleet.event("dead_letter", van=exc.get("van", ""), incident=exc.get("id", ""),
                          text=f"{name} gave up after {capsule.get('attempts')} attempt(s): "
                               f"{capsule.get('detail') or 'handler failed'}. Parked for the desk")

    # The card in the agent registry: the skills this agent serves. A handler
    # that keeps failing is retried, then dead-lettered.
    handle = laser.spawn_agent(name, C.INBOX, handler, respond_on=C.INBOX,
                               capabilities=C.SKILLS[name], poll_interval_ms=20,
                               retry_max_attempts=C.RETRY_ATTEMPTS, retry_base_delay_ms=C.RETRY_DELAY_MS,
                               dead_letter=dead_letter)
    await handle.ready()
    await fleet.event("agent_up", text=f"{name} up (pid {os.getpid()}, LLM {llm.name})")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
