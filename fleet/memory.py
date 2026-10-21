"""Customer memory: what happened to each customer on earlier days.

The customer agent remembers every delay and every missed drop, scoped to the
customer, on a memory topic in its own stream (config.MEMORY_STREAM). A new
day drops the day's stream, not this one, so the next time the same customer
is affected the agent recalls their history and words the message for it.
Laser Stack folds the memory topic into a read view; recall reads that view.
"""

from __future__ import annotations

import json
import time

from . import config as C

# What each incident means for the customer, in the words the history uses.
WHAT = {"road_closure": "late (road closure)", "unreachable": "late (change of driver)",
        "not_home": "not home"}


ULID_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def incident_time(incident: str) -> int | None:
    """When the incident was raised, in microseconds, from its ULID's time part."""
    if len(incident) != 26 or any(c not in ULID_ALPHABET for c in incident.upper()):
        return None
    ms = 0
    for c in incident[:10].upper():
        ms = ms * 32 + ULID_ALPHABET.index(c)
    return ms * 1000


def fact(stop: str, kind: str, clock: int, incident: str) -> dict:
    """The same incident always makes the same fact, so remembering it again
    after a redelivery is deduplicated."""
    at = incident_time(incident)
    return {"stop": stop, "customer": C.STOPS[stop]["customer"], "kind": kind,
            "clock": clock, "incident": incident, "at": at if at is not None else time.time_ns() // 1000}


def describe(history: list[dict]) -> str:
    """One line for the log: 'not home at 10:42; late (road closure) at 11:05'."""
    return "; ".join(f"{WHAT.get(h.get('kind'), h.get('kind'))} at {h['clock'] // 60:02d}:{h['clock'] % 60:02d}"
                     for h in history if isinstance(h.get("clock"), int))


def follow_up(kind: str, history: list[dict]) -> str | None:
    """The extra sentence for a customer this has happened to before, or None."""
    if not history:
        return None
    if kind == "not_home" and any(h.get("kind") == "not_home" for h in history):
        return "We missed you last time too, so if you're out the driver will leave it somewhere safe."
    if len(history) == 1:
        return "Sorry, this is the second time we've been late to you, and we've flagged it."
    return f"Sorry, this has happened {len(history)} times before. We've flagged your address."


class CustomerMemory:
    """Remember and recall per customer. A customer is scoped by their stop id."""

    def __init__(self, laser) -> None:
        self.laser = laser
        self.handle = None

    async def open(self):
        if self.handle is None:
            await self.laser.stream(C.MEMORY_STREAM).ensure()
            self.handle = await self.laser.memory_topic(C.MEMORY_TOPIC, stream=C.MEMORY_STREAM)
        return self.handle

    async def recall(self, stop: str, limit: int = 10) -> list[dict]:
        memory = await self.open()
        items = await memory.recall(user=stop, limit=limit, stream=C.MEMORY_STREAM)
        facts = [d for d in (decode(i) for i in items) if d]
        return sorted(facts, key=lambda h: h.get("at", 0))

    async def remember(self, value: dict) -> None:
        memory = await self.open()
        # dedup: a redelivered notify step remembers the same incident once.
        await memory.remember(json.dumps(value, sort_keys=True), user=value["stop"],
                              stream=C.MEMORY_STREAM, dedup=True)

    async def recent(self, limit: int = 12) -> list[dict]:
        """The newest facts across every customer, for the desk."""
        memory = await self.open()
        items = await memory.recall(limit=200, stream=C.MEMORY_STREAM)
        facts = [d for d in (decode(i) for i in items) if d]
        return sorted(facts, key=lambda h: h.get("at", 0), reverse=True)[:limit]

    async def forget_all(self) -> int:
        """Forget every fact, one tombstone each, the way an erasure request would."""
        memory = await self.open()
        gone = 0
        for item in await memory.recall(limit=1000, stream=C.MEMORY_STREAM):
            value = decode(item)
            await memory.forget(item.id, user=(value or {}).get("stop"), stream=C.MEMORY_STREAM)
            gone += 1
        return gone


def decode(item) -> dict | None:
    try:
        value = item.json()
    except Exception:  # noqa: BLE001 - anything that is not one of ours
        return None
    return value if isinstance(value, dict) and "stop" in value else None
