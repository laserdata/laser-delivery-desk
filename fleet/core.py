"""Shared plumbing: the connection, topics and views, state in KV, and the
Fleet helpers every process (simulator, agents, server) uses."""

from __future__ import annotations

import asyncio
import json
import os
import time

import laser_sdk as ls

from . import config as C


async def connect() -> ls.Laser:
    return await ls.Laser.connect(C.connection_string(), stream=C.STREAM)


async def reset(laser: ls.Laser) -> None:
    """A new day: drop the stream and everything on it."""
    try:
        await laser.stream(C.STREAM).delete()
    except ls.LaserError:
        pass
    await laser.stream(C.STREAM).ensure()


async def setup(laser: ls.Laser) -> None:
    """Idempotent: the stream, the topics and the two views."""
    await laser.stream(C.STREAM).ensure()
    await laser.sessions().bootstrap(C.PARTITIONS, ls.TopicRetention.expire_after(86_400_000))
    await laser.topic(C.POSITIONS_TOPIC).ensure(partitions=C.PARTITIONS)
    await laser.topic(C.EVENTS_TOPIC).ensure(partitions=C.PARTITIONS)
    await view(laser, C.POSITIONS_TOPIC, C.POSITIONS_INDEX, C.POSITIONS_FIELDS)
    await view(laser, C.EVENTS_TOPIC, C.EVENTS_INDEX, C.EVENTS_FIELDS)


async def view(laser: ls.Laser, topic: str, index: str, fields: list[str]) -> None:
    """Declare a queryable view over a topic, with a change feed, and wait
    until it answers."""
    pid = f"{index}.v1"
    try:
        await laser.projections().register({
            "id": pid, "name": index, "version": 1, "content_type": "json",
            "extraction": {"fields": [{"name": f, "pointer": f"/{f}"} for f in fields],
                           "inline_payload": False},
            "inline_payload_default": False})
        await laser.bindings().apply({
            "source": {"stream": laser.default_stream, "topic": topic},
            "allowed_projections": [pid], "default_projection": pid,
            "notify": True, "index": index})
    except ls.LaserError as error:
        if "exist" not in str(error).lower() and "conflict" not in str(error).lower():
            raise
    deadline = time.monotonic() + 60
    while True:
        try:
            await laser.query(index).limit(1).fetch()
            return
        except ls.LaserError:
            if time.monotonic() > deadline:
                raise
            await asyncio.sleep(0.2)


async def wait_ready(laser: ls.Laser) -> None:
    """Agents start alongside the server: wait for its setup to finish."""
    while True:
        try:
            await laser.query(C.EVENTS_INDEX).limit(1).fetch()
            await laser.query(C.POSITIONS_INDEX).limit(1).fetch()
            return
        except ls.LaserError:
            await asyncio.sleep(0.3)


# ---------------------------------------------------------------- geography

def metres(a: dict, b: dict) -> float:
    """Flat-earth distance, close enough across one city."""
    from math import cos, radians, sqrt
    dy = (a["lat"] - b["lat"]) * 111_320
    dx = (a["lon"] - b["lon"]) * 111_320 * cos(radians((a["lat"] + b["lat"]) / 2))
    return sqrt(dx * dx + dy * dy)


def place(stop: str) -> dict:
    return C.DEPOT if stop == "depot" else C.STOPS[stop]


def clock(minutes: int) -> str:
    return f"{minutes // 60:02d}:{minutes % 60:02d}"


# ---------------------------------------------------------------- state

async def get_json(kv, key: str):
    entry = await kv.get_entry(key)
    return entry.decode_value() if entry is not None else None


async def put_json(kv, key: str, value) -> None:
    await kv.set(key).json(value).send()


async def put_once(kv, key: str, value) -> bool:
    """Write only if nobody has. False means somebody already did."""
    try:
        await kv.set(key).json(value).expect_absent().commit()
        return True
    except ls.KvError as error:
        if getattr(error, "version_conflict", False):
            return False
        raise


async def cas_update(kv, key: str, change):
    """Read a JSON value, change it, and write it back only if nobody else wrote
    in between; if they did, read again and retry. change(value) returns the new
    value, or None to leave it as it is."""
    while True:
        entry = await kv.get_entry(key)
        current = entry.decode_value() if entry is not None else None
        new = change(current)
        if new is None:
            return current
        write = kv.set(key).json(new)
        write = write.expect_version(entry.version) if entry is not None else write.expect_absent()
        try:
            await write.commit()
            return new
        except ls.KvError as error:
            if not getattr(error, "version_conflict", False):
                raise


async def scan_json(kv) -> dict:
    page = await kv.scan().limit(500).fetch()
    return {entry.key_str(): entry.decode_value() for entry in page.entries}


class Fleet:
    """What a process needs: the log and the state."""

    def __init__(self, laser: ls.Laser, name: str) -> None:
        self.laser = laser
        self.name = name
        self.routes = laser.kv(C.NS_ROUTES)
        self.stops = laser.kv(C.NS_STOPS)
        self.vans = laser.kv(C.NS_VANS)
        self.incidents = laser.kv(C.NS_INCIDENTS)
        self.exceptions = laser.kv(C.NS_EXCEPTIONS)
        self.results = laser.kv(C.NS_RESULTS)
        self.day = laser.kv(C.NS_DAY)
        self.dlq = laser.kv(C.NS_DLQ)

    async def event(self, kind: str, *, van: str = "", stop: str = "", incident: str = "",
                    agent: str | None = None, **extra) -> None:
        record = {"kind": kind, "van": van, "stop": stop, "incident": incident,
                  "agent": agent or self.name, "ts": time.time_ns() // 1000,
                  "pid": os.getpid(), **extra}
        await self.laser.topic(C.EVENTS_TOPIC).publish().json(record).send()

    async def minutes(self) -> int:
        """The simulated time of day, in minutes since midnight."""
        value = await get_json(self.day, "clock")
        return int(value) if value is not None else C.DAY_START_MINUTES

    async def seed(self) -> None:
        """The day's plan: every van's round, its stops pending. Stops for orders
        that come in later get their record when the order arrives."""
        for van, spec in C.VANS.items():
            await put_json(self.routes, van, {"stops": list(spec["stops"]), "status": "active"})
            for stop in spec["stops"]:
                await put_json(self.stops, stop, {"status": "pending", "attempts": 0})
        await put_json(self.day, "clock", C.DAY_START_MINUTES)


def body_json(message) -> dict | None:
    try:
        value = json.loads(bytes(message.body()))
    except ValueError:
        return None
    return value if isinstance(value, dict) else None
