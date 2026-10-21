"""Time travel: the day so far, rebuilt from the log alone.

Replay reads the positions and events topics from offset 0 on a fresh cursor,
with nothing from KV or the server's memory, and folds them into one frame per
minute of the day's clock. The browser scrubs through the frames: where each
van was, which drops were done, what the agents were doing at that minute.
"""

from __future__ import annotations

from bisect import bisect_right

from . import config as C

POSITION_FIELDS = ("van", "lat", "lon", "status", "target", "metres", "incident")


async def read(laser, topic: str) -> tuple[list[dict], list[int]]:
    """Every record on `topic`, oldest first, and the offsets the cursor reached."""
    cursor = laser.topic(topic).replay()
    records = []
    while True:
        messages = await cursor.poll()
        if not messages:
            return records, list(cursor.offsets)
        for message in messages:
            try:
                value = message.json()
            except Exception:  # noqa: BLE001 - not one of ours
                continue
            if isinstance(value, dict):
                records.append(value)


def build(positions: list[dict], events: list[dict]) -> dict:
    """Fold the two topics into frames. A frame is the last position of each van
    reported in that minute; a van missing from a frame has not moved since the
    frame before. Each event gets the clock minute it happened in."""
    positions = sorted((p for p in positions if isinstance(p.get("clock"), int)), key=lambda p: p.get("ts", 0))
    frames: dict[int, dict] = {}
    for p in positions:
        frames.setdefault(p["clock"], {})[p["van"]] = {k: p.get(k) for k in POSITION_FIELDS}
    stamps = [p.get("ts", 0) for p in positions]
    first = positions[0]["clock"] if positions else C.DAY_START_MINUTES

    def clock_at(ts: int) -> int:
        i = bisect_right(stamps, ts)
        return positions[i - 1]["clock"] if i else first

    timed = [{**e, "clock": clock_at(e.get("ts", 0))} for e in sorted(events, key=lambda e: e.get("ts", 0))]
    clocks = sorted(frames)
    return {
        "start": clocks[0] if clocks else first,
        "end": clocks[-1] if clocks else first,
        "frames": [[c, frames[c]] for c in clocks],
        "events": timed,
    }


async def rebuild(laser) -> dict:
    positions, pos_offsets = await read(laser, C.POSITIONS_TOPIC)
    events, ev_offsets = await read(laser, C.EVENTS_TOPIC)
    out = build(positions, events)
    out["read"] = {"positions": len(positions), "events": len(events),
                   "offsets": {C.POSITIONS_TOPIC: pos_offsets, C.EVENTS_TOPIC: ev_offsets}}
    return out
