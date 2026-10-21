"""Route planning: plain functions, no I/O, so they are easy to test."""

from __future__ import annotations

from .core import metres, place
from .roads import ROAD_FACTOR, road_metres

MINUTES_PER_KM = 3      # inner-city driving, on the simulated clock
MINUTES_PER_DROP = 4


def nearest_order(start: dict, stops: list[str]) -> list[str]:
    """Visit the nearest unvisited stop next. Good enough for five or ten drops."""
    left, order, here = list(stops), [], start
    while left:
        nxt = min(left, key=lambda s: metres(here, place(s)))
        order.append(nxt)
        left.remove(nxt)
        here = place(nxt)
    return order


def etas(now: int, start: dict, stops: list[str]) -> dict[str, int]:
    """Minutes since midnight at which each stop is reached, in order, by road."""
    out, prev, t = {}, None, now
    for stop in stops:
        far = road_metres(prev, stop) if prev else metres(start, place(stop)) * ROAD_FACTOR
        t += round(far / 1000 * MINUTES_PER_KM)
        out[stop] = t
        t += MINUTES_PER_DROP
        prev = stop
    return out


def window(eta: int) -> tuple[int, int]:
    """A one hour window around an ETA, on the half hour."""
    start = (eta // 30) * 30
    return start, start + 60


def detour(position: dict, route: list[str], blocked: str) -> list[str]:
    """The road to `blocked` is closed: do the rest first, come back to it last."""
    rest = [s for s in route if s != blocked]
    return nearest_order(position, rest) + ([blocked] if blocked in route else [])


def retry_last(route: list[str], failed: str) -> list[str]:
    """Nobody home: try again at the end of the round."""
    return [s for s in route if s != failed] + [failed]


def pick_receiver(van: str, routes: dict, positions: dict, open_counts: dict) -> str | None:
    """The van that takes over a parked van's stops: the active van with the
    lowest cost, counting each stop it already has as 2 km of driving plus how
    far away it is."""
    here = positions.get(van)
    candidates = [v for v, r in routes.items() if v != van and r.get("status") == "active"]
    if not candidates:
        return None

    def cost(v: str) -> float:
        away = metres(here, positions[v]) / 1000 if here and v in positions else 0.0
        return open_counts.get(v, 0) * 2 + away
    return min(candidates, key=cost)


def reassign(receiver_position: dict, receiver_route: list[str], moved: list[str]) -> list[str]:
    """Fold the moved stops into the receiving van's round."""
    return nearest_order(receiver_position, receiver_route + [s for s in moved if s not in receiver_route])


LOAD_M = 1500  # each drop a van already has counts as this much extra driving


def insertion(position: dict, route: list[str], stop: str) -> tuple[int, float]:
    """The cheapest place to fit `stop` into a van's open round, and the extra
    metres it costs: drive from where the van is, through the round, back to the
    depot."""
    def leg(a, b) -> float:
        return metres(position, place(b)) * ROAD_FACTOR if a is None else road_metres(a, b)
    points = [None, *route, "depot"]
    best = (0, float("inf"))
    for i in range(len(points) - 1):
        a, b = points[i], points[i + 1]
        extra = leg(a, stop) + road_metres(stop, b) - leg(a, b)
        if extra < best[1]:
            best = (i, extra)
    return best


def pick_van(stop: str, rounds: dict[str, list[str]], positions: dict) -> tuple[str, int, float] | None:
    """The van that can best take a new order: least extra driving, counting the
    drops it already has. Returns (van, position in its round, extra metres)."""
    best = None
    for van, route in rounds.items():
        at, extra = insertion(positions.get(van, place("depot")), route, stop)
        cost = extra + LOAD_M * len(route)
        if best is None or cost < best[0]:
            best = (cost, van, at, extra)
    return None if best is None else best[1:]
