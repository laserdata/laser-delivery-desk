"""Fetch the road route between every pair of places (the depot and every
stop) from the public OSRM demo router, and save them to fleet/data/roads.json.

    python scripts/fetch_roads.py

Run it once after changing the stops in fleet/config.py; it fetches only the
legs the file does not have yet. The demo itself reads the saved file and never
calls a routing service. Each request routes out and back from one place to
BATCH others, so it brings 2 x BATCH legs. The OSRM demo server asks for light
use, so this waits a second between requests.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from fleet import config as C  # noqa: E402

OSRM = "https://router.project-osrm.org/route/v1/driving/{coords}?overview=false&steps=true&geometries=geojson"
BATCH = 12
OUT = ROOT / "fleet" / "data" / "roads.json"
TOLERANCE_M = 4.0


def simplify(points: list[list[float]]) -> list[list[float]]:
    """Douglas-Peucker in metres, so a leg keeps its shape at a fraction of the points."""
    from math import cos, radians
    if len(points) < 3:
        return points
    k = cos(radians(points[0][0])) * 111_320

    def dist(p, a, b):
        ax, ay, bx, by, px, py = a[1] * k, a[0] * 111_320, b[1] * k, b[0] * 111_320, p[1] * k, p[0] * 111_320
        dx, dy = bx - ax, by - ay
        if dx == dy == 0:
            return ((px - ax) ** 2 + (py - ay) ** 2) ** 0.5
        t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
        return ((px - ax - t * dx) ** 2 + (py - ay - t * dy) ** 2) ** 0.5

    keep = [False] * len(points)
    keep[0] = keep[-1] = True
    stack = [(0, len(points) - 1)]
    while stack:
        i, j = stack.pop()
        far, at = 0.0, -1
        for n in range(i + 1, j):
            d = dist(points[n], points[i], points[j])
            if d > far:
                far, at = d, n
        if far > TOLERANCE_M:
            keep[at] = True
            stack += [(i, at), (at, j)]
    return [p for p, kept in zip(points, keep) if kept]


def fetch(places: dict, waypoints: list[str]) -> list[dict]:
    coords = ";".join(f"{places[w]['lon']},{places[w]['lat']}" for w in waypoints)
    req = urllib.request.Request(OSRM.format(coords=coords),
                                 headers={"User-Agent": "laser-delivery-desk/0.1 (demo route cache)"})
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.load(resp)["routes"][0]["legs"]
        except Exception as error:  # noqa: BLE001
            print(f"  {waypoints[0]}: {error}, retrying", flush=True)
            time.sleep(3 * (attempt + 1))
    raise SystemExit(f"could not fetch the legs from {waypoints[0]}")


def leg_points(leg: dict) -> list[list[float]]:
    """A leg's road, joined from its steps' geometries."""
    pts: list[list[float]] = []
    for step in leg["steps"]:
        for lon, lat in step["geometry"]["coordinates"]:
            point = [round(lat, 5), round(lon, 5)]
            if not pts or pts[-1] != point:
                pts.append(point)
    return pts


def save(legs: dict) -> None:
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps({"source": "OpenStreetMap data via the OSRM demo router",
                               "legs": legs}, separators=(",", ":")))


def main() -> None:
    places = {"depot": C.DEPOT, **C.STOPS}
    legs = json.loads(OUT.read_text())["legs"] if OUT.exists() else {}
    names = list(places)
    todo: dict[str, list[str]] = {}
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            if f"{a}>{b}" not in legs or f"{b}>{a}" not in legs:
                todo.setdefault(a, []).append(b)
    batches = [(a, bs[k:k + BATCH]) for a, bs in todo.items() for k in range(0, len(bs), BATCH)]
    print(f"{sum(len(bs) for bs in todo.values())} pairs to fetch in {len(batches)} requests", flush=True)
    for n, (a, bs) in enumerate(batches, 1):
        waypoints = [a]
        for b in bs:
            waypoints += [b, a]
        for k, leg in enumerate(fetch(places, waypoints)):
            legs[f"{waypoints[k]}>{waypoints[k + 1]}"] = {"m": round(leg["distance"]),
                                                         "pts": simplify(leg_points(leg))}
        if n % 10 == 0 or n == len(batches):
            save(legs)
            print(f"  {n}/{len(batches)}", flush=True)
        time.sleep(1)


if __name__ == "__main__":
    main()
