"""Road routes between every pair of places, fetched once by
scripts/fetch_roads.py from OpenStreetMap data and kept in fleet/data/roads.json.
Without that file, legs fall back to straight lines."""

from __future__ import annotations

import json
from pathlib import Path

from .core import metres, place

FILE = Path(__file__).resolve().parent / "data" / "roads.json"
ROAD_FACTOR = 1.35  # roads against a straight line, for the fallback and part legs

_legs: dict = json.loads(FILE.read_text())["legs"] if FILE.exists() else {}


def leg(a: str, b: str) -> list[list[float]]:
    """The points of the road from place a to place b, both ends included."""
    found = _legs.get(f"{a}>{b}")
    if found:
        return found["pts"]
    pa, pb = place(a), place(b)
    return [[pa["lat"], pa["lon"]], [pb["lat"], pb["lon"]]]


def road_metres(a: str, b: str) -> float:
    found = _legs.get(f"{a}>{b}")
    return found["m"] if found else metres(place(a), place(b)) * ROAD_FACTOR
