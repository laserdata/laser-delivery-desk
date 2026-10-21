"""The city, the vans, the stops, the scenarios and the settings.

Everything here is made up for the demo: the customers, the carrier and the
rates. The coordinates are suburb centres in inner Melbourne.
"""

from __future__ import annotations

import os

import laser_sdk as ls


def env(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def connection_string() -> str:
    return env("LASER_CONNECTION_STRING", "iggy:laser@127.0.0.1:8090")


STREAM = env("DESK_STREAM", "delivery-desk")
PARTITIONS = 1

# The log
POSITIONS_TOPIC = "fleet.positions"  # every van's position, each tick
EVENTS_TOPIC = "fleet.events"        # drops, failed drops, agent steps, outcomes
INBOX = ls.AgentTopic.Sessions       # workflow steps, replies and approvals

# Views over the log (managed by Laser Stack)
POSITIONS_INDEX = "fleet_positions"
POSITIONS_FIELDS = ["van", "status", "incident", "ts", "metres"]
EVENTS_INDEX = "fleet_events"
EVENTS_FIELDS = ["van", "stop", "kind", "incident", "agent", "step", "state", "ts"]

# Agent skills: what each agent advertises on its card in the agent registry.
# Dispatch asks for a skill, never an agent by name, and the registry resolves
# who serves it.
# Two routing agents serve the same skills, so the registry has a real choice.
SKILLS = {"routing": ["assess", "replan", "assign"], "routing-2": ["assess", "replan", "assign"],
          "customer": ["notify"], "invoice": ["invoice-check"]}

# Customer memory lives on its own stream, so it outlives the day: a new day
# drops DESK_STREAM, not this one.
MEMORY_STREAM = env("DESK_MEMORY_STREAM", f"{STREAM}-memory")
MEMORY_TOPIC = "customer.memory"

# State
NS_ROUTES = "fleet.routes"          # van -> remaining stops, in order
NS_STOPS = "fleet.stops"            # stop -> pending, delivered, failed, retry
NS_VANS = "fleet.vans"              # van -> the simulator's own state
NS_INCIDENTS = "fleet.incidents"    # van -> the incident injected on it
NS_EXCEPTIONS = "fleet.exceptions"  # incident -> the exception and its workflow run
NS_RESULTS = "fleet.results"        # one result per step per exception, so a redelivery is a no-op
NS_DAY = "fleet.day"                # clock, carrier invoice, invoice check, SMS gateway fault
NS_DLQ = "fleet.dlq"                # dead letters the agents gave up on, until redriven

# A handler that keeps failing is retried this many times, then dead-lettered.
RETRY_ATTEMPTS = 3
RETRY_DELAY_MS = 300

AGENTS = ["dispatch", "routing", "routing-2", "customer", "invoice"]
PROCESSES = ["sim", *AGENTS]
APPROVER = "ops"  # the person on the desk; lives in the web server

# The simulator
TICK = float(env("DESK_TICK", "0.5"))            # seconds between position reports
SPEED_M = float(env("DESK_SPEED", "120"))        # metres a van moves per tick
DWELL_TICKS = int(env("DESK_DWELL", "4"))        # ticks spent at each drop
SIM_MINUTES_PER_TICK = 1                         # the clock on screen
DAY_START_MINUTES = 9 * 60

# Seconds the routing agent holds before it commits a new route, so there is
# time to kill it mid-step (0 = off).
STEP_PAUSE = float(env("DESK_STEP_PAUSE", "0"))

# The carrier's rates, in cents
RATE_DROP = 1200
RATE_FAILED = 600
RATE_KM = 90
DISPUTE_THRESHOLD = 1000  # differences under $10 are paid without asking

DEPOT = {"name": "Depot", "suburb": "West Melbourne", "lat": -37.8075, "lon": 144.9420}

STOPS = {
    "s01": {"customer": "Mia Chen", "suburb": "Carlton", "lat": -37.8001, "lon": 144.9671},
    "s02": {"customer": "Liam O'Brien", "suburb": "Parkville", "lat": -37.7870, "lon": 144.9510},
    "s03": {"customer": "Ava Patel", "suburb": "Brunswick", "lat": -37.7670, "lon": 144.9620},
    "s04": {"customer": "Noah Williams", "suburb": "Northcote", "lat": -37.7700, "lon": 145.0000},
    "s05": {"customer": "Isla Rossi", "suburb": "Fitzroy North", "lat": -37.7836, "lon": 144.9800},
    "s06": {"customer": "Jack Nguyen", "suburb": "Fitzroy", "lat": -37.7990, "lon": 144.9780},
    "s07": {"customer": "Grace Kelly", "suburb": "Collingwood", "lat": -37.8020, "lon": 144.9880},
    "s08": {"customer": "Leo Murphy", "suburb": "Abbotsford", "lat": -37.8040, "lon": 145.0010},
    "s09": {"customer": "Zoe Martin", "suburb": "Hawthorn", "lat": -37.8220, "lon": 145.0300},
    "s10": {"customer": "Ethan Brown", "suburb": "Richmond", "lat": -37.8230, "lon": 144.9980},
    "s11": {"customer": "Chloe Singh", "suburb": "Southbank", "lat": -37.8250, "lon": 144.9640},
    "s12": {"customer": "Lucas Taylor", "suburb": "South Yarra", "lat": -37.8380, "lon": 144.9920},
    "s13": {"customer": "Ruby Lee", "suburb": "St Kilda", "lat": -37.8670, "lon": 144.9800},
    "s14": {"customer": "Henry Walker", "suburb": "Albert Park", "lat": -37.8410, "lon": 144.9550},
    "s15": {"customer": "Ella Wilson", "suburb": "South Melbourne", "lat": -37.8340, "lon": 144.9580},
    "s16": {"customer": "Oscar Jones", "suburb": "Docklands", "lat": -37.8150, "lon": 144.9390},
    "s17": {"customer": "Sophie Clarke", "suburb": "Yarraville", "lat": -37.8160, "lon": 144.8890},
    "s18": {"customer": "Max Fraser", "suburb": "Footscray", "lat": -37.8000, "lon": 144.9000},
    "s19": {"customer": "Lily Anderson", "suburb": "Flemington", "lat": -37.7870, "lon": 144.9190},
    "s20": {"customer": "Archie Scott", "suburb": "Kensington", "lat": -37.7930, "lon": 144.9300},
    "s21": {"customer": "Harper Evans", "suburb": "Coburg", "lat": -37.7440, "lon": 144.9660},
    "s22": {"customer": "Mason Hughes", "suburb": "Thornbury", "lat": -37.7550, "lon": 145.0010},
    "s23": {"customer": "Evie Morris", "suburb": "Preston", "lat": -37.7430, "lon": 145.0070},
    "s24": {"customer": "Hugo Bennett", "suburb": "Fairfield", "lat": -37.7790, "lon": 145.0170},
    "s25": {"customer": "Willow Ward", "suburb": "Clifton Hill", "lat": -37.7880, "lon": 144.9950},
    "s26": {"customer": "Theo Cooper", "suburb": "Carlton North", "lat": -37.7840, "lon": 144.9720},
    "s27": {"customer": "Matilda Reid", "suburb": "Brunswick East", "lat": -37.7720, "lon": 144.9790},
    "s28": {"customer": "Felix Grant", "suburb": "Brunswick West", "lat": -37.7650, "lon": 144.9440},
    "s29": {"customer": "Amelia Price", "suburb": "Essendon", "lat": -37.7550, "lon": 144.9190},
    "s30": {"customer": "Leo Russo", "suburb": "Moonee Ponds", "lat": -37.7660, "lon": 144.9220},
    "s31": {"customer": "Sienna Ali", "suburb": "Ascot Vale", "lat": -37.7780, "lon": 144.9220},
    "s32": {"customer": "Hamish Doyle", "suburb": "Travancore", "lat": -37.7810, "lon": 144.9360},
    "s33": {"customer": "Ivy Chan", "suburb": "North Melbourne", "lat": -37.7990, "lon": 144.9440},
    "s34": {"customer": "Arlo Kaur", "suburb": "Kew", "lat": -37.8060, "lon": 145.0300},
    "s35": {"customer": "Olivia Burke", "suburb": "Hawthorn East", "lat": -37.8250, "lon": 145.0480},
    "s36": {"customer": "Jasper Lim", "suburb": "Burnley", "lat": -37.8280, "lon": 145.0070},
    "s37": {"customer": "Charlotte Day", "suburb": "Cremorne", "lat": -37.8300, "lon": 144.9930},
    "s38": {"customer": "Elijah Moore", "suburb": "Toorak", "lat": -37.8410, "lon": 145.0140},
    "s39": {"customer": "Frankie Shaw", "suburb": "Armadale", "lat": -37.8560, "lon": 145.0190},
    "s40": {"customer": "Riley Ng", "suburb": "Prahran", "lat": -37.8510, "lon": 144.9930},
    "s41": {"customer": "Poppy Nolan", "suburb": "Windsor", "lat": -37.8560, "lon": 144.9920},
    "s42": {"customer": "Kai Romano", "suburb": "Malvern", "lat": -37.8580, "lon": 145.0310},
    "s43": {"customer": "Alice Fox", "suburb": "Alphington", "lat": -37.7790, "lon": 145.0300},
    "s44": {"customer": "Harrison Bell", "suburb": "Ivanhoe", "lat": -37.7690, "lon": 145.0450},
    "s45": {"customer": "Layla Hart", "suburb": "Elwood", "lat": -37.8820, "lon": 144.9850},
    "s46": {"customer": "Ned Sullivan", "suburb": "St Kilda East", "lat": -37.8670, "lon": 145.0000},
    "s47": {"customer": "Rose Tran", "suburb": "Balaclava", "lat": -37.8690, "lon": 144.9930},
    "s48": {"customer": "Billy Carr", "suburb": "Port Melbourne", "lat": -37.8390, "lon": 144.9420},
    "s49": {"customer": "Nina Park", "suburb": "Middle Park", "lat": -37.8490, "lon": 144.9620},
    "s50": {"customer": "Toby Quinn", "suburb": "Ripponlea", "lat": -37.8760, "lon": 144.9960},
    "s51": {"customer": "Daisy Wong", "suburb": "Caulfield North", "lat": -37.8740, "lon": 145.0240},
    "s52": {"customer": "Sebastian Ito", "suburb": "Melbourne", "lat": -37.8136, "lon": 144.9631},
    "s53": {"customer": "Georgia Lane", "suburb": "East Melbourne", "lat": -37.8130, "lon": 144.9850},
    "s54": {"customer": "Patrick Ruiz", "suburb": "Seddon", "lat": -37.8060, "lon": 144.8910},
    "s55": {"customer": "Hazel Ford", "suburb": "West Footscray", "lat": -37.8000, "lon": 144.8780},
    "s56": {"customer": "Angus Reyes", "suburb": "Maribyrnong", "lat": -37.7740, "lon": 144.8890},
    "s57": {"customer": "Lucy Barton", "suburb": "Spotswood", "lat": -37.8300, "lon": 144.8850},
    "s58": {"customer": "Zara Mills", "suburb": "Williamstown", "lat": -37.8570, "lon": 144.8970},
    "s59": {"customer": "Oliver Kim", "suburb": "Newport", "lat": -37.8440, "lon": 144.8830},
    "s60": {"customer": "Eva Santos", "suburb": "Kingsville", "lat": -37.8090, "lon": 144.8770},
}

# The drivers on the books. A day runs VAN_COUNT of them. Each van starts the
# day with JOBS_PER_VAN planned drops (see fleet()); the stops left over are the
# customers who order during the day.
VAN_POOL = [
    ("Sam", "#b8ff52"), ("Priya", "#5aa9ff"), ("Tom", "#c792ff"), ("Aisha", "#5fd7e8"),
    ("Ben", "#ffd75a"), ("Mei", "#ff8fb1"), ("Kofi", "#ff9f5a"), ("Lena", "#7ef0b5"),
]
MIN_VANS, MAX_VANS = 2, len(VAN_POOL)

JOBS_PER_VAN = max(1, int(env("DESK_JOBS", "6")))

# New orders during the day: the first ORDER_FIRST seconds after the vans set
# off, then one every ORDER_EVERY seconds, ORDERS in all (fewer if the stops run
# out). Dispatch gives each one to the van that can fit it in best.
ORDERS = max(0, int(env("DESK_ORDERS", "10")))
ORDER_FIRST = float(env("DESK_ORDER_FIRST", "15"))
ORDER_EVERY = float(env("DESK_ORDER_EVERY", "8"))


def rounds(count: int) -> list[list[str]]:
    """Plan the day for `count` vans: sweep the stops round the depot by compass
    bearing and cut the sweep into runs of near equal length, so each van gets
    a patch of the city. Each van takes the JOBS_PER_VAN stops of its patch
    nearest the depot, ordered nearest first; the rest of the patch is left for
    new orders."""
    from math import atan2, cos, radians

    def xy(p: dict) -> tuple[float, float]:
        return ((p["lon"] - DEPOT["lon"]) * cos(radians(DEPOT["lat"])), p["lat"] - DEPOT["lat"])

    def far(a: dict, b: dict) -> float:
        (ax, ay), (bx, by) = xy(a), xy(b)
        return (ax - bx) ** 2 + (ay - by) ** 2

    swept = sorted(STOPS, key=lambda s: atan2(xy(STOPS[s])[1], xy(STOPS[s])[0]))
    size, extra = divmod(len(swept), count)
    out, at = [], 0
    for i in range(count):
        run = swept[at:at + size + (1 if i < extra else 0)]
        at += len(run)
        run = sorted(run, key=lambda s: far(DEPOT, STOPS[s]))[:JOBS_PER_VAN]
        order, here = [], DEPOT
        while run:
            nxt = min(run, key=lambda s: far(here, STOPS[s]))
            order.append(nxt)
            run.remove(nxt)
            here = STOPS[nxt]
        out.append(order)
    return out


def order_pool(vans: dict) -> list[str]:
    """The stops no van has at the start of the day: the customers who order later."""
    planned = {s for v in vans.values() for s in v["stops"]}
    return [s for s in STOPS if s not in planned]


def fleet(count: int) -> dict:
    """The vans for a day of `count` vans, each with its round."""
    count = max(MIN_VANS, min(MAX_VANS, count))
    return {f"van-{i + 1}": {"driver": driver, "colour": colour, "stops": stops}
            for i, ((driver, colour), stops) in enumerate(zip(VAN_POOL, rounds(count)))}


# DESK_VANS sets the size of the fleet. The server passes it to the simulator
# and the agents each day, so the vans slider takes effect on the next day.
VAN_COUNT = max(MIN_VANS, min(MAX_VANS, int(env("DESK_VANS", "4"))))
VANS = fleet(VAN_COUNT)

# What each scenario injects. The simulator plays the incident; dispatch has to
# spot it on the log and the agents have to deal with it.
SCENARIOS = {
    "road-closure": "The road ahead is closed. The van stops; dispatch replans its round.",
    "driver-unreachable": "The driver stops answering. The van is parked; its stops move to another van.",
    "not-home": "Nobody is home at the next drop. The parcel stays on the van for a second attempt.",
    "overbilled": "The carrier bills for drops and kilometres the log does not show.",
}

# Incident kinds as the simulator reports them
INCIDENT_KINDS = {"road-closure": "road_closure", "driver-unreachable": "unreachable",
                  "not-home": "not_home"}

# ---------------------------------------------------------------- chaos monkey
# Off by default. When on, the chaos monkey SIGKILLs a random agent every
# CHAOS_MIN to CHAOS_MAX seconds. Its kills go on the log like a Kill click, and
# the supervisor restarts the agent. The Chaos button in the header turns it on
# and off on a running server.
CHAOS = env("DESK_CHAOS", "0") == "1"
CHAOS_AGENTS = [a.strip() for a in env("DESK_CHAOS_AGENTS", ",".join(AGENTS)).split(",") if a.strip()]
CHAOS_MIN = float(env("DESK_CHAOS_MIN", "3"))
CHAOS_MAX = float(env("DESK_CHAOS_MAX", "10"))

# ---------------------------------------------------------------- demo loop
# DESK_DEMO=1 (or the Demo loop button) runs the day unattended, for a booth or
# a screen in the corner: every day the carrier overbills, the incidents in
# DEMO_SCRIPT go in at their times (seconds after the vans set off), approvals
# decide themselves after DEMO_APPROVE_DELAY seconds, and a new day starts
# DEMO_PAUSE seconds after the invoice is settled.
DEMO = env("DESK_DEMO", "0") == "1"
DEMO_PAUSE = float(env("DESK_DEMO_PAUSE", "10"))
DEMO_APPROVE_DELAY = float(env("DESK_DEMO_APPROVE_DELAY", "4"))
DEMO_SCRIPT = [(12, "road-closure"), (30, "not-home"), (50, "driver-unreachable")]
