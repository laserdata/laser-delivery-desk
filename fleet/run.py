"""A whole day without a browser:

    python -m fleet.run                       every incident, overbilled, one kill
    python -m fleet.run --scenario not-home   one incident
    python -m fleet.run --chaos               the chaos monkey on as well
    python -m fleet.run --no-kill --no-overbill

It starts the same simulator and agent processes as the web demo, puts the
incidents in, kills the routing agent holding a replan once (unless --no-kill),
lets approvals decide themselves, and waits for the day to settle. Then it
prints the exceptions, the readers and the evals, and exits non-zero if any
eval failed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time

INCIDENTS = ["road-closure", "not-home", "driver-unreachable"]


def parse() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--scenario", action="append", choices=INCIDENTS,
                   help="an incident to put in (repeat for more); default all three")
    p.add_argument("--chaos", action="store_true", help="turn the chaos monkey on")
    p.add_argument("--no-kill", action="store_true", help="do not kill routing mid-replan")
    p.add_argument("--no-overbill", action="store_true", help="the carrier bills correctly")
    p.add_argument("--timeout", type=float, default=600, help="seconds to wait for the day to settle")
    return p.parse_args()


async def main(args: argparse.Namespace) -> int:
    from .server import Desk

    desk = Desk()
    desk.chaos_on = args.chaos
    await desk.start_day(fresh=True)
    try:
        if not args.no_overbill:
            await desk.inject("overbilled")
        killer = None if args.no_kill else asyncio.create_task(kill_mid_replan(desk))
        start = time.monotonic()
        for n, scenario in enumerate(args.scenario or INCIDENTS):
            await asyncio.sleep(max(0.0, start + 10 + 15 * n - time.monotonic()))
            for _ in range(30):
                result = await desk.inject(scenario)
                if result.get("ok"):
                    print(f"{scenario}: {result['van']}", flush=True)
                    break
                await asyncio.sleep(1)
            else:
                print(f"{scenario}: no van could take it", flush=True)
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            if desk.evals is not None:
                break
            await asyncio.sleep(0.5)
        else:
            print("the day did not settle in time", flush=True)
            return 2
        from .core import get_json, scan_json
        exceptions = await scan_json(desk.fleet.exceptions)
        check = await get_json(desk.fleet.day, "invoice_check") or {}
        report(desk, exceptions, check)
        return 0 if all(e["ok"] for e in desk.evals) else 1
    finally:
        if killer is not None:
            killer.cancel()
        await desk.stop_day()
        await desk.laser.close()


async def kill_mid_replan(desk) -> None:
    """Watch from the start of the day and kill the routing agent that is holding
    a replan, while it holds: the first hold whose step has not finished."""
    while True:
        feed = list(desk.feed)
        for n, e in enumerate(feed):
            if e["kind"] != "holding":
                continue
            done = any(later["kind"] == "step" and later.get("state") == "done"
                       and later.get("incident") == e.get("incident") and later.get("step") == e.get("step")
                       for later in feed[n + 1:])
            if done:
                continue
            name = e.get("agent") or "routing"
            pid = await desk.sup.kill(name)
            await desk.fleet.event("killed", agent=name, text=f"{name} pid {pid} killed (SIGKILL)")
            print(f"killed {name} pid {pid} mid-replan", flush=True)
            return
        await asyncio.sleep(0.2)


def report(desk, exceptions: dict, check: dict) -> None:
    print("\nExceptions")
    for e in sorted(exceptions.values(), key=lambda e: e["opened"]):
        print(f"  {e['state']:<9} {e['van']:<6} {e['kind']:<13} {e.get('summary', '')}")
    print(f"\nInvoice\n  {check.get('summary', 'not checked')}")
    print("\nReaders")
    for key, value in desk.readers.items():
        print(f"  {key:<16} {json.dumps(value)}")
    print("\nEvals")
    for e in desk.evals:
        print(f"  {'PASS' if e['ok'] else 'FAIL'}  {e['name']}: {e['detail']}")


if __name__ == "__main__":
    arguments = parse()
    os.environ.setdefault("DESK_AUTO_DECIDE", "2")
    if not arguments.no_kill:
        os.environ.setdefault("DESK_STEP_PAUSE", "3")
    sys.exit(asyncio.run(main(arguments)))
