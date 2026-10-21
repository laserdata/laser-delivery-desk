"""The demo loop: a whole day, unattended, again and again.

Each day starts clean with the carrier set to overbill. The incidents in
config.DEMO_SCRIPT go in at their times, approvals decide themselves after a
short delay (a person who clicks first still decides), and once every van is
back and the invoice is settled the loop pauses and starts the next day.
"""

from __future__ import annotations

import asyncio
import time

from . import config as C

DAY_TIMEOUT = 600  # seconds before a day that never finishes is left behind


class DemoLoop:
    def __init__(self, desk) -> None:
        self.desk = desk  # the server's Desk: new_day, inject, day_settled, broadcast
        self.task: asyncio.Task | None = None

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()

    def start(self) -> None:
        if not self.running:
            self.task = asyncio.create_task(self._run())

    def stop(self) -> None:
        if self.running:
            self.task.cancel()

    async def halt(self) -> None:
        """Stop and wait until the loop has finished, so it can start again."""
        task = self.task
        self.stop()
        if task is not None:
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _say(self, text: str) -> None:
        self.desk.demo_text = text
        await self.desk.broadcast({"t": "demo", "running": self.running, "text": text})

    async def _run(self) -> None:
        day = 0
        try:
            while True:
                day += 1
                await self._say(f"Day {day}: starting")
                await self.desk.new_day()
                await self.desk.inject("overbilled")
                started = time.monotonic()
                for at, scenario in C.DEMO_SCRIPT:
                    await self._say(f"Day {day}: {scenario.replace('-', ' ')} at {at}s")
                    await asyncio.sleep(max(0.0, started + at - time.monotonic()))
                    for _ in range(20):  # wait for a van the incident fits
                        if (await self.desk.inject(scenario)).get("ok"):
                            break
                        await asyncio.sleep(1)
                await self._say(f"Day {day}: finishing the rounds")
                deadline = time.monotonic() + DAY_TIMEOUT
                while not await self.desk.day_settled() and time.monotonic() < deadline:
                    await asyncio.sleep(1)
                await self._say(f"Day {day} done. Next day in {C.DEMO_PAUSE:g}s")
                await asyncio.sleep(C.DEMO_PAUSE)
        except asyncio.CancelledError:
            pass
        except Exception as error:  # noqa: BLE001
            print(f"  demo loop stopped: {error!r}", flush=True)
        finally:
            self.desk.demo_text = ""
            await self.desk.broadcast({"t": "demo", "running": False, "text": ""})
