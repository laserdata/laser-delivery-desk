"""Folding the replayed topics into frames: python -m unittest discover tests"""

import unittest

from fleet import timeline


def pos(van, clock, ts, **extra):
    return {"van": van, "lat": -37.8, "lon": 144.9, "status": "moving", "target": "s01",
            "metres": 0, "incident": "", "clock": clock, "ts": ts, **extra}


class TimelineTest(unittest.TestCase):
    def test_one_frame_per_minute_with_the_last_position(self):
        out = timeline.build([pos("van-1", 540, 1), pos("van-1", 540, 2, status="dwelling"),
                              pos("van-2", 541, 3)], [])
        self.assertEqual(out["start"], 540)
        self.assertEqual(out["end"], 541)
        frames = dict((c, vans) for c, vans in out["frames"])
        self.assertEqual(frames[540]["van-1"]["status"], "dwelling")
        self.assertNotIn("van-1", frames[541])

    def test_events_get_the_minute_they_happened_in(self):
        positions = [pos("van-1", 540, 100), pos("van-1", 541, 200), pos("van-1", 542, 300)]
        events = [{"kind": "delivered", "ts": 250}, {"kind": "injected", "ts": 50}]
        out = timeline.build(positions, events)
        self.assertEqual([(e["kind"], e["clock"]) for e in out["events"]],
                         [("injected", 540), ("delivered", 541)])

    def test_out_of_order_records_are_sorted(self):
        out = timeline.build([pos("van-1", 541, 200), pos("van-1", 540, 100)], [])
        self.assertEqual([c for c, _ in out["frames"]], [540, 541])

    def test_nothing_on_the_log(self):
        out = timeline.build([], [{"kind": "agent_up", "ts": 1}])
        self.assertEqual(out["frames"], [])
        self.assertEqual(out["events"][0]["clock"], out["start"])


if __name__ == "__main__":
    unittest.main()
