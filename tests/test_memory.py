"""Wording messages from a customer's history: python -m unittest discover tests"""

import json
import unittest

from fleet import memory


class Item:
    def __init__(self, value):
        self.value = value

    def json(self):
        if isinstance(self.value, Exception):
            raise self.value
        return json.loads(self.value)


class MemoryTest(unittest.TestCase):
    def test_no_history_no_follow_up(self):
        self.assertIsNone(memory.follow_up("not_home", []))

    def test_not_home_twice(self):
        text = memory.follow_up("not_home", [{"kind": "not_home", "clock": 600}])
        self.assertIn("last time", text)

    def test_late_before(self):
        self.assertIn("second time", memory.follow_up("road_closure", [{"kind": "unreachable", "clock": 600}]))
        self.assertIn("3 times", memory.follow_up("road_closure", [{"kind": "road_closure", "clock": c} for c in (1, 2, 3)]))

    def test_describe(self):
        self.assertEqual(memory.describe([{"kind": "not_home", "clock": 642}, {"kind": "road_closure", "clock": 665}]),
                         "not home at 10:42; late (road closure) at 11:05")

    def test_fact_names_the_customer(self):
        f = memory.fact("s03", "not_home", 600, "inc-1")
        self.assertEqual(f["customer"], "Ava Patel")
        self.assertEqual((f["stop"], f["kind"], f["clock"], f["incident"]), ("s03", "not_home", 600, "inc-1"))

    def test_same_incident_same_fact(self):
        # A redelivered notify step remembers again; dedup needs identical facts.
        first = memory.fact("s03", "not_home", 600, "01M5CB259HE2GM69JZ3XF2GBVP")
        again = memory.fact("s03", "not_home", 600, "01M5CB259HE2GM69JZ3XF2GBVP")
        self.assertEqual(json.dumps(first, sort_keys=True), json.dumps(again, sort_keys=True))

    def test_incident_time_from_the_ulid(self):
        self.assertEqual(memory.incident_time("01M5CB259HE2GM69JZ3XF2GBVP"), 1792489362737000)
        self.assertIsNone(memory.incident_time("inc-1"))

    def test_decode_keeps_only_our_facts(self):
        self.assertEqual(memory.decode(Item('{"stop": "s01"}')), {"stop": "s01"})
        self.assertIsNone(memory.decode(Item('{"other": 1}')))
        self.assertIsNone(memory.decode(Item('"text"')))
        self.assertIsNone(memory.decode(Item(ValueError("bad"))))


if __name__ == "__main__":
    unittest.main()
