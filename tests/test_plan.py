"""Route planning, without a server: python -m unittest discover tests"""

import unittest

from fleet import config as C
from fleet import plan


class PlanTest(unittest.TestCase):
    def test_nearest_order_visits_every_stop_once(self):
        stops = C.VANS["van-1"]["stops"]
        order = plan.nearest_order(C.DEPOT, stops)
        self.assertEqual(sorted(order), sorted(stops))

    def test_detour_puts_the_blocked_stop_last(self):
        route = ["s01", "s02", "s03", "s04"]
        order = plan.detour(C.STOPS["s01"], route, "s02")
        self.assertEqual(order[-1], "s02")
        self.assertEqual(sorted(order), sorted(route))

    def test_retry_last(self):
        self.assertEqual(plan.retry_last(["s01", "s02", "s03"], "s01"), ["s02", "s03", "s01"])

    def test_receiver_prefers_the_nearer_van_with_less_to_do(self):
        routes = {v: {"stops": [], "status": "active"} for v in ("van-1", "van-2", "van-3")}
        positions = {"van-1": C.STOPS["s16"], "van-2": C.STOPS["s17"], "van-3": C.STOPS["s04"]}
        counts = {"van-2": 1, "van-3": 1}
        self.assertEqual(plan.pick_receiver("van-1", routes, positions, counts), "van-2")

    def test_no_receiver_when_every_other_van_is_parked(self):
        routes = {"van-1": {"stops": [], "status": "active"}, "van-2": {"stops": [], "status": "parked"}}
        self.assertIsNone(plan.pick_receiver("van-1", routes, {}, {}))

    def test_reassign_keeps_both_rounds(self):
        order = plan.reassign(C.DEPOT, ["s01", "s02"], ["s02", "s16"])
        self.assertEqual(sorted(order), ["s01", "s02", "s16"])

    def test_etas_increase_along_the_round(self):
        times = plan.etas(600, C.DEPOT, ["s16", "s20", "s19"])
        self.assertLess(times["s16"], times["s20"])
        self.assertLess(times["s20"], times["s19"])

    def test_window_is_an_hour_on_the_half_hour(self):
        self.assertEqual(plan.window(617), (600, 660))

    def test_insertion_on_the_way_costs_less(self):
        # Fitzroy (s06) sits between the depot and Collingwood (s07); Yarraville (s17) does not.
        on_way = plan.insertion(C.DEPOT, ["s07"], "s06")[1]
        off_way = plan.insertion(C.DEPOT, ["s07"], "s17")[1]
        self.assertLess(on_way, off_way)

    def test_pick_van_takes_the_nearer_van(self):
        rounds = {"van-1": ["s16"], "van-2": ["s07"]}
        positions = {"van-1": C.STOPS["s16"], "van-2": C.STOPS["s07"]}
        van, at, _ = plan.pick_van("s08", rounds, positions)
        self.assertEqual(van, "van-2")

    def test_pick_van_spreads_the_load(self):
        positions = {"van-1": C.STOPS["s07"], "van-2": C.STOPS["s06"]}
        rounds = {"van-1": ["s07", "s09", "s10", "s25", "s27", "s34"], "van-2": []}
        self.assertEqual(plan.pick_van("s08", rounds, positions)[0], "van-2")

    def test_no_van(self):
        self.assertIsNone(plan.pick_van("s01", {}, {}))


if __name__ == "__main__":
    unittest.main()
