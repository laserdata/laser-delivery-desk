"""Planning the day for the vans: python -m unittest discover tests"""

import unittest

from fleet import config as C


class FleetTest(unittest.TestCase):
    def test_no_stop_on_two_vans(self):
        for count in range(C.MIN_VANS, C.MAX_VANS + 1):
            vans = C.fleet(count)
            self.assertEqual(len(vans), count)
            stops = [s for v in vans.values() for s in v["stops"]]
            self.assertEqual(len(stops), len(set(stops)), count)

    def test_planned_and_orders_cover_every_stop(self):
        for count in range(C.MIN_VANS, C.MAX_VANS + 1):
            vans = C.fleet(count)
            planned = {s for v in vans.values() for s in v["stops"]}
            pool = C.order_pool(vans)
            self.assertFalse(planned & set(pool), count)
            self.assertEqual(planned | set(pool), set(C.STOPS), count)

    def test_each_van_starts_with_its_jobs(self):
        for count in range(C.MIN_VANS, C.MAX_VANS + 1):
            sizes = {len(v["stops"]) for v in C.fleet(count).values()}
            self.assertEqual(sizes, {C.JOBS_PER_VAN}, count)

    def test_enough_orders_for_a_full_fleet(self):
        self.assertGreaterEqual(len(C.order_pool(C.fleet(C.MAX_VANS))), C.ORDERS)

    def test_count_is_clamped(self):
        self.assertEqual(len(C.fleet(0)), C.MIN_VANS)
        self.assertEqual(len(C.fleet(99)), C.MAX_VANS)

    def test_each_van_has_its_own_driver_and_colour(self):
        vans = C.fleet(C.MAX_VANS)
        self.assertEqual(len({v["driver"] for v in vans.values()}), C.MAX_VANS)
        self.assertEqual(len({v["colour"] for v in vans.values()}), C.MAX_VANS)


if __name__ == "__main__":
    unittest.main()
