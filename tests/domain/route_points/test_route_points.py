import unittest
from route_events.points.base import RoutePointEvents
import polars as pl


class TestRoutePoints(unittest.TestCase):
    def test_route_points(self):
        df = pl.DataFrame({
            "LINKID": ["a" for _ in range(3)],
            "STA": [0, 10, 20],
            "LANE_CODE": ["L1" for _ in range(3)]
        })

        e = RoutePointEvents(
            df.to_arrow(),
            route="a"
        )

        self.assertTrue(e.min_sta == 0)
        self.assertTrue(e.max_sta == 20)
        self.assertFalse(e.no_data)
