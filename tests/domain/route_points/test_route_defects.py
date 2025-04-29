import unittest
from src.route_events.points.defect.model import RouteDefects


class TestRouteDefect(unittest.TestCase):
    def test_from_excel(self):
        excel_path = "tests/domain/route_points/defect_01001.xlsx"

        e = RouteDefects.from_excel(
            excel_path,
            '01001',
            data_year=2018
        )

        self.assertTrue(True)