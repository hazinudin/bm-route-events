import unittest
from src.route_events.points.defect.model import RouteDefects


class TestRouteDefect(unittest.TestCase):
    def test_from_excel(self):
        excel_path = "tests/domain/route_points/defect_010362.xlsx"

        e = RouteDefects.from_excel(
            excel_path,
            '010362',
            data_year=2018
        )

        self.assertTrue(True)