import unittest
from src.service.points.analysis.join import segments_points_join
from route_events import RouteRoughness, RouteDefects


class TestSegmentsPointsJoin(unittest.TestCase):
    def test_join(self):
        iri = RouteRoughness.from_excel(
            'tests/domain/route_segments/input_excels/iri_1_010362.xlsx',
            linkid='010362',
            ignore_review=True
        )

        defc = RouteDefects.from_excel(
            'tests/domain/route_points/defect_010362.xlsx',
            linkid='010362'
        )

        inner = segments_points_join(
            segments=iri,
            points=defc,
            how='inner'
        )

        self.assertFalse(inner.is_empty())

        anti = segments_points_join(
            segments=iri,
            points=defc,
            how='anti'
        )

        self.assertFalse(anti.is_empty())
