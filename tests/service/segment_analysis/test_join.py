import unittest
from src.service.segments.analysis.join import segments_join
from src.route_events import RouteSegmentEvents
import polars as pl


class TestSegmentsJoin(unittest.TestCase):
    def test_join_agg(self):
        events = RouteSegmentEvents.from_excel(
            'tests/domain/route_segments/input_excels/balai_5_15010.xlsx',
            config_path='tests/domain/route_segments/input_config/rni_config.json',
            linkid='15010',
            ignore_review=True
        )

        joined = segments_join(
            left=events,
            right=events,
            l_select=['SURF_TYPE'],
            r_select=['LANE_WIDTH'],
            l_agg=[pl.col('SURF_TYPE').max()],
            r_agg=[pl.col('LANE_WIDTH').sum()]
        )

        self.assertTrue(joined.shape[0] == 924)