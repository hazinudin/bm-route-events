import unittest

import polars as pl
from sqlalchemy import create_engine

from src.route_events import RouteRNI
from src.service import RouteRNIValidation
from src.service.validation_result.result import ValidationResult


MEDIAN_FIXTURE = 'tests/domain/route_segments/input_excels/rni_7_01-08-2025_095037_4760.xlsx'
NO_MEDIAN_FIXTURE = 'tests/domain/route_segments/input_excels/balai_5_15010.xlsx'

MEDIAN_ROUTE = '2402416'
NO_MEDIAN_ROUTE = '15010'

SEGMENT_KEY = ['LINKID', 'FROM_STA', 'TO_STA']


class DecreasingSurfWidthCheckTest(unittest.TestCase):
    """
    `decreasing_surf_width_check` must compare per-side max() of SURF_WIDTH for segments
    with a median, so lane count changes do not look like a surface width change.
    """

    @staticmethod
    def read_fixture(path: str) -> pl.DataFrame:
        df = pl.read_excel(path)
        return df.rename(
            {c: c.strip().upper() for c in df.columns}
        ).with_columns(
            pl.col('SURF_WIDTH').cast(pl.Float64)
        )

    @staticmethod
    def segment_count(df: pl.DataFrame) -> int:
        return df.group_by(SEGMENT_KEY).len().height

    @staticmethod
    def run_check(route: str, events: pl.DataFrame, prev: pl.DataFrame) -> pl.DataFrame:
        check = RouteRNIValidation(
            route,
            RouteRNI(artable=events.to_arrow(), route=route, data_year=2025),
            lrs=None,
            sql_engine=create_engine('sqlite://'),
            results=ValidationResult(route),
            survey_year=2025
        )

        # Bypass the database lookup of last year's data.
        check._prev_data = RouteRNI(artable=prev.to_arrow(), route=route, data_year=2024)

        check.decreasing_surf_width_check()

        return check._result.get_all_messages()

    def setUp(self):
        self.median = self.read_fixture(MEDIAN_FIXTURE)
        self.no_median = self.read_fixture(NO_MEDIAN_FIXTURE)

    def test_median_no_change(self):
        messages = self.run_check(MEDIAN_ROUTE, self.median, self.median)

        self.assertTrue(messages.is_empty())

    def test_median_width_decrease_detected(self):
        current = self.median.with_columns(pl.col('SURF_WIDTH').sub(1.0))

        messages = self.run_check(MEDIAN_ROUTE, current, self.median)

        # SURF_WIDTH is recorded per side, so both sides are reported as a total.
        self.assertEqual(messages.height, self.segment_count(self.median))
        self.assertTrue(messages['msg'].str.contains('dari 18.0 ke 16.0').all())

    def test_median_lane_count_change_is_not_a_decrease(self):
        # Same widths, one lane row dropped on each side: no width change at all.
        current = self.median.filter(~pl.col('LANE_CODE').is_in(['L2', 'R2']))

        messages = self.run_check(MEDIAN_ROUTE, current, self.median)

        self.assertTrue(messages.is_empty())

    def test_median_width_decrease_detected_regardless_of_lane_count(self):
        # Both sides narrowed by 0.5 m while gaining an extra lane row per side.
        narrowed = self.median.with_columns(pl.col('SURF_WIDTH').sub(0.5))
        extra_lanes = pl.concat([
            narrowed.filter(pl.col('LANE_CODE').eq('L1')).with_columns(
                pl.col('LANE_CODE').replace('L1', 'L3')
            ),
            narrowed.filter(pl.col('LANE_CODE').eq('R1')).with_columns(
                pl.col('LANE_CODE').replace('R1', 'R3')
            )
        ])
        current = pl.concat([narrowed, extra_lanes], how='diagonal_relaxed')

        messages = self.run_check(MEDIAN_ROUTE, current, self.median)

        self.assertEqual(messages.height, self.segment_count(self.median))
        self.assertTrue(messages['msg'].str.contains('dari 18.0 ke 17.0').all())

    def test_no_median_width_decrease_detected(self):
        current = self.no_median.with_columns(pl.col('SURF_WIDTH').sub(0.5))

        messages = self.run_check(NO_MEDIAN_ROUTE, current, self.no_median)

        self.assertEqual(messages.height, self.segment_count(self.no_median))
        self.assertTrue(messages['msg'].str.contains('tidak memiliki median').all())

        # Single carriageway width is read with max(), e.g. segment 0-10: 7.0 -> 6.5
        segment = messages.filter(pl.col('msg').str.contains('Segmen 0.0-10.0'))
        self.assertEqual(segment.height, 1)
        self.assertIn('dari 7.0 ke 6.5', segment['msg'][0])


if __name__ == '__main__':
    unittest.main()
