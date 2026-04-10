import unittest
from src.service.points.analysis.join import segments_points_join
from route_events import RouteRoughness, RouteDefects
import polars as pl


class TestSegmentsPointsJoinFWD(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.iri = RouteRoughness.from_excel(
            "tests/domain/route_segments/input_excels/iri_1_010362.xlsx",
            linkid="010362",
            ignore_review=True,
        )
        cls.defc = RouteDefects.from_excel(
            "tests/domain/route_points/defect_010362.xlsx", linkid="010362"
        )

    def test_inner_join_basic(self):
        inner = segments_points_join(segments=self.iri, points=self.defc, how="inner")
        self.assertFalse(inner.is_empty())
        self.assertIn(self.iri._linkid_col, inner.columns)
        self.assertIn(self.defc._sta_col, inner.columns)

    def test_anti_join_basic(self):
        anti = segments_points_join(segments=self.iri, points=self.defc, how="anti")
        self.assertFalse(anti.is_empty())

    def test_inner_join_with_segment_select(self):
        inner = segments_points_join(
            segments=self.iri,
            points=self.defc,
            how="inner",
            segment_select=["IRI"],
        )
        self.assertFalse(inner.is_empty())
        self.assertIn("IRI", inner.columns)

    def test_inner_join_with_point_select(self):
        inner = segments_points_join(
            segments=self.iri,
            points=self.defc,
            how="inner",
            point_select=["SURF_TYPE"],
        )
        self.assertFalse(inner.is_empty())
        self.assertIn("SURF_TYPE", inner.columns)

    def test_inner_join_with_segment_and_point_select(self):
        inner = segments_points_join(
            segments=self.iri,
            points=self.defc,
            how="inner",
            segment_select=["IRI"],
            point_select=["SURF_TYPE"],
        )
        self.assertFalse(inner.is_empty())
        self.assertIn("IRI", inner.columns)
        self.assertIn("SURF_TYPE", inner.columns)

    def test_inner_join_with_suffix_no_conflict(self):
        inner = segments_points_join(
            segments=self.iri,
            points=self.defc,
            how="inner",
            segment_select=["IRI"],
            suffix="_r",
        )
        self.assertFalse(inner.is_empty())
        self.assertIn("IRI", inner.columns)

    def test_inner_join_with_suffix_conflict(self):
        inner = segments_points_join(
            segments=self.iri,
            points=self.defc,
            how="inner",
            segment_select=["SURVEY_DIREC"],
            point_select=["SURVEY_DIREC"],
            suffix="_r",
        )
        self.assertFalse(inner.is_empty())
        self.assertIn("SURVEY_DIREC", inner.columns)
        self.assertIn("SURVEY_DIREC_r", inner.columns)

    def test_inner_join_surface_thickness_check_scenario(self):
        inner = segments_points_join(
            segments=self.iri,
            points=self.defc,
            how="inner",
            point_select=["SURF_TYPE"],
            segment_select=["IRI"],
            suffix="_r",
        )
        self.assertFalse(inner.is_empty())
        self.assertIn("IRI", inner.columns)
        self.assertIn("SURF_TYPE", inner.columns)
        self.assertIn(self.iri._from_sta_col, inner.columns)
        self.assertIn(self.iri._to_sta_col, inner.columns)

    def test_inner_join_median_direction_check_scenario(self):
        inner = segments_points_join(
            segments=self.iri,
            points=self.defc,
            how="inner",
            segment_select=["IRI"],
            suffix="_r",
        )
        self.assertFalse(inner.is_empty())
        self.assertIn("IRI", inner.columns)
        self.assertIn(self.iri._from_sta_col, inner.columns)
        self.assertIn(self.iri._to_sta_col, inner.columns)

    def test_anti_join_filter_by_sta_range(self):
        anti = segments_points_join(
            segments=self.iri,
            points=self.defc,
            how="anti",
            segment_select=["IRI"],
        )
        self.assertFalse(anti.is_empty())

    def test_anti_join_with_suffix(self):
        anti = segments_points_join(
            segments=self.iri,
            points=self.defc,
            how="anti",
            segment_select=["IRI"],
            suffix="_r",
        )
        self.assertFalse(anti.is_empty())

    def test_invalid_how_raises_error(self):
        with self.assertRaises(ValueError) as context:
            segments_points_join(segments=self.iri, points=self.defc, how="left")
        self.assertIn("Only supports 'inner' or 'anti' join", str(context.exception))


if __name__ == "__main__":
    unittest.main()
