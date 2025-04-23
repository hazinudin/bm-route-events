from src.route_events.segments.base.model import RouteSegmentEvents
import unittest
import pyarrow as pa
import polars as pl
from pydantic import ValidationError


class TestRouteSegments(unittest.TestCase):
    def test_route_segments_from_excel(self):
        """
        Load data from Excel file with correct schema.
        """
        excel_path = 'tests/domain/route_segments/input_excels/balai_5_15010.xlsx'
        config_path = 'tests/domain/route_segments/input_config/rni_config.json'
        se = RouteSegmentEvents.from_excel(
            excel_path=excel_path,
            config_path=config_path,
            linkid='ALL',
            ignore_review=True
            )

        self.assertTrue(type(se.artable) == pa.Table)

    def test_multiple_route_from_excel(self):
        """
        Load data from Excel containing multiple routes.
        """
        excel_path = 'tests/domain/route_segments/input_excels/balai_5_15007_15008_15009_15010.xlsx'
        config_path = 'tests/domain/route_segments/input_config/rni_config.json'
        se = RouteSegmentEvents.from_excel(
            excel_path=excel_path,
            config_path=config_path,
            linkid='ALL',
            ignore_review=True
            )

        self.assertTrue(type(se.artable) == pa.Table)

    def test_route_segments_with_invalid_data_from_excel(self):
        """
        Load Excel data with incorrect data type.
        """
        excel_path = 'tests/domain/route_segments/input_excels/balai_5_15010_incorrect_data_type.xlsx'
        config_path = 'tests/domain/route_segments/input_config/rni_config.json'
        
        
        with self.assertRaises(ValidationError):
            se = RouteSegmentEvents.from_excel(
                excel_path=excel_path,
                config_path=config_path,
                linkid='ALL',
                ignore_review=True
                )
            
    def test_segment(self):
        """
        Test segment measurement and STA related method.
        """
        df = pl.DataFrame({
            "LINKID": ["a" for _ in range(3)],
            "FROM_STA": [0, 10, 20],
            "TO_STA": [10, 20, 30],
            "LANE_CODE": ["L1" for _ in range(3)]
        })

        se = RouteSegmentEvents(
            df.to_arrow(),
            route = "a"
        )

        # STA
        self.assertTrue(se.last_segment.from_sta == 20)
        self.assertTrue(se.last_segment.to_sta == 30)
        self.assertTrue(se.max_to_sta == 30)
        self.assertTrue(se.min_from_sta == 0)

        # Get all segments
        self.assertTrue(se.get_all_segments().shape[0] == 3)

    def test_duplicate(self):
        """
        Test duplicate segment.
        """
        df = pl.DataFrame(
            {
                "LINKID": ["a" for _ in range(6)],
                "FROM_STA": [0, 0, 10, 20, 40, 40],
                "TO_STA": [10, 10, 20, 30, 50, 50],
                "LANE_CODE": ["L1" for _ in range(6)]
            }
        )

        se = RouteSegmentEvents(
            df.to_arrow(),
            route = "a"
        )

        self.assertTrue(len(se.is_duplicate_segment()) == 2)

        df = pl.DataFrame(
            {
                "LINKID": ["a" for _ in range(4)],
                "FROM_STA": [0, 10, 20, 40],
                "TO_STA": [10, 20, 30, 50],
                "LANE_CODE": ["L1" for _ in range(4)]
            }
        )

        se = RouteSegmentEvents(
            df.to_arrow(),
            route = "a"
        )

        self.assertTrue(len(se.is_duplicate_segment()) == 0)


    def test_incorrect_lane_sequence(self):
        """
        Test incorrect lane sequence.
        """
        df = pl.DataFrame(
            {
                "LINKID": ["a" for _ in range(6)],
                "FROM_STA": [0, 0, 10, 10, 20, 20],
                "TO_STA": [10, 10, 20, 20, 30, 30],
                "LANE_CODE": ["L1", "L3", "L1", "L3", "L1", "L2"]
            }
        )

        se = RouteSegmentEvents(
            df.to_arrow(),
            route = "a"
        )

        self.assertTrue(len(se.incorrect_lane_sequence()) == 2)

    def test_segment_length_and_sta(self):
        """
        Test incorrect segment length.    
        """
        df = pl.DataFrame(
            {
                "LINKID": ["a" for _ in range(6)],
                "FROM_STA": [0, 0, 10, 10, 20, 20],
                "TO_STA": [10, 10, 20, 20, 25, 25],
                "LANE_CODE": ["L1", "L2", "L1", "L2", "L1", "L2"],
                "SEGMENT_LENGTH": [0.1, 0.98, 0.11, 0.110000003, 0.05999, 0.0399]
            }
        )

        se = RouteSegmentEvents(
            df.to_arrow(),
            route = "a",
            segment_length = 0.1
        )

        seg_len_error = se.incorrect_segment_length(tolerance=0.01)
        self.assertTrue(len(seg_len_error) == 2)

        sta_diff_error = se.incorrect_sta_diff(tolerance=0.01)
        self.assertTrue(len(sta_diff_error) == 3)

    def test_sta_gap(self):
        """
        Test STA gap.
        """ 
        df = pl.DataFrame(
            {
                "LINKID": ["a" for _ in range(5)],
                "FROM_STA": [0, 0, 10, 20, 20],
                "TO_STA": [10, 10, 20, 25, 25],
                "LANE_CODE": ["L1", "L2", "L1", "L1", "L2"],
            }
        )

        se = RouteSegmentEvents(
            df.to_arrow(),
            route = "a"
        )

        gap = se.sta_gap()

        self.assertTrue(len(gap) == 1)
        self.assertTrue(gap[0].from_sta == 10)
        self.assertTrue(gap[0].to_sta == 20)
    
    def test_overlapping_segments(self):
        """
        Test overlapping segments.
        """
        df = pl.DataFrame(
            {
                "LINKID": ["a" for _ in range(6)],
                "FROM_STA": [0, 0, 9, 10, 15, 20],
                "TO_STA": [10, 10, 20, 20, 25, 25],
                "LANE_CODE": ["L1", "L2", "L1", "L2", "L1", "L2"]
            }
        )

        se = RouteSegmentEvents(
            df.to_arrow(),
            route = "a"
        )

        ol = se.overlapping_segments()

        self.assertTrue(len(ol) == 2)

    def test_segment_attribute_n_unique(self):
        """
        Test segment attribute n unique.
        """
        df = pl.DataFrame(
            {
                "LINKID": ["a" for _ in range(6)],
                "FROM_STA": [0, 0, 10, 10, 20, 20],
                "TO_STA": [10, 10, 20, 20, 25, 25],
                "LANE_CODE": ["L1", "L2", "L1", "L2", "L1", "L2"],
                "ATTR": [1, 1, 2, 3, 4, 4],
                "ATTR2": [1, None, 1, None, 1, 3]
            }
        )

        se = RouteSegmentEvents(
            df.to_arrow(),
            route = "a"
        )

        dtos = se.segment_attribute_n_unique(
            ['ATTR', 'ATTR2'],
            filter = ('gt', 1)
        )

        self.assertTrue(len(dtos) == 2)
