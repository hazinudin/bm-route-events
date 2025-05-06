import pyarrow as pa
import polars as pl
from .schema import RouteSegmentEventSchema
from pydantic import TypeAdapter
from typing import List, Literal, Union, Type, Tuple
from ...geometry import Points
from ...geometry import LAMBERT_WKT
from .dto import Segment, CenterlineSegment, OverlappingSegment
from .utils import to_meter
from functools import cached_property


class RouteSegmentEvents(object):
    """
    Model of Segment Events in a route.
    """
    @classmethod
    def from_excel(
        cls, 
        excel_path: str, 
        config_path: str, 
        linkid: str | list = 'ALL', 
        linkid_col: str = 'LINKID',
        ignore_review = False,
        data_year: int = None,
        data_semester: int = None,
        segment_length: float = 0.1
        ):
        """
        Parse data from Excel file to Arrow format.
        """
        schema = RouteSegmentEventSchema(config_path=config_path, ignore_review_err=ignore_review)
        df_str = pl.read_excel(
            excel_path, 
            engine='calamine',
            infer_schema_length=None
            ).rename(
                str.upper
            ).cast(
                pl.String  # Cast all values into string for Pydantic validation.
            )

        # Validate using Pydantic
        ta = TypeAdapter(List[schema.model])
        df = pl.DataFrame(
            ta.validate_python(df_str.to_dicts()),
            infer_schema_length=None
        )

        if linkid == 'ALL':
            pass
        elif (type(linkid) == str) and (linkid != 'ALL'):
            df = df.filter(pl.col(linkid_col) == linkid)
        elif type(linkid) == list:
            df = df.filter(pl.col(linkid_col).str.is_in(linkid))
        else:
            raise TypeError(f"LINKID argument with type {type(linkid)} is invalid type.")
        
        return cls(
            artable=df.to_arrow(),
            route=linkid,
            segment_length=segment_length,
            data_year=data_year,
            data_semester=data_semester
        )
    
    def __init__(
            self, 
            artable: pa.Table, 
            route: str = None,
            segment_length:float = 0.1,
            data_year: int = None,
            data_semester: Literal[1,2] = None,
            sta_unit: str = 'dm'
        ):
        # Columns
        self._linkid_col = 'LINKID'
        self._from_sta_col = 'FROM_STA'
        self._to_sta_col = 'TO_STA'
        self._lane_code_col = 'LANE_CODE'
        self._seg_len_col = 'SEGMENT_LENGTH'
        self._lat_col = 'TO_STA_LAT'
        self._long_col = 'TO_STA_LONG'
        self._year_col = 'SURVEY_YEAR'
        self._semester_col = 'SEMESTER'

        # Units
        self._segment_length_unit = 'km'
        self._sta_unit = sta_unit

        self.artable = artable
        self._pl_df = pl.from_arrow(self.artable)
        self._route_id = route  # Route of the events
        self._lane_data = True  # Indicator if the events data is lane based
        self._data_year = data_year
        self._data_semester = data_semester
        self._segment_length = segment_length

        if (self.pl_df.get_column(self._long_col, default=None) is None) or (self.pl_df.get_column(self._lat_col, default=None) is None):
            self._points_4326 = None
            self._points_lambert = None
        else:
            # Points geometry
            self._points_4326 = Points(
                self._pl_df.select([
                    self._linkid_col,
                    self._from_sta_col,
                    self._to_sta_col,
                    self._lane_code_col,
                    self._long_col, 
                    self._lat_col
                ]),
                long_col = self._long_col,
                lat_col = self._lat_col,
                wkt='EPSG:4326',
                ids_column = [
                    self._linkid_col,
                    self._from_sta_col,
                    self._to_sta_col,
                    self._lane_code_col
                ]
            )

            self._points_lambert = self._points_4326.transform(
                LAMBERT_WKT, 
                invert = True
            )

    @property
    def no_data(self) -> bool:
        """
        Return True if data is empty.
        """
        return self._pl_df.is_empty()
    
    @property
    def lane_data(self) -> bool:
        """
        Return True if events is lane based data.
        """
        return self._lane_data

    @property
    def route_id(self) -> str:
        return self._route_id
    
    @property
    def year(self) -> int:
        return self._data_year
    
    @property
    def semester(self) -> int:
        return self._data_semester

    @cached_property
    def pl_df(self) -> pl.DataFrame:
        return self._pl_df
    
    @property
    def points_lambert(self):
        return self._points_lambert
    
    @property
    def lanes(self) -> List:
        """
        Return all available lanes.
        """
        return self.pl_df[self._lane_code_col].unique().to_list()
    
    @property
    def seg_len_conversion(self):
        return to_meter[self._segment_length_unit]
    
    @property
    def sta_conversion(self):
        return to_meter[self._sta_unit]
    
    @property
    def last_segment(self) -> Segment:
        """
        Return segment with the largest FROM_STA number.
        """
        last_segment = self.pl_df.filter(
            (pl.col(self._from_sta_col) == self.pl_df[self._from_sta_col].max()) &
            (pl.col(self._to_sta_col) == self.pl_df[self._to_sta_col].max())
        )
        
        return self._segment_dto_mapper(last_segment)[0]
    
    @property
    def max_to_sta(self) -> int:
        """
        Return largest TO STA number.
        """
        return self.pl_df[self._to_sta_col].max()
    
    @property
    def max_from_sta(self) -> int:
        """
        Return largets FROM STA number.
        """
        return self.pl_df[self._from_sta_col].max()
    
    @property
    def min_from_sta(self) -> int:
        """
        Return smallest FROM STA number.
        """
        return self.pl_df[self._from_sta_col].min()

    @property
    def sta_unit(self) -> str:
        return self._sta_unit 
    
    @sta_unit.setter
    def sta_unit(self, unit: str):
        if unit not in to_meter:
            raise ValueError(f"Unit '{unit}' is invalid or unsupported.")
        
        self._sta_unit = unit
    
    def correct_data_year(self) -> bool:
        """
        Check if all row contain the same data year with the data year from __init__ argument.
        """
        return self.pl_df.filter(
            pl.col(self._year_col) != self._data_year
        ).is_empty()
    
    def correct_data_semester(self) -> bool:
        """
        Check if all row contain the same semester with the semester from __init__ argument.
        """
        return self.pl_df.filter(
            pl.col(self._semester_col) != self._data_semester
        ).is_empty()
    
    def get_all_segments(self, lane: str = 'all') -> pl.DataFrame:
        """
        Return all segments from the selected lane. If lane selection is 'all' then return segments from all lanes.
        """
        if lane == 'all':
            return self.pl_df.select(
                [
                    self._linkid_col,
                    self._from_sta_col,
                    self._to_sta_col,
                    self._lane_code_col
                ]
            ).sort(
                [self._from_sta_col, self._lane_code_col], descending=False
            )
        else:
            return self.pl_df.filter(
                pl.col(self._lane_code_col) == lane
            ).select(
                [
                    self._linkid_col,
                    self._from_sta_col,
                    self._to_sta_col,
                    self._lane_code_col
                ]
            ).sort(
                [self._from_sta_col, self._lane_code_col], descending=False
            )
    
    def is_duplicate_segment(self) -> List[Segment]:
        """
        Return duplicate segment.
        """
        df = self.pl_df.filter(
            self.pl_df.select(
                [
                    self._linkid_col,
                    self._from_sta_col,
                    self._to_sta_col,
                    self._lane_code_col
                ]
            ).is_duplicated()
        ).unique(
            subset=[
                self._linkid_col,
                self._from_sta_col,
                self._to_sta_col,
                self._lane_code_col
            ]
        )

        return self._segment_dto_mapper(df)
    
    def incorrect_lane_sequence(self) -> List[CenterlineSegment]:
        """
        Return segment with incorrect lane sequence. Segment lane should start from L1 or R1.
        """
        df = self.pl_df.with_columns(
            lane_seq=pl.col(self._lane_code_col).str.tail(1).cast(pl.Int16)
        ).group_by(
            [
                self._linkid_col,
                self._from_sta_col,
                self._to_sta_col
            ]
        ).agg(
            pl.col('lane_seq').
            sort().
            diff().
            fill_null(1).
            gt(1).
            any(),
            pl.col(self._lane_code_col)
        ).filter(
            pl.col('lane_seq')
        )

        return self._csegment_dto_mapper(df, lanes_col=self._lane_code_col)

    def incorrect_segment_length(self, tolerance=0) -> List[Segment]:
        """
        Return segment with incorrect segment length. Exclude the last segment, because last segment could have short segment length.
        """
        df = self.pl_df.filter(
            (pl.col(self._seg_len_col).gt(self._segment_length + tolerance)) |
            (
                (pl.col(self._seg_len_col).lt(self._segment_length - tolerance)) &
                (pl.col(self._from_sta_col) != self.last_segment.from_sta) &
                (pl.col(self._to_sta_col) != self.last_segment.to_sta)
            )
        )

        return self._segment_dto_mapper(df, additional_cols=[self._seg_len_col])
    
    def incorrect_sta_diff(self, tolerance=0) -> List[Segment]:
        """
        Return segment with incorrect STA difference, compared to the stated segment length (segment length column).
        """
        tolerance = tolerance*self.seg_len_conversion

        df = self.pl_df.with_columns(
            sta_diff=(pl.col(self._to_sta_col)-pl.col(self._from_sta_col))*self.sta_conversion
        ).filter(
            (pl.col('sta_diff').lt(0)) |  # Negative, means FROM_STA is larger than TO_STA
            (pl.col('sta_diff').gt(pl.col(self._seg_len_col).mul(self.seg_len_conversion).add(tolerance))) |
            (pl.col('sta_diff').lt(pl.col(self._seg_len_col).mul(self.seg_len_conversion).sub(tolerance)))
        )

        return self._segment_dto_mapper(df, additional_cols=[self._seg_len_col])
    
    def sta_gap(self) -> List[Segment]:
        """
        Return measurement gap in each lane.
        """
        df = self.pl_df.group_by(
            [self._linkid_col, self._lane_code_col]
        ).agg(
            pl.col(self._from_sta_col),
            pl.col(self._to_sta_col)
        ).with_columns(
            pl.col(self._from_sta_col).list.sort(descending=False),
            pl.col(self._to_sta_col).list.sort(descending=False),
            shifted_from = pl.col(self._from_sta_col).list.sort(descending=False).list.shift(-1)
        ).explode(
            pl.col(self._to_sta_col),
            pl.col('shifted_from')
        ).filter(
            pl.col(self._to_sta_col) < pl.col('shifted_from')
        ).select(
            pl.col(self._linkid_col),
            pl.col(self._lane_code_col),
            pl.col(self._to_sta_col).alias(self._from_sta_col),
            pl.col('shifted_from').alias(self._to_sta_col)
        )

        return self._segment_dto_mapper(df)
    
    def overlapping_segments(self) -> List[OverlappingSegment]:
        """
        Return segments which overlap other segment
        """
        df = self.pl_df.sort(
            [
                self._linkid_col,
                self._from_sta_col,
                self._to_sta_col
            ]
        ).group_by(
            [self._linkid_col, self._lane_code_col]
        ).agg(
            pl.col(self._from_sta_col),
            pl.col(self._to_sta_col)
        ).with_columns(
            pl.col(self._from_sta_col),
            pl.col(self._to_sta_col),
            shifted_from = pl.col(self._from_sta_col).list.shift(-1),
            shifted_to = pl.col(self._to_sta_col).list.shift(-1)
        ).explode(
            pl.col(self._from_sta_col),
            pl.col(self._to_sta_col),
            pl.col('shifted_from'),
            pl.col('shifted_to')
        ).filter(
            pl.col(self._to_sta_col) > pl.col('shifted_from')
        ).select(
            pl.col(self._linkid_col),
            pl.col(self._lane_code_col),
            pl.col(self._from_sta_col),
            pl.col(self._to_sta_col),
            pl.col('shifted_from'),
            pl.col('shifted_to')
        )

        segment = self._segment_dto_mapper(
            df, 
            out_dto=OverlappingSegment
        )
        
        overlapped = self._segment_dto_mapper(
            df,
            from_sta_col='shifted_from',
            to_sta_col='shifted_to'
        )

        for i in range(len(segment)):
            segment[i].overlapped = overlapped[i]

        return segment
    
    def segment_attribute_n_unique(
            self, 
            columns: List[str], 
            as_df: bool = False,
            filter: Union[Tuple[Literal['ge', 'gt', 'le', 'lt'], int]] = None
        ) -> Union[pl.DataFrame | Type[CenterlineSegment]]:
        """
        Return the unique value count of segment attributes.
        """
        df = self.pl_df.group_by(
            [
                self._linkid_col,
                self._from_sta_col,
                self._to_sta_col
            ]
        ).agg(
            pl.col(self._lane_code_col),
            *[
                pl.col(_col).drop_nulls().n_unique() for _col in columns
            ]
        )

        if filter is not None:
            df = df.filter(
                pl.any_horizontal(
                    *[
                        getattr(pl.col(_col), filter[0])(filter[1]) 
                        for _col in columns
                    ]
                )
            )

        if as_df:
            return df
        else:
            dtos = self._csegment_dto_mapper(
                df,
                additional_cols = [
                    _col for _col in columns
                ]
            )

            return dtos

    def _segment_dto_mapper(
            self, 
            df: pl.DataFrame, 
            additional_cols: List[str] = [],
            linkid_col = None,
            from_sta_col = None,
            to_sta_col = None,
            lane_code_col = None,
            out_dto: Type[Segment] = Segment
        ) -> List[Type[Segment]]:
        """
        Map DataFrame rows into Segment DTO.
        """
        if linkid_col is None:
            linkid_col = self._linkid_col

        if from_sta_col is None:
            from_sta_col = self._from_sta_col

        if to_sta_col is None:
            to_sta_col = self._to_sta_col

        if lane_code_col is None:
            lane_code_col = self._lane_code_col

        df = df.select(
            [
                linkid_col,
                from_sta_col,
                to_sta_col,
                lane_code_col
            ] + additional_cols
        ).rename(
            dict(
                {
                    linkid_col: "route_id",
                    from_sta_col: 'from_sta',
                    to_sta_col: 'to_sta',
                    lane_code_col: 'lane'
                },
                **{
                    col: str(col).lower() for col in additional_cols
                }
            )
        )

        return TypeAdapter(List[out_dto]).validate_python(
            df.rows(named=True)
        )
    
    def _csegment_dto_mapper(
            self, 
            df: pl.DataFrame, 
            lanes_col: str = "LANE_CODE",
            additional_cols: List[str] = [],
            out_dto: Type[CenterlineSegment] = CenterlineSegment
        ) -> List[Type[CenterlineSegment]]:
        """
        Map DataFrame rows into CenterlineSegment DTO.
        """
        df = df.select(
            [
                self._linkid_col,
                self._from_sta_col,
                self._to_sta_col,
                lanes_col
            ] + additional_cols
        ).rename(
            dict(
                {
                    self._linkid_col: "route_id",
                    self._from_sta_col: "from_sta",
                    self._to_sta_col: "to_sta",
                    lanes_col: "lanes"
                },
                **{
                    col: str(col).lower() for col in additional_cols
                }
            )
        )

        return TypeAdapter(List[out_dto]).validate_python(
            df.rows(named=True)
        )
