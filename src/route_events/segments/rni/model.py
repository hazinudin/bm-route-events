from ..base import RouteSegmentEvents
from ..base.schema import RouteSegmentEventSchema
from .dto import TypeSidedColumnError, ValueSidedColumnError, CenterlineSegment
from .road_type import _road_types as road_types
from .surf_type import _surface_types as surface_types
from pydantic import TypeAdapter
from dataclasses import dataclass
from typing import Literal, List, Union, Type
import os
import polars as pl

@dataclass
class TypeSidedColumn(object):
    name: str
    empty_type: int
    side: Literal['L', 'R']


@dataclass
class ValueSidedColumn(object):
    name: str
    type_column: TypeSidedColumn
    side: Literal['L', 'R']


class RouteRNI(RouteSegmentEvents):
    """
    Route segment RNI model.
    """
    @classmethod
    def from_excel(
        cls, 
        excel_path: str, 
        linkid: str | list = 'ALL', 
        linkid_col: str = 'LINKID',
        ignore_review = False,
        data_year: int = None
    ):
        """
        Parse data from Excel file to Arrow format and load it into RNI object.
        """
        config_path = os.path.dirname(__file__) + '/schema.json'
        segment_length = 0.1

        schema = RouteSegmentEventSchema(
            config_path=config_path, 
            ignore_review_err=ignore_review
        )

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
            data_year=data_year
        )
    
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args, **kwargs
        )
        #Default columns
        self._road_type_col = 'ROAD_TYPE'
        self._surf_type_col = 'SURF_TYPE'
        self._med_width_col = 'MED_WIDTH'
        self._l_inn_shwdith_col = 'L_INN_SHWIDTH'
        self._r_inn_shwidth_col = 'R_INN_SHWIDTH'
        self._lane_width_col = 'LANE_WIDTH'
        self._surf_width_col = 'SURF_WIDTH'

        # Sided type columns
        l_inn_shtype = TypeSidedColumn(name='L_INN_SHTYPE', empty_type=0, side='L')
        l_out_shtype = TypeSidedColumn(name='L_OUT_SHTYPE', empty_type=0, side='L')
        l_ditch_type = TypeSidedColumn(name='L_DITCH_TYPE', empty_type=5, side='L')
        r_inn_shtype = TypeSidedColumn(name='R_INN_SHTYPE', empty_type=0, side='R')
        r_out_shtype = TypeSidedColumn(name='R_OUT_SHTYPE', empty_type=0, side='R')
        r_ditch_type = TypeSidedColumn(name='R_DITCH_TYPE', empty_type=5, side='R')

        # Sided value columns
        l_inn_shwidth = ValueSidedColumn(name='L_INN_SHWIDTH', type_column=l_inn_shtype, side='L')
        l_out_shwidth = ValueSidedColumn(name='L_OUT_SHWIDTH', type_column=l_out_shtype, side='L')
        l_ditch_depth = ValueSidedColumn(name='L_DITCH_DEPTH', type_column=l_ditch_type, side='L')
        l_ditch_width = ValueSidedColumn(name='L_DITCH_WIDTH', type_column=l_ditch_type, side='L')
        r_inn_shwidth = ValueSidedColumn(name='R_INN_SHWIDTH', type_column=r_inn_shtype, side='R')
        r_out_shwidth = ValueSidedColumn(name='R_OUT_SHWIDTH', type_column=r_out_shtype, side='R')
        r_ditch_depth = ValueSidedColumn(name='R_DITCH_DEPTH', type_column=r_ditch_type, side='R')
        r_ditch_width = ValueSidedColumn(name='R_DITCH_WIDTH', type_column=r_ditch_type, side='R')

        self._sided_type_columns = [
            l_inn_shtype,
            l_out_shtype,
            l_ditch_type,
            r_inn_shtype,
            r_out_shtype,
            r_ditch_type
        ]

        self._sided_value_columns = [
            l_inn_shwidth,
            l_out_shwidth,
            l_ditch_depth,
            l_ditch_width,
            r_inn_shwidth,
            r_out_shwidth,
            r_ditch_depth,
            r_ditch_width
        ]

        # Surface types mapping for types in data
        self._surf_types_map = None

    @property
    def surface_types_mapping(self) -> pl.DataFrame:
        """
        Return surface types mapping and other properties for available surfaces in this model.
        """
        if self._surf_types_map is None:
            self._surf_types_map = pl.DataFrame(surface_types).cast({
                'iri_kemantapan': pl.Array(shape=(3,), inner=pl.Int16),
                'pci_kemantapan': pl.Array(shape=(3,), inner=pl.Int16),
                'iri_rating': pl.Array(shape=(4,), inner=pl.Int16),
                'pci_rating': pl.Array(shape=(4,), inner=pl.Int16)
            }).filter(
                pl.col('surf_type').is_in(
                    self.pl_df[self._surf_type_col].unique()
                )
            )
            return self._surf_types_map
        
        else:
            return self._surf_types_map
    
    def incorrect_side_columns(self, dump=True, survey_year: int | str = 'ALL') -> List[Type[CenterlineSegment]]:
        # Filter for survey year
        if survey_year == 'ALL':
            filter_ = True  # Select all
        elif type(survey_year) is int:
            filter_ = pl.col(self._survey_date_col).dt.year().eq(survey_year)
        else:
            raise ValueError("survey_year is neither an integer or 'ALL'")
        
        # Segment group using LINKID, FROM_STA and TO_STA
        # Aggregate to acquire lanes and directions data
        segment_group = self.pl_df.filter(
            filter_
        ).group_by(
            [
                self._linkid_col,
                self._from_sta_col,
                self._to_sta_col
            ]
        ).agg(
            pl.col(self._lane_code_col).alias('lanes'),
            pl.col(self._lane_code_col).str.head(1).n_unique().alias('dir')
        )

        # Segment side group using LINKID, FROM_STA, TO_STA and side (L|R)
        # Aggregate to acquire sided columns fill, n_count and its value
        segment_side_group = self.pl_df.group_by(
            [
                self._linkid_col,
                self._from_sta_col,
                self._to_sta_col,
                pl.col(self._lane_code_col).str.head(1).alias('side')
            ]
        ).agg(
            *[
                pl.struct(
                    pl.col(col.name).is_null().all().alias('empty'),
                    pl.col(col.name).drop_nulls().n_unique().alias('n_count'),
                    pl.col(col.name).max().alias('val')
                ).alias(col.name)
                for col in self._sided_type_columns
            ] + [
                pl.struct(
                    pl.col(col.name).is_null().all().alias('empty'),
                    pl.col(col.name).drop_nulls().n_unique().alias('n_count'),
                    pl.col(col.name).max().alias('val'),
                    pl.lit(col.type_column.empty_type).alias('empty_type')
                ).alias(col.name)
                for col in self._sided_value_columns
            ]
        ).join(
            segment_group,
            on=[
                self._linkid_col,
                self._from_sta_col,
                self._to_sta_col
            ],
            how='inner'
        )

        # Lazy DataFrames from column selection
        lazy_dfs = []

        # Error query/expressions
        def na_expr(col: Union[TypeSidedColumn, ValueSidedColumn]):
            expr = (
                (
                    pl.col('dir').eq(2) &
                    (pl.col('side') == col.side) &
                    pl.col(col.name).struct.field('empty')
                ) |
                (
                    pl.col('dir').eq(1) &
                    pl.col(col.name).struct.field('empty')
                )
            )

            return expr

        def wrong_side_expr(col: Union[TypeSidedColumn, ValueSidedColumn]):
            expr = (
                (pl.col('dir').eq(2)) & 
                (pl.col('side') != col.side) & 
                (pl.col(col.name).struct.field('empty').not_())
            )

            return expr
        
        def single_value_expr(col: Union[TypeSidedColumn, ValueSidedColumn]):
            expr = (
                pl.col(col.name).struct.field('n_count').le(1)
            )

            return expr
        
        def wrong_value_type(val_col: ValueSidedColumn):
            expr = (
                (
                    pl.col(val_col.type_column.name).struct.field('val').eq(
                        val_col.type_column.empty_type
                    ) &
                    pl.col(val_col.name).struct.field('val').ne(0)
                    &
                    pl.col(val_col.type_column.name).struct.field('empty').not_()
                    &
                    pl.col(val_col.name).struct.field('empty').not_()
                ) | 
                (
                    pl.col(val_col.type_column.name).struct.field('val').ne(
                        val_col.type_column.empty_type
                    ) &
                    pl.col(val_col.name).struct.field('val').eq(0)
                    &
                    pl.col(val_col.type_column.name).struct.field('empty').not_()
                    &
                    pl.col(val_col.name).struct.field('empty').not_()
                )
            )

            return expr

        # Select error rows
        for col in self._sided_type_columns + self._sided_value_columns:
            if type(col) == TypeSidedColumn:
                ldf = segment_side_group.lazy().with_columns(
                    col=pl.lit(col.name),  # The column name itself
                    na=na_expr(col),  # The na or empty query
                    wrong_side=wrong_side_expr(col),  # Incorrect side or wrong side
                    single_value=single_value_expr(col)  # Incorrect type
                ).filter(
                    pl.col('na') |
                    pl.col('wrong_side') |
                    pl.col('single_value').not_()
                ).select(
                    [
                        self._linkid_col,
                        self._from_sta_col,
                        self._to_sta_col,
                        'col',
                        'side',
                        'dir',
                        'lanes',
                        'na',
                        'wrong_side',
                        'single_value'
                    ]
                )
            
            elif type(col) == ValueSidedColumn:
                ldf = segment_side_group.lazy().with_columns(
                    col=pl.lit(col.name),
                    na=na_expr(col),
                    wrong_side=wrong_side_expr(col),
                    single_value=single_value_expr(col),
                    wrong_value_type=wrong_value_type(col),
                    type_column=pl.lit(col.type_column.name)
                ).filter(
                    pl.col('na') |
                    pl.col('wrong_side') |
                    pl.col('single_value').not_() |
                    pl.col('wrong_value_type')
                ).select(
                    [
                        self._linkid_col,
                        self._from_sta_col,
                        self._to_sta_col,
                        'col',
                        'type_column',
                        'side',
                        'dir',
                        'lanes',
                        'na',
                        'wrong_side',
                        'single_value',
                        'wrong_value_type'
                    ]
                )

            lazy_dfs.append(ldf)    

        error_rows = pl.concat(
            lazy_dfs, 
            how='diagonal',
            parallel=True
        ).collect().rename(
            {'col': 'column'}
        )

        value_sided_error_dto = self._csegment_dto_mapper(
            error_rows.filter(pl.col('type_column').is_not_null()),
            out_dto = ValueSidedColumnError,
            lanes_col = 'lanes',
            additional_cols = [
                'dir',
                'side',
                'column',
                'na',
                'wrong_side',
                'single_value',
                'type_column',
                'wrong_value_type'
            ],
            dump=dump
        )

        type_sided_error_dto = self._csegment_dto_mapper(
            error_rows.filter(pl.col('type_column').is_null()),
            out_dto = TypeSidedColumnError,
            lanes_col = 'lanes',
            additional_cols = [
                'dir',
                'side',
                'column',
                'na',
                'wrong_side',
                'single_value'
            ],
            dump=dump
        )

        return value_sided_error_dto + type_sided_error_dto
    
    def incorrect_road_type_spec(self, dump=True) -> List[Type[CenterlineSegment]]:
        spec_df = pl.DataFrame(road_types)

        error_rows = self.pl_df.group_by(
            [
                self._linkid_col,
                self._from_sta_col,
                self._to_sta_col
            ]
        ).agg(
            pl.col(self._lane_code_col),
            road_type = pl.col(self._road_type_col).max(),
            lane_count = pl.col(self._lane_code_col).len(),
            dir = pl.col(self._lane_code_col).str.head(1).n_unique(),
            median = pl.col(self._med_width_col).sum()
        ).join(
            spec_df,
            on = 'road_type'
        ).filter(
            (pl.col('lane_count').ne(pl.col('lane_count_right'))) |
            (pl.col('dir').ne(pl.col('dir_right'))) |
            (pl.col('median').eq(0).and_(pl.col('median_right'))) |
            (pl.col('median').ne(0).and_(pl.col('median_right').not_()))
        ).select(
            [
                self._linkid_col,
                self._from_sta_col,
                self._to_sta_col,
                self._lane_code_col,
                'road_type',
                'lane_count',
                'dir',
                'median'
            ]
        )

        dtos = self._csegment_dto_mapper(
            error_rows,
            additional_cols=[
                'road_type',
                'lane_count',
                'dir',
                'median'
            ],
            dump=dump
        )

        return dtos
    
    def incorrect_inner_shoulder(self, dump=True, survey_year: int | str = 'ALL') -> List[Type[CenterlineSegment]]:
        # Filter for survey year
        if survey_year == 'ALL':
            filter_ = True  # Select all
        elif type(survey_year) is int:
            filter_ = pl.col(self._survey_date_col).dt.year().eq(survey_year)
        else:
            raise ValueError("survey_year is neither an integer or 'ALL'")
        
        error_rows = self.pl_df.filter(
            filter_
        ).group_by(
            [
                self._linkid_col,
                self._from_sta_col,
                self._to_sta_col
            ]
        ).agg(
            pl.col(self._lane_code_col),
            has_median = (
                (pl.col(self._med_width_col).max() > 0) & 
                (pl.col(self._med_width_col).is_null().all().not_())
            ),
            has_inner_sh =  (
                (
                    (pl.col(self._l_inn_shwdith_col).drop_nulls().max() > 0) &
                    (pl.col(self._l_inn_shwdith_col).is_not_null().any())
                ) |
                (
                    (pl.col(self._r_inn_shwidth_col).drop_nulls().max() > 0) &
                    (pl.col(self._r_inn_shwidth_col).is_not_null().any())
                )
            )
        ).filter(
            pl.col('has_median').and_(pl.col('has_inner_sh').not_()) |
            pl.col('has_median').not_().and_(pl.col('has_inner_sh'))
        )

        dtos = self._csegment_dto_mapper(
            error_rows,
            additional_cols=[
                'has_median',
                'has_inner_sh'
            ],
            dump=dump
        )

        return dtos
    
    def incorrect_surface_width(self, width_delta: int = 2, dump=True) -> List[Type[CenterlineSegment]]:
        error_rows = self.pl_df.group_by(
            [
                self._linkid_col,
                self._from_sta_col,
                self._to_sta_col
            ]
        ).agg(
            pl.col(self._lane_code_col),

            l_lane_width_sum = pl.when(
                pl.col(self._lane_code_col).str.starts_with('L')
            ).then(
                pl.col(self._lane_width_col)
            ).sum(),

            r_lane_width_sum = pl.when(
                pl.col(self._lane_code_col).str.starts_with('R')
            ).then(
                pl.col(self._lane_width_col)
            ).sum(),

            l_surf_width = pl.when(
                pl.col(self._lane_code_col).str.starts_with('L')
            ).then(
                pl.col(self._surf_width_col)
            ).max(),

            r_surf_width = pl.when(
                pl.col(self._lane_code_col).str.starts_with('R')
            ).then(
                pl.col(self._surf_width_col)
            ).max(),

            lane_width_sum = pl.col(self._lane_width_col).sum(),
            surface_width = pl.col(self._surf_width_col).max(),
            has_median = (
                (pl.col(self._med_width_col).max() > 0) & 
                (pl.col(self._med_width_col).is_null().all().not_())
            )
        ).filter(
            pl.col('has_median').not_().and_(
                (
                    pl.col('lane_width_sum').add(width_delta).lt(pl.col('surface_width'))
                ) |
                (
                    pl.col('lane_width_sum').gt(pl.col('surface_width'))
                )
            ) |
            pl.col('has_median').and_(
                (
                    pl.col('r_lane_width_sum').add(width_delta).lt(pl.col('r_surf_width'))
                ) |
                (
                    pl.col('r_lane_width_sum').gt(pl.col('r_surf_width'))
                ) |
                (
                    pl.col('l_lane_width_sum').add(width_delta).lt(pl.col('l_surf_width'))
                ) |
                (
                    pl.col('l_lane_width_sum').gt(pl.col('l_surf_width'))
                )
            )
        )

        dtos = self._csegment_dto_mapper(
            error_rows,
            additional_cols= [
                'has_median',
                'l_lane_width_sum',
                'l_surf_width',
                'r_lane_width_sum',
                'r_surf_width',
                'lane_width_sum',
                'surface_width'
            ],
            dump=dump
        )

        return dtos