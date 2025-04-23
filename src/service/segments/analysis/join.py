from route_events import RouteSegmentEvents
from typing import Type, Dict, List
import polars as pl


def segments_join(
        left: Type[RouteSegmentEvents],
        right: Type[RouteSegmentEvents],
        l_select: list = [],
        r_select: list = [],
        how: str = 'inner',
        l_agg: List[pl.Expr] = None,
        r_agg: List[pl.Expr] = None,
        suffix: str = '_r'
) -> pl.DataFrame:
    """
    Perform DataFrame join between RouteSegmentEvents type.
    """
    if (type(l_select) != list) or (type(r_select) != list):
        raise TypeError("Only accepts list for columns selection.")
    
    def _segment_id_col(obj: Type[RouteSegmentEvents], convert_to_m=False):
        if convert_to_m:
            return [
                pl.col(obj._linkid_col),
                pl.col(obj._from_sta_col).mul(obj.sta_conversion).cast(pl.Int32),
                pl.col(obj._to_sta_col).mul(obj.sta_conversion).cast(pl.Int32),
                pl.col(obj._lane_code_col)
            ]
        else:
            return [
                pl.col(obj._linkid_col),
                pl.col(obj._from_sta_col),
                pl.col(obj._to_sta_col),
                pl.col(obj._lane_code_col)
            ]
    
    def _csegment_id_col(obj: Type[RouteSegmentEvents]):
        return [
            obj._linkid_col,
            obj._from_sta_col,
            obj._to_sta_col
        ]
    
    # Initial selection
    ldf = left.pl_df.select(
        _segment_id_col(left, convert_to_m=True) + l_select
    )

    rdf = right.pl_df.select(
        _segment_id_col(right, convert_to_m=True) + r_select
    )

    # Check if aggregation function is supplied
    if l_agg is not None:
        ldf = ldf.group_by(
            _csegment_id_col(left)
        ).agg(
            *l_agg
        )

    if r_agg is not None:
        rdf = rdf.group_by(
            _csegment_id_col(right)
        ).agg(
            *r_agg
        )

    if (r_agg is not None) or (l_agg is not None):
        joined = ldf.join(
            rdf,
            left_on=_csegment_id_col(left),
            right_on=_csegment_id_col(right),
            how=how,
            suffix=suffix
        )
    else:
        joined = ldf.join(
            rdf,
            left_on=_segment_id_col(left),
            right_on=_segment_id_col(right),
            how=how,
            suffix=suffix
        )
    
    return joined