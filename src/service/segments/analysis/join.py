from route_events import RouteSegmentEvents, RouteRNI
from typing import Type, Dict, List, Literal
import polars as pl
import duckdb


def segments_coverage_join(
        covering: Type[RouteSegmentEvents],
        target: Type[RouteSegmentEvents],
        covering_select: list = [],
        target_select: list = [],
        covering_agg: List[pl.Expr] = None,
        target_agg: List[pl.Expr] = None,
        suffix: str = '_r'
) -> pl.DataFrame:
    """
    Perform DataFrame join between RouteSegmentEvents type, using STA from 'covering' events.
    Covering becomes the 'left' and the target become the 'right'.
    """        
    ddb = duckdb.connect()

    def _segment_id_col(obj: Type[RouteSegmentEvents], convert_to_m=False):
        if convert_to_m:
            selection = [
                pl.col(obj._linkid_col),
                pl.col(obj._from_sta_col).mul(obj.sta_conversion).cast(pl.Int32),
                pl.col(obj._to_sta_col).mul(obj.sta_conversion).cast(pl.Int32),
            ]
        else:
            selection = [
                pl.col(obj._linkid_col),
                pl.col(obj._from_sta_col),
                pl.col(obj._to_sta_col),
            ]
        
        if obj.lane_data:
            selection.append(pl.col(obj._lane_code_col))
            return selection
        else:
            return selection
    
    def _csegment_id_col(obj: Type[RouteSegmentEvents]):
        return [
            obj._linkid_col,
            obj._from_sta_col,
            obj._to_sta_col
        ]
    
    # Initial selection
    ldf = covering.pl_df.select(
        _segment_id_col(covering, convert_to_m=True) + covering_select
    )

    rdf = target.pl_df.select(
        _segment_id_col(target, convert_to_m=True) + target_select
    )

    # Check if aggregation function is supplied
    if covering_agg is not None:
        ldf = ldf.group_by(
            _csegment_id_col(covering)
        ).agg(
            *covering_agg
        )
    
    if target_agg is not None:
        rdf = rdf.group_by(
            _csegment_id_col(target)
        ).agg(
            *target_agg
        )

    # STA Query
    sta_query = f"""
    (
    r.{target._from_sta_col} <= l.{covering._from_sta_col} and
    r.{target._to_sta_col} > l.{covering._from_sta_col}
    ) or
    (
    r.{target._from_sta_col} >= l.{covering._from_sta_col} and
    r.{target._to_sta_col} <= l.{covering._to_sta_col}
    ) or
    (
    r.{target._from_sta_col} < l.{covering._to_sta_col} and
    r.{target._to_sta_col} >= l.{covering._to_sta_col}
    )
    """

    # Group by function
    if (
        covering._lane_code_col not in ldf.columns
    ) or (
        target._lane_code_col not in rdf.columns
    ):
        right_columns = []

        for col in rdf.columns:
            if col in ldf.columns:
                right_columns.append(f"r.{col} as {col}{suffix}")
            else:
                right_columns.append(col)

        join_query = f"""
        select {', '.join(['l.'+_ for _ in ldf.columns])}, {', '.join(right_columns)}
        from ldf as l
        left join rdf as r
        on 
        l.{covering._linkid_col} = r.{target._linkid_col} and
        ({sta_query})
        """
    
    # Not a group by function
    else:
        right_columns = []

        for col in rdf.columns:
            if col in ldf.columns:
                right_columns.append(f"r.{col} as {col}{suffix}")
            else:
                right_columns.append(col)

        join_query = f"""
        select {', '.join(['l.'+_ for _ in ldf.columns])}, {', '.join(right_columns)}
        from ldf as l
        left join rdf as r
        on 
        l.{covering._linkid_col} = r.{target._linkid_col} and
        r.{target._lane_code_col} = l.{covering._lane_code_col} and
        ({sta_query})
        """

    joined = ddb.sql(join_query)

    return joined.pl()

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


class CompareRNISegments:
    """
    Object for comparing Route Segment Events against RNI
    """
    def __init__(
            self,
            rni: RouteRNI,
            other: Type[RouteSegmentEvents]
    ):
        if not isinstance(rni, RouteRNI):
            raise TypeError(f"'rni' is not an RouteRNI object")
        
        if not isinstance(other, RouteSegmentEvents):
            raise TypeError(f"'other' is not an RouteSegmentEvents")
        
        self.rni = rni
        self.other = other

    def rni_with_no_match(self) -> pl.DataFrame:
        """
        Segments from RNI with no match with other event.
        """
        rni_anti = segments_join(
            self.rni,
            self.other,
            how='anti'
        )

        return rni_anti
    
    def other_with_no_match(self) -> pl.DataFrame:
        """
        Segment from other event with no match with RNI segment
        """
        other_anti = segments_join(
            self.other,
            self.rni,
            how='anti'
        )

        return other_anti
