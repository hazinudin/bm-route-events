from route_events import RouteSegmentEvents, RoutePointEvents
from typing import Type, List, Literal
import polars as pl


def segments_points_join(
        segments: Type[RouteSegmentEvents],
        points: Type[RoutePointEvents],
        segment_select: list = [],
        point_select: list = [],
        how: str = ['innner', 'anti'],
        segments_agg: List[pl.Expr] = None,
        points_agg: List[pl.Expr] = None,
        suffix: str = '_r'
):
    """
    Perform DataFrame join between RouteSegmentEvents and RoutePointEvents.
    Left is the Points and right is the Segments.
    """
    if not isinstance(segments, RouteSegmentEvents):
        raise TypeError("Only accepts RouteSegmentEvents for 'segments'")
    
    if not isinstance(points, RoutePointEvents):
        raise TypeError("Only accepts RoutePointEvents for 'points'")
    
    if how not in ['inner', 'anti']:
        raise ValueError("Only supports 'inner' or 'anti' join.")
    
    segment_id_col = [
        pl.col(segments._linkid_col),
        pl.col(segments._from_sta_col).mul(segments.sta_conversion).cast(pl.Int32),
        pl.col(segments._to_sta_col).mul(segments.sta_conversion).cast(pl.Int32),
        pl.col(segments._lane_code_col)
    ]

    csegment_id_col = [
        pl.col(segments._linkid_col),
        pl.col(segments._from_sta_col),
        pl.col(segments._to_sta_col)
    ]

    if points.lane_data:
        point_id_col = [
            pl.col(points._linkid_col),
            pl.col(points._sta_col).mul(points.sta_conversion),
            pl.col(points._lane_code_col)
        ]
    else:
        point_id_col = [
            pl.col(points._linkid_col),
            pl.col(points._sta_col).mul(points.sta_conversion)
        ]
    
    # Segment DataFrame
    sdf = segments.pl_df.select(
        segment_id_col + segment_select
    )

    # Point DataFrame
    pdf = points.pl_df.select(
        point_id_col + point_select
    )

    # Aggregation for segment data
    if segments_agg is not None:
        sdf = sdf.group_by(
            csegment_id_col
        ).agg(
            *segments_agg
        )
    
    # Aggregation for points data
    if points.lane_data and (points_agg is not None):
        pdf = pdf.group_by(
            [
                points._linkid_col,
                points._sta_col
            ]
        ).agg(
            *points_agg
        )

    # Start data join
    ctx = pl.SQLContext(register_globals=True)

    # SQL additional selection columns
    r_cols = []
    for col in segment_select:
        if col in point_select:
            r_cols.append('s.'+col+' as '+col+suffix)

    l_cols = ['p.'+col for col in point_select]

    if (points.lane_data and (points_agg is not None)) and (segments_agg is not None):
        # Join only using STA and FROM/TO_STA
        select = f"""
        select {points._linkid_col}, {points._sta_col}, {segments._from_sta_col}, {segments._to_sta_col}
        """
        
        if len(l_cols) > 0:
            select = select + f"{', '.join(l_cols)}"

        if len(r_cols) > 0:
            select = select + f", {', '.join(r_cols)}"

        query = select + f""""
        from
        pdf p
        left join sdf s 
        on
        p.{points._linkid_col} = s.{segments._linkid_col}
        """

    else:
        # Joint using STA + LANE_CODE and FROM/TO_STA + LANE_CODE
        select = f"""
        select {points._linkid_col}, {points._sta_col}, {points._lane_code_col}, {segments._from_sta_col}, {segments._to_sta_col},
        """

        if len(l_cols) > 0:
            select = select + f"{', '.join(l_cols)}"

        if len(r_cols) > 0:
            select = select + f", {', '.join(r_cols)}"

        query = select + f"""
        from
        pdf p
        left join sdf s 
        on
        p.{points._linkid_col} = s.{segments._linkid_col} and
        p.{points._lane_code_col} = s.{segments._lane_code_col}
        """
    
    if how == 'inner':
        return ctx.execute(query).filter(
            pl.col(points._sta_col).le(pl.col(segments._to_sta_col)) &
            pl.col(points._sta_col).ge(pl.col(segments._from_sta_col))
        ).collect()

    elif how == 'anti':
        if (points.lane_data and (points_agg is not None)) and (segments_agg is not None):
            return pdf.lazy().join(
                ctx.execute(query).filter(
                    pl.col(points._sta_col).le(pl.col(segments._to_sta_col)) &
                    pl.col(points._sta_col).ge(pl.col(segments._from_sta_col))
                ),
                how = 'anti',
                on = [points._linkid_col, points._sta_col]
            ).collect()
        else:
            return pdf.lazy().join(
                ctx.execute(query).filter(
                    pl.col(points._sta_col).le(pl.col(segments._to_sta_col)) &
                    pl.col(points._sta_col).ge(pl.col(segments._from_sta_col))
                ),
                how = 'anti',
                on = [points._linkid_col, points._sta_col, points._lane_code_col]
            ).collect()
            