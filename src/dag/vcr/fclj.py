from .pipeline import PipelineStep, PipelineContext
import polars as pl
from .expr import CapacityExpressions
from copy import deepcopy


class CapacityFCLJLookup(PipelineStep):
    def __init__(self):
        super().__init__(step_name='fclj_lookup')

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        """
        Join RNI data with FLCJ lane width(min or sum) and its lane width correction factor.
        """
        expr = CapacityExpressions(ctx)
        
        # Create coefficient column in LazyFrame depends on road stat and median/one way
        lf = ctx.lf.with_columns(
            FCLJ_STRUCT=pl.when(
                # Condition:
                # Road status = K 
                # Single direction OR two way and divided
                expr.k_stat().and_(
                    expr.one_way().or_(
                        expr.two_way().and_(
                            expr.divided()
                        )
                    )
                )
            ).then(
                pl.lit(
                    {
                        "le_agg": "min",
                        "le": [3.0, 3.25, 3.5, 3.75, 4.0],
                        "lj": [0.92, 0.96, 1.0, 1.04, 1.08]
                    },
                    allow_object=True
                )
            ).when(
                # Condition
                # Road status = K
                # Without median
                # Two direction
                # Total lane count <= 3
                expr.k_stat().and_(
                    expr.undivided()
                ).and_(
                    expr.two_way()
                ).and_(
                    expr.lane_count(3, 'le')
                )
            ).then(
                pl.lit(
                    {   
                        "le_agg": "sum",
                        "le": [5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0],
                        "lj": [0.56, 0.87, 1.0, 1.14, 1.25, 1.29, 1.34]
                    },
                    allow_object=True
                )
            ).when(
                # Condition:
                # Road status = K
                # Without median
                # Two direction
                # Total lane count >= 4
                expr.k_stat().and_(
                    expr.undivided()
                ).and_(
                    expr.two_way()
                ).and_(
                    expr.lane_count(4, 'ge')
                )
            ).then(
                pl.lit(
                    {
                        "le_agg": "mean",
                        "le": [3.0, 3.25, 3.5, 3.75, 4.0],
                        "lj": [0.91, 0.95, 1.0, 1.05, 1.09]
                    },
                    allow_object=True
                )
            ).when(
                # Condition:
                # Road status = LK
                # With median
                # Two direction
                # OR single direction
                expr.lk_stat().and_(
                    expr.one_way().or_(
                        expr.divided().and_(
                            expr.two_way()
                        )
                    )
                )
            ).then(
                pl.lit(
                    {
                        "le_agg": "min",
                        "le": [3.0, 3.25, 3.5, 3.75],
                        "lj": [0.91, 0.96, 1.0, 1.03]
                    },
                    allow_object=True
                )
            ).when(
                # Condition:
                # Road status = LK
                # Without median
                # Two direction
                # Total lane count <= 3
                expr.lk_stat().and_(
                    expr.undivided()
                ).and_(
                    expr.two_way()
                ).and_(
                    expr.lane_count(3, 'le')
                )
            ).then(
                pl.lit(
                    {
                        "le_agg": "sum",
                        "le": [5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0],
                        "lj": [0.69, 0.91, 1.0, 1.08, 1.15, 1.21, 1.27]
                    },
                    allow_object=True
                )
            ).when(
                # Condition:
                # Road status = LK
                # 4 or more lanes, 2 dir, no median
                expr.lk_stat().and_(
                    expr.lane_count(4, 'ge').and_(
                        expr.two_way()
                    ).and_(
                        expr.undivided()
                    )
                )
            ).then(
                pl.lit(
                    {
                        "le_agg": "mean",
                        "le": [3.0, 3.25, 3.5, 3.75],
                        "lj": [0.91, 0.96, 1.0, 1.03]
                    },
                    allow_object=True
                )
            )
        )

        out_ctx = PipelineContext()
        out_ctx.lf = lf

        return out_ctx


class CapacityFCLJInterpolation(PipelineStep):
    def __init__(self):
        super().__init__(step_name='fclj_interpolation')

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        """
        Interpolate FCLJ value based on the lane width.
        """
        if 'FCLJ_STRUCT' not in ctx.lf.collect_schema().names():
            raise ValueError("'FCLJ_STRUCT' not in the context LazyFrame columns.")
        
        def lj_lanewidth_check(lw_col: str) -> pl.Expr:
            """
            Check the lane width value if it is exceed the 'le' value range.
            """
            return pl.when(
                pl.col(lw_col).ge(pl.col('le').list.max())
            ).then(
                pl.col('lj').list.concat(pl.col('lj').list.max())
            ).when(
                pl.col(lw_col).le(pl.col('le').list.min())
            ).then(
                pl.col('lj').list.concat(pl.col('lj').list.min())
            ).otherwise(
                pl.col('lj').list.concat(None) # Append null value that will be interpolated
            )

        lf = ctx.lf.unnest(
            'FCLJ_STRUCT'
        ).with_columns(
            lj=pl.when(
                pl.col('le_agg').eq('mean')
            ).then(
                lj_lanewidth_check(ctx.avg_lanew_col)
            ).when(
                pl.col('le_agg').eq('sum')
            ).then(
                lj_lanewidth_check(ctx.total_lanew_col)
            ).when(
                pl.col('le_agg').eq('min')
            ).then(
                lj_lanewidth_check(ctx.min_lanew_col)
            ),
            
            # Check for the 'le_agg' method
            le=pl.when(
                pl.col('le_agg').eq('mean')
            ).then(
                pl.col('le').list.concat(pl.col(ctx.avg_lanew_col))
            ).when(
                pl.col('le_agg').eq('sum')
            ).then(
                pl.col('le').list.concat(pl.col(ctx.total_lanew_col))
            ).when(
                pl.col('le_agg').eq('min')
            ).then(
                pl.col('le').list.concat(pl.col(ctx.min_lanew_col))
            )
        ).explode(
            'lj', 'le'
        ).group_by(
            [
                ctx.linkid_col,
                ctx.from_sta_col,
                ctx.to_sta_col
            ]
        ).agg(
            pl.col('lj').interpolate_by('le').last().alias('FCLJ')
        )

        out_ctx = deepcopy(ctx)
        out_ctx.lf = lf

        return out_ctx