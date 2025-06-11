from .pipeline import PipelineContext, PipelineStep
import polars as pl
from .expr import CapacityExpressions


class CapacityFCUK(PipelineStep):
    """
    Capacity FCUK calculation.
    """
    def __init__(self):
        super().__init__(step_name='FCUK_calculation')

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        if 'TOTAL_POP' not in ctx.lf.collect_schema().names():
            raise ValueError("LazyFrame does not have 'TOTAL_POP' columns")
        
        if 'ROAD_STAT' not in ctx.lf.collect_schema().names():
            raise ValueError("LazyFrame does not have 'ROAD_STAT' columns.")
        
        expr = CapacityExpressions(ctx)

        lf = ctx.lf.with_columns(
            FCUK=pl.when(
                # Table 4-7
                # Just for K road
                expr.k_stat()
            ).then(
                pl.when(
                    # Less than 100K
                    pl.col(ctx.total_pop_col).lt(100000)
                ).then(
                    pl.lit(0.86)
                ).when(
                    # Between 100K to 500K
                    pl.col(ctx.total_pop_col).is_between(100000, 500000)
                ).then(
                    pl.lit(0.9)
                ).when(
                    # Between 500K to 1M
                    pl.col(ctx.total_pop_col).is_between(500000, 1000000)
                ).then(
                    pl.lit(0.94)
                ).when(
                    # Between 1M-3M
                    pl.col(ctx.total_pop_col).is_between(1000000, 3000000)
                ).then(
                    pl.lit(1.0)
                ).when(
                    # Greater than 3M
                    pl.col(ctx.total_pop_col).gt(3000000)
                ).then(
                    pl.lit(1.04)
                )
            ).otherwise(
                pl.lit(1.0)
            )
        ).select(
            ctx.linkid_col,
            ctx.from_sta_col,
            ctx.to_sta_col,
            'FCUK'
        )

        out_ctx = PipelineContext()
        out_ctx.lf = lf

        return out_ctx
        