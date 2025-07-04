from .pipeline import PipelineContext, PipelineStep, MultiDataContext
from .expr import CapacityExpressions
import polars as pl
from copy import deepcopy


class CapacityFCPALookup(PipelineStep):
    """
    Capacity FCPA lookup step.
    """
    def __init__(self):
        super().__init__(step_name='fcpa_lookup')

    def execute(self, ctx: MultiDataContext) -> PipelineContext:
        expr = CapacityExpressions(ctx)

        # N volume ratio to total volume
        nratio_lf = ctx.datas['VOLH'].with_columns(
            TOTAL_VOL=pl.sum_horizontal(
                [ctx.veh1_col, ctx.veh2_col, ctx.veh3_col, ctx.veh4_col, ctx.veh5_col]
            )
        ).group_by(
            ctx.linkid_col
        ).agg(
            pl.col('TOTAL_VOL').filter(
                pl.col(ctx.dir_col).eq('N')
            ).sum().truediv(
                pl.col('TOTAL_VOL').sum()
            ).alias(
                'N_RATIO'
            )
        ).select(
            pl.col(ctx.linkid_col),
            N_RATIO=pl.when(
                pl.col('N_RATIO').lt(0.5)
            ).then(
                pl.col('N_RATIO').mul(-1).add(1)
            ).when(
                pl.col('N_RATIO').is_nan()
            ).then(
                pl.lit(0)
            ).otherwise(
                pl.col('N_RATIO')
            ).truediv(5).round(2).mul(500).cast(pl.Int32)
        )

        # Join RNI and N_RATIO data
        lf = ctx.datas['RNI'].join(
            nratio_lf,
            on=ctx.linkid_col
        ).with_columns(
            FCPA=pl.when(
                expr.two_way().and_(
                    expr.undivided()
                )
            ).then(
                pl.when(
                # Table 4-4 page 107 (For K) but same with LK
                    pl.col('N_RATIO').le(50)
                ).then(
                    pl.lit(1.0)
                ).when(
                    pl.col('N_RATIO').eq(55)
                ).then(
                    pl.lit(0.97)
                ).when(
                    pl.col('N_RATIO').eq(60)
                ).then(
                    pl.lit(0.94)
                ).when(
                    pl.col('N_RATIO').eq(65)
                ).then(
                    pl.lit(0.91)
                ).when(
                    pl.col('N_RATIO').ge(70)
                ).then(
                    pl.lit(0.88)
                )
            ).otherwise(
                pl.lit(1)
            )
        ).select(
            [
                ctx.linkid_col,
                ctx.from_sta_col,
                ctx.to_sta_col,
                'FCPA'
            ]
        )

        out_ctx = deepcopy(ctx)
        out_ctx.lf = lf

        return out_ctx