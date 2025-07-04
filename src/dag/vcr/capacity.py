import polars as pl
from .pipeline import (
    PipelineStep,
    MultiDataContext,
    PipelineContext
)


class FinalCapacityCalculation(PipelineStep):
    def __init__(self):
        super().__init__(step_name='calculate_capacity')

    def execute(self, ctx: MultiDataContext) -> PipelineContext:
        """
        Calculate final segment capacity
        """
        if (
            'C0' not in ctx.datas.keys()
        ) or (
            'FCHS' not in ctx.datas.keys()
        ) or (
            'FCLJ' not in ctx.datas.keys()
        ) or (
            'FCPA' not in ctx.datas.keys()
        ) or (
            'FCUK' not in ctx.datas.keys()
        ) or (
            'RNI' not in ctx.datas.keys()
        ):
            raise KeyError('Context does not contain RNI, C0, FCHS, FCLJ or FCPA dataset.')
        
        lf = ctx.datas['C0'].join(
            ctx.datas['FCLJ'],
            on=ctx.join_key['FCLJ']
        ).join(
            ctx.datas['FCHS'],
            on=ctx.join_key['FCHS']
        ).join(
            ctx.datas['FCPA'],
            on=ctx.join_key['FCPA'],
        ).join(
            ctx.datas['FCUK'],
            on=ctx.join_key['FCUK']
        ).join(
            ctx.datas['RNI'].group_by(
                ctx.linkid_col
            ).agg(
                pl.col(ctx.rni_year_col).max()
            ),
            on=ctx.join_key['RNI']
        ).with_columns(
            pl.col('C0').mul(
                pl.col('FCLJ')
            ).mul(
                pl.col('FCHS')
            ).mul(
                pl.col('FCPA')
            ).mul(
                pl.col('FCUK')
            ).alias('CAPACITY')
        )

        out_ctx = PipelineContext()
        out_ctx.lf = lf

        return out_ctx

