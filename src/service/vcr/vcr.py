import polars as pl
from .pipeline import PipelineStep, MultiDataContext, PipelineContext
import os


class CalculateVCR(PipelineStep):
    def __init__(self, year:int):
        super().__init__(step_name='calculate_VCR')
        self.year = year

    def execute(self, ctx: MultiDataContext) -> PipelineContext:
        """
        Calculate VCR using Capacity and PCE data.
        """
        if (
            'CAPACITY' not in MultiDataContext.datas.keys()
        ) or (
            'PCE' not in MultiDataContext.datas.keys()
        ):
            raise KeyError('Context does not contain CAPACITY or PCE dataset')
        
        lf = ctx.datas['CAPACITY'].join(
            ctx.datas['PCE'],
            on=ctx.join_key['PCE']
        ).with_columns(
            pl.col('TOTAL_PCE').truediv(
                pl.col('CAPACITY')
            ).alias('VCR'),
            pl.lit(self.year).alias('YEAR')
        )

        out_ctx = PipelineContext()
        out_ctx.lf = lf

        return out_ctx
    

class CalculateVCRSummary(PipelineStep):
    def __init__(self):
        super().__init__('VCR_summary')

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        """
        Calculate VCR summary.
        """
        sk_lf = pl.scan_parquet(
            f'{os.path.dirname(__file__)}/data/route_sk_length.parquet'
        )

        lf = ctx.lf.group_by(
            ['LINKID', 'FROM_STA', 'TO_STA']
        ).agg(
            pl.col('VCR').max(),
            pl.col('YEAR').max()
        ).group_by(
            'LINKID'
        ).agg(
            pl.col('YEAR').max(),
            pl.col('VCR').mean().alias('AVG_VCR'), 
            pl.when(
                pl.col('VCR').lt(0.25)
            ).then(
                pl.col('TO_STA')-pl.col('FROM_STA')
            ).sum().alias('VCR_<_0.25'), 
            pl.when(
                pl.col('VCR').is_between(0.25, 0.5, 'left')
            ).then(
                pl.col('TO_STA')-pl.col('FROM_STA')
            ).sum().alias('VCR_0.25_0.5'), 
            pl.when(
                pl.col('VCR').is_between(0.5, 0.85, 'left')
            ).then(
                pl.col('TO_STA')-pl.col('FROM_STA')
            ).sum().alias('VCR_0.5_0.85'), 
            pl.when(
                pl.col('VCR').is_between(0.85, 1, 'left')
            ).then(
                pl.col('TO_STA')-pl.col('FROM_STA')
            ).sum().alias('VCR_0.85_1'), 
            pl.when(
                pl.col('VCR').ge(1)
            ).then(
                pl.col('TO_STA')-pl.col('FROM_STA')
            ).sum().alias('VCR_>=_1')
        ).with_columns(
            pl.col(pl.Int64).cast(pl.Float64).truediv(100), 
            pl.sum_horizontal(pl.col(pl.Int64)).alias('TOTAL_LEN').cast(pl.Float64).truediv(100)
        ).select(
            pl.all(),
            pl.col('VCR_<_0.25').truediv(pl.col('TOTAL_LEN')).mul(100).name.suffix('_PERCENT'),
            pl.col('VCR_0.25_0.5').truediv(pl.col('TOTAL_LEN')).mul(100).name.suffix('_PERCENT'),
            pl.col('VCR_0.5_0.85').truediv(pl.col('TOTAL_LEN')).mul(100).name.suffix('_PERCENT'),
            pl.col('VCR_0.85_1').truediv(pl.col('TOTAL_LEN')).mul(100).name.suffix('_PERCENT'),
            pl.col('VCR_>=_1').truediv(pl.col('TOTAL_LEN')).mul(100).name.suffix('_PERCENT')
        ).join(
            sk_lf,
            on=ctx.linkid_col
        ).with_columns(
            # Calculate SK Length projection.
            pl.col('VCR_<_0.25').mul(pl.col('SK_LENGTH').truediv(pl.col('TOTAL_LEN'))),
            pl.col('VCR_0.25_0.5').mul(pl.col('SK_LENGTH').truediv(pl.col('TOTAL_LEN'))),
            pl.col('VCR_0.5_0.85').mul(pl.col('SK_LENGTH').truediv(pl.col('TOTAL_LEN'))),
            pl.col('VCR_0.85_1').mul(pl.col('SK_LENGTH').truediv(pl.col('TOTAL_LEN'))),
            pl.col('VCR_>=_1').mul(pl.col('SK_LENGTH').truediv(pl.col('TOTAL_LEN'))),
            pl.col('SK_LENGTH').alias('TOTAL_LEN')
        )

        out_ctx = PipelineContext()
        out_ctx.lf = lf

        return out_ctx