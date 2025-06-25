import polars as pl
from .pipeline import PipelineStep, MultiDataContext, PipelineContext
import os
from typing import Literal
from sqlalchemy import Engine, text, inspect
from ...route_events.utils import ora_pl_dtype
from datetime import datetime


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
    def __init__(
            self,
            level: Literal['route', 'segment']='segment', 
            agg_method: Literal['mean', 'max']='max'
        ):
        super().__init__('VCR_summary')

        self.agg_method = agg_method
        self.level = level

    def execute(self, ctx: MultiDataContext) -> PipelineContext:
        """
        Calculate VCR summary.
        """
        if (
            'RNI' not in ctx.datasets
        ) or (
            'VCR' not in ctx.datasets
        ):
            raise KeyError('Context does not contain RNI or VCR dataset.')

        sk_lf = pl.scan_parquet(
            f'{os.path.dirname(__file__)}/data/route_sk_length.parquet'
        )

        lf = ctx.datas['VCR'].group_by(
            [
                ctx.linkid_col, 
                ctx.from_sta_col, 
                ctx.to_sta_col,
                ctx.survey_date_col,
                ctx.survey_hours_col
            ]
        ).agg(
            # pl.col('VCR').max(),
            pl.col('YEAR').max(),
            pl.col('CAPACITY').min(),
            # pl.col('TOTAL_PCE').max(),
            pl.col('^PCE_VEH.*$').max()
        ).group_by(
            [
                ctx.linkid_col,
                ctx.from_sta_col,
                ctx.to_sta_col,
                ctx.survey_date_col
            ]
        ).agg(
            pl.col('YEAR').max(),
            pl.col('CAPACITY').mean().alias('MEAN_CAPACITY'),
            pl.col('CAPACITY').max().alias('MAX_CAPACITY'),
            pl.col('^PCE_VEH.*$').max().name.prefix('MAX_'),
            pl.col('^PCE_VEH.*$').mean().name.prefix('MEAN_')
        ).group_by(
            ctx.linkid_col,
            ctx.from_sta_col,
            ctx.to_sta_col
        ).agg(
            pl.col('YEAR').max(),
            pl.col('MAX_CAPACITY').mean(),
            pl.col('MEAN_CAPACITY').max(),

            pl.col('^MAX_PCE_VEH.*$').mean(),
            pl.col('^MEAN_PCE_VEH.*$').mean(),

            pl.sum_horizontal(
                pl.col('^MAX_PCE_VEH.*$').mean()
            ).alias(
                'MAX_TOTAL_PCE'
            ),

            pl.sum_horizontal(
                pl.col('^MEAN_PCE_VEH.*$').mean()
            ).alias(
                'MEAN_TOTAL_PCE'
            )
        )
        
        if self.level == 'route':
            lf = lf.with_columns(
                VCR=pl.col(
                    f'{self.agg_method.upper()}_TOTAL_PCE'
                ).truediv(
                    pl.col(
                        f'{self.agg_method.upper()}_CAPACITY'
                    )
                )
            ).group_by(
                ctx.linkid_col
            ).agg(
                pl.col('YEAR').max(),
                pl.col(f'{self.agg_method.upper()}_TOTAL_PCE').mean().alias('AVG_TOTAL_PCE'),
                pl.col(f'{self.agg_method.upper()}_CAPACITY').mean().alias('AVG_CAPACITY'),
                pl.col('VCR').mean().alias('AVG_VCR'),
                pl.col('VCR').mul(
                    pl.col(ctx.to_sta_col).sub(
                        pl.col(ctx.from_sta_col)
                    )
                ).sum().truediv(
                    pl.col(ctx.to_sta_col).sub(
                        pl.col(ctx.from_sta_col)
                    ).sum()
                ).alias('VCR_WEIGHTED_AVG'), 
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
                pl.sum_horizontal(
                    pl.col('VCR_<_0.25'),
                    pl.col('VCR_0.25_0.5'),
                    pl.col('VCR_0.5_0.85'),
                    pl.col('VCR_0.85_1'),
                    pl.col('VCR_>=_1')
                ).alias('TOTAL_LEN').cast(pl.Float64).truediv(100)
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
            ).join(
                ctx.datas['RNI'].select(
                    ctx.linkid_col, 
                    pl.col(ctx.rni_year_col).cast(pl.Int16)
                ).group_by(
                    ctx.linkid_col
                ).agg(
                    pl.col(ctx.rni_year_col).max()
                ),
                on=ctx.linkid_col,
                how='left'
            )

        out_ctx = PipelineContext()
        out_ctx.lf = lf

        return out_ctx
    

class SegmentVCRLoader(PipelineStep):
    def __init__(
            self,
            sql_engine: Engine,
            table_name: str
    ):
        super().__init__(step_name='vcr_segment_loader')
        self._engine = sql_engine
        self.table_name = table_name
        self._inspect = inspect(sql_engine)

    def execute(self, ctx: PipelineContext):
        with self._engine.connect() as conn, conn.execution_options(
            isolation_level='READ COMMITTED'
        ):
            try:
                self._delete(conn, ctx.linkid_col, 'YEAR', ctx.lf)
                self._insert(conn, ctx.lf)
            except Exception as e:
                conn.rollback()
                raise e
            
            conn.commit()

        return
    
    def _delete(
            self,
            conn,
            linkid_col: str,
            year_col: str,
            lf: pl.LazyFrame
    ) -> None:
        """
        Delete the rows based on route ID and year column.
        """
        if not self._inspect.has_table(self.table_name):
            return
        
        # Delete chunks
        del_chunks = 600

        year_groups = lf.group_by(
            year_col
        ).agg(
            pl.col(linkid_col).unique()
        ).collect().rows()

        for group in year_groups:
            year_ = group[0]
            routes = [f"'{_}'" for _ in group[1]]

            if len(routes) <= del_chunks:
                conn.execute(
                    text(f"delete from {self.table_name} where {year_col} = {year_} and {linkid_col} in ({', '.join(routes)})")
                )
            else:

                # Iterate all through the chunks
                for chunk in range(len(routes)//del_chunks):
                    routes_chunk = routes[chunk*del_chunks:(chunk+1)*del_chunks]
                    conn.execute(
                        text(f"delete from {self.table_name} where {year_col} = {year_} and {linkid_col} in ({', '.join(routes_chunk)})")
                    )
                
                # Last part
                conn.execute(
                    text(f"delete from {self.table_name} where {year_col} = {year_} and {linkid_col} in ({', '.join(routes[chunk*del_chunks:len(routes)-1])})")
                )

        return
    
    def _insert(
            self,
            conn,
            lf: pl.LazyFrame
    ) -> None:
        """
        Insert all rows from the LazyFrame.
        """
        # Add update date column.
        df = lf.collect().with_columns(
            UPDATE_DATE=pl.lit(datetime.now()).dt.datetime()
        )

        if not self._inspect.has_table(self.table_name):
            df.write_database(
                self.table_name,
                connection=conn,
                engine_options={
                    'dtype': ora_pl_dtype(
                        df,
                        date_cols_keywords='DATE'
                    )
                }
            )
        else:
            df.write_database(
                self.table_name,
                connection=conn,
                if_table_exists='append'
            )

        return