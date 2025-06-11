from .pipeline import PipelineStep, PipelineContext
from sqlalchemy import Engine
import polars as pl


class RouteidSKMapping(PipelineStep):
    def __init__(
            self,
            sql_engine: Engine,
            latest_reference_table: str
    ):
        super().__init__(step_name='SK_mapping')
        self._engine = sql_engine
        self.sk_mapping_table = 'MISC.MAPPING_LINKID'
        self.ref_table = latest_reference_table

    def execute(self, ctx: PipelineContext):
        df_map = pl.read_database_uri(
            'select linkid_15, linkid_22 from misc.mapping_linkid where linkid_22 is not null',
            uri=self._engine.url.render_as_string(hide_password=False)
        )

        df_ref = pl.read_database_uri(
            f'select distinct({ctx.linkid_col}) from {self.ref_table}',
            uri=self._engine.url.render_as_string(hide_password=False)
        )

        lf = ctx.lf.join(
            df_map.lazy(),
            left_on=ctx.linkid_col,
            right_on='LINKID_15',
            how='left'
        ).with_columns(
            pl.when(
                pl.col('LINKID_22').is_null()
            ).then(
                pl.col(ctx.linkid_col)
            ).otherwise(
                pl.col('LINKID_22')
            ).alias(
                ctx.linkid_col
            )
        ).filter(
            pl.col(ctx.linkid_col).is_in(
                df_ref[ctx.linkid_col]
            )
        ).select(
            pl.exclude('LINKID_22')
        )

        out_ctx = PipelineContext()
        out_ctx.lf = lf

        return out_ctx