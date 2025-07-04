from .pipeline import PipelineContext, PipelineStep
from sqlalchemy import Engine
from typing import List, Union
import polars as pl
import os


class HourlyVolumeExtractor(PipelineStep):
    def __init__(
            self,
            sql_engine: Engine,
            table_name: str,
            routes: Union[List[str] | str] = 'ALL'
    ):
        super().__init__(step_name='hourly_volume_extractor')
        self._engine = sql_engine
        self.table_name = table_name
        self.routes = routes

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        """
        Load hourly volumne from RTC data into a Parquet file.
        """
        if self.routes == 'ALL':
            where_ = 'where 1=1'
        elif type(self.routes) is list:
            routes = [f"'{str(_)}'" for _ in self.routes]
            where_ = f"where {ctx.linkid_col} in ({', '.join(routes)})"

        query = f"""
        select 
        row_number() over (order by {ctx.linkid_col}) as rnum,
        {ctx.linkid_col}, {ctx.rtc_dir_col} as {ctx.dir_col}, {ctx.survey_hours_col}, 
        sum(num_veh1) as {ctx.veh1_col}, 
        sum(num_veh2)+sum(num_veh3)+sum(num_veh4) as {ctx.veh2_col},
        sum(num_veh5a) as {ctx.veh3_col},
        sum(num_veh5b)+sum(num_veh6a)+sum(num_veh6b) as {ctx.veh4_col},
        sum(num_veh7a)+sum(num_veh7b)+sum(num_veh7c)+sum(num_veh8) as {ctx.veh5_col},
        sum(num_veh8) as {ctx.non_motor_col},
        survey_date
        from {self.table_name}
        {where_}
        group by {ctx.linkid_col}, {ctx.rtc_dir_col}, {ctx.survey_hours_col}, {ctx.survey_date_col}
        --having count(*) = 4 --Cause serious issue, some routes use 3 interval instead of 4
        having count(*) >= 3
        """

        # Parquet file path
        pa_path = f'{os.path.dirname(__file__)}/data/{self.table_name}.parquet'

        df = pl.read_database_uri(
            query,
            uri=self._engine.url.render_as_string(hide_password=False),
            partition_on='rnum',
            partition_num=15
        ).select(
            pl.exclude('rnum')
        )

        # Write to parquet file
        df.write_parquet(
            pa_path
        )

        ctx.lf = pl.scan_parquet(pa_path)

        return ctx