from .pipeline import PipelineContext, PipelineStep
import duckdb
import os
from route_events.geometry import LAMBERT_WKT


class RNISpatialQuery(PipelineStep):
    """
    Add Kota/Kabupaten attribute to the RNI data.
    """
    def __init__(self, rni_parquet_file: str):
        super().__init__(step_name='RNI_spatial_query')
        self.rni_file = rni_parquet_file
    
    def execute(self, ctx: PipelineContext) -> PipelineContext:
        conn = duckdb.connect(f'{os.path.dirname(__file__)}/data/duck.db')
        conn.execute('install spatial; load spatial;')

        result = conn.sql(
            f"""
            copy(
                select 
                rni.* exclude({ctx.road_stat_col}),
                {ctx.regency_name_col}, 
                case when(contains({ctx.regency_name_col}, 'KOTA')) then 'K' else 'LK' end as {ctx.road_stat_col}, 
                {ctx.total_pop_col}
                from '{self.rni_file}' rni
                cross join {ctx.admin_table}
                where 
                ST_Contains(
                    geom,
                    ST_Transform(ST_Point({ctx.lat_col}, {ctx.long_col}), 'EPSG:4326', '{LAMBERT_WKT}')
                )
            ) to '{self.rni_file.split('.')[0] + '_spatial_query.parquet'}'
            """
        )

        return