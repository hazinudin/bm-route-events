from .pipeline import PipelineStep, PipelineContext, MultiDataContext
from sqlalchemy import Engine
from typing import List, Union
import polars as pl
import os


class RNISegmentsExtractor(PipelineStep):
    def __init__(
            self, 
            sql_engine: Engine,
            table_name: str,
            routes: Union[List[str] | str] = 'ALL'
        ):
        super().__init__(step_name='rni_db_extractor')
        self._engine = sql_engine
        self.table_name = table_name
        self.routes = routes

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        """
        Load RNI data from database into a Parquet file.
        """
        if self.routes == 'ALL':
            where_ = 'where 1=1'
        elif type(self.routes) is list:
            routes = [f"'{str(_)}'" for _ in self.routes]
            where_ = f"where {ctx.linkid_col} in ({', '.join(routes)})"

        query = f"""
        select 
        row_number() over (order by {ctx.linkid_col}, {ctx.from_sta_col}, {ctx.to_sta_col}) as rnum,        
        {ctx.linkid_col}, 
        {ctx.from_sta_col}, 
        {ctx.to_sta_col},
        
        case when length({ctx.linkid_col}) <= 5 then 'LK' else 'K' end as {ctx.road_stat_col},
        
        max({ctx.survey_year_col}) as {ctx.rni_year_col},

        max({ctx.road_type_col}) as {ctx.road_type_col},

        max({ctx.long_col}) as {ctx.long_col},
        max({ctx.lat_col}) as {ctx.lat_col},

        sum({ctx.lanew_col}) as {ctx.total_lanew_col},
        avg({ctx.lanew_col}) as {ctx.avg_lanew_col},
        min({ctx.lanew_col}) as {ctx.min_lanew_col},
        min(case when substr({ctx.lane_code_col}, 1 ,1) = 'L' then {ctx.lanew_col} else 0 end) as {ctx.min_llanew_col},
        min(case when substr({ctx.lane_code_col}, 1, 1) = 'R' then {ctx.lanew_col} else 0 end) as {ctx.min_rlanew_col},
        
        count({ctx.lane_code_col}) as {ctx.lane_count_col},
        sum(case when substr({ctx.lane_code_col}, 1 ,1) = 'L' then 1 else 0 end) as LLANE_COUNT,
        sum(case when substr({ctx.lane_code_col}, 1 ,1) = 'R' then 1 else 0 end) as RLANE_COUNT,

        case when 
        (sum(case when substr({ctx.lane_code_col}, 1 ,1) = 'L' then 1 else 0 end) >= 1) and
        (sum(case when substr({ctx.lane_code_col}, 1 ,1) = 'R' then 1 else 0 end) >= 1) then 2 else 1 
        end as {ctx.dir_count_col},
        
        case when max({ctx.medw_col}) > 0 then 1 else 0 end as {ctx.has_med_col},        

        greatest(
            cast(max(substr({ctx.l_terrain_col}, 2, 1)) as int),
            cast(max(substr({ctx.r_terrain_col}, 2,1)) as int)
        ) as {ctx.terrain_col},

        greatest(max({ctx.l_land_use_col}), max({ctx.r_land_use_col})) as {ctx.land_use_col},
        
        max({ctx.r_out_shwidth_col})+max({ctx.l_out_shwidth_col}) as {ctx.shwidth_col},
        max({ctx.r_out_shwidth_col}) as {ctx.r_out_shwidth_col},
        max({ctx.l_out_shwidth_col}) as {ctx.l_out_shwidth_col}

        from {self.table_name}
        {where_}
        group by {ctx.linkid_col}, {ctx.from_sta_col}, {ctx.to_sta_col}
        """
        # Parquet file path
        pa_path = f'{os.path.dirname(__file__)}/data/{self.table_name.lower()}.parquet'

        df = pl.read_database_uri(
            query, 
            uri=self._engine.url.render_as_string(hide_password=False),
            partition_on='rnum',
            partition_num=15
        ).select(
            pl.exclude(['RNUM'])
        ).with_columns(
            pl.col(ctx.rni_year_col).cast(pl.Int16),
            pl.col(ctx.from_sta_col).cast(pl.Int64),
            pl.col(ctx.to_sta_col).cast(pl.Int64),
            pl.col(ctx.lane_count_col).cast(pl.Int16),
            pl.col(ctx.dir_count_col).cast(pl.Int16),
            pl.col(ctx.terrain_col).cast(pl.Int16)
        )
        
        # Write to parquet file
        df.write_parquet(
            pa_path
        )

        ctx.lf = pl.scan_parquet(pa_path)

        return ctx
    

class RNICombineSpatialJoin(MultiDataContext):
    def __init__(self):
        super().__init__('RNI_join_with_spatial_query_result')

    def execute(self, ctx: MultiDataContext) -> PipelineContext:
        if (
            'RNI' not in ctx.datas.keys()
        ) or (
            'RNI_SPATIAL_QUERY' not in ctx.datas.keys()
        ):
            raise KeyError('Context does not contain RNI or RNI_SPATIAL_QUERY')
        
        lf = ctx.datas['RNI'].select(
            pl.exclude(ctx.road_stat_col)
        ).join(
            ctx.datas['RNI_SPATIAL_QUERY'].with_columns(
                pl.col(ctx.from_sta_col).cast(pl.Int64),
                pl.col(ctx.to_sta_col).cast(pl.Int64)
            ),
            on=ctx.join_key['RNI_SPATIAL_QUERY'],
            how='left'
        ).select(
            pl.exclude(
                '^*._right$',
                'KAB_KOTA_NAME',
                ctx.total_pop_col,
                ctx.road_stat_col
            ),
            pl.col(ctx.total_pop_col).forward_fill().over(
                [ctx.linkid_col], order_by=[ctx.from_sta_col]
            ),
            pl.when(
                pl.col('KAB_KOTA_NAME').str.contains('KOTA')
            ).then(
                pl.lit('K')
            ).otherwise(
                pl.lit('LK')
            ).forward_fill().over(
                # Try to forward fill from previous segment
                [ctx.linkid_col], 
                order_by=[ctx.from_sta_col]
            ).fill_null(
                # If the route is completely null then fill with LK
                'LK'
            ).alias(
                ctx.road_stat_col
            ),
            pl.col('KAB_KOTA_NAME').forward_fill().over(
                [ctx.linkid_col], order_by=[ctx.from_sta_col]
            )
        )

        out_ctx = PipelineContext()
        out_ctx.lf = lf

        return out_ctx