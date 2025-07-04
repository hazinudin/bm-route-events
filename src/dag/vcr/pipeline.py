from dataclasses import dataclass
import polars as pl
from typing import Dict, Any, List
from abc import ABC, abstractmethod
from sqlalchemy import Engine
from pathlib import Path
import os


@dataclass
class PipelineContext:
    """
    Context object to pass on between pipeline stages.
    """
    lf: pl.LazyFrame = None
    
    # DuckDB table
    admin_table: str = 'POP_KAB_KOTA_24'
    total_pop_col: str = 'TOTAL_POP'
    regency_name_col: str = 'KAB_KOTA_N'

    # Default column names
    # RNI and capacity columns
    linkid_col: str = 'LINKID'
    survey_year_col: str = 'SURVEY_YEAR'
    rni_year_col: str = 'RNI_YEAR'
    road_type_col: str = 'ROAD_TYPE'
    road_stat_col: str = 'ROAD_STAT'  # Road status 'L' or 'LK'
    from_sta_col: str = 'FROM_STA'
    to_sta_col: str = 'TO_STA'
    lat_col: str = 'TO_STA_LAT'
    long_col: str = 'TO_STA_LONG'
    lane_code_col: str = 'LANE_CODE'
    lane_count_col: str = 'LANE_COUNT'
    llane_count_col: str = 'LLANE_COUNT'
    rlane_count_col: str = 'RLANE_COUNT'
    lanew_col: str = 'LANE_WIDTH'
    medw_col: str = 'MED_WIDTH'
    has_med_col: str = 'HAS_MEDIAN'
    total_lanew_col: str = 'TOTAL_LANE_WIDTH'
    avg_lanew_col: str = 'AVG_LANE_WIDTH'
    min_lanew_col: str = 'MIN_LANE_WIDTH'
    min_llanew_col: str = 'MIN_LLANE_WIDTH'
    min_rlanew_col: str = 'MIN_RLANE_WIDTH'
    dir_count_col: str = 'DIR_COUNT'  # Direction counts, 1 or 2
    dir_col: str = 'DIR'
    l_terrain_col: str = 'L_TERRAIN_TYPE'
    r_terrain_col: str = 'R_TERRAIN_TYPE'
    terrain_col: str = 'TERRAIN'
    l_land_use_col: str = 'L_LAND_USE'
    r_land_use_col: str = 'R_LAND_USE'
    land_use_col: str = 'LAND_USE'
    r_out_shwidth_col: str = 'R_OUT_SHWIDTH'
    l_out_shwidth_col: str = 'L_OUT_SHWIDTH'
    shwidth_col: str = 'SHWIDTH'

    # RTC and volume columns
    rtc_dir_col: str = 'SURVEY_DIREC'
    survey_hours_col: str = 'SURVEY_HOURS'
    survey_date_col: str = 'SURVEY_DATE'
    veh1_col: str = 'VEH1'
    veh2_col: str = 'VEH2'
    veh3_col: str = 'VEH3'
    veh4_col: str = 'VEH4'
    veh5_col: str = 'VEH5'
    non_motor_col: str = 'NON_MOTORIZED'

    # Step execution time
    execution_time: float = 0

    @property
    def row_count(self) -> int:
        if self.lf is None:
            return None
        else:
            return self.lf.select(pl.len()).collect().item()
    

class MultiDataContext(PipelineContext):
    """
    Pipeline context for multiple data join.
    """
    datas: Dict[str, pl.LazyFrame] = {}
    join_key: Dict[str, List[str]] = {}

    def add_dataset(
            self,
            name: str,
            lazyframe: pl.LazyFrame,
            join_key: List[str]
    ):
        self.datas[name.upper()] = lazyframe
        self.join_key[name.upper()] = join_key

        return self

    @property
    def datasets(self) -> list:
        """
        Return all available datasets name.
        """
        return self.datas.keys()


class PipelineStep(ABC):
    """
    PipelineStep object.
    """
    def __init__(self, step_name: str):
        self.step_name = step_name

    @abstractmethod
    def execute(self, context: PipelineContext) -> PipelineContext:
        """
        Execute the pipeline step
        """
        pass


class VCRPipeline:
    """
    VCR data pipeline which consists of:
        1. Road capacity data pipeline
        2. Passenger Car Equivalent (PCE) data pipeline
        3. LINKID mapping and data extraction
    """
    def __init__(
            self,
            year: int,
            rni_table: str,
            rtc_table: str
    ):
        self.year = year
        self.rni_table = rni_table
        self.rtc_table = rtc_table

        self.ctx = PipelineContext()

    def execute_capacity_steps(
            self,
            rni_ctx: PipelineContext,
            volh_ctx: PipelineContext,
        ) -> PipelineContext:
        """
        Capacity calculation pipeline steps
        """
        from .c0 import CapacityC0
        from .fclj import CapacityFCLJLookup, CapacityFCLJInterpolation
        from .fchs import CapacityFCHSLookup, CapacityFCHSInterpolation
        from .fcpa import CapacityFCPALookup
        from .fcuk import CapacityFCUK
        from .capacity import FinalCapacityCalculation

        # RNI and VOLH context
        multi_ctx = MultiDataContext()

        multi_ctx.add_dataset(
            name='RNI',
            lazyframe=rni_ctx.lf,
            join_key=[
                rni_ctx.linkid_col
            ]
        )

        multi_ctx.add_dataset(
            name='VOLH',
            lazyframe=volh_ctx.lf,
            join_key=[
                volh_ctx.linkid_col
            ]
        )

        c0_lookup = CapacityC0()
        fclj_lookup = CapacityFCLJLookup()
        fclj_interpolation = CapacityFCLJInterpolation()
        fchs_lookup = CapacityFCHSLookup()
        fchs_interpolation = CapacityFCHSInterpolation()
        fcpa_lookup = CapacityFCPALookup()
        fcuk_lookup = CapacityFCUK()
        final_cap = FinalCapacityCalculation()

        c0 = c0_lookup.execute(rni_ctx)
        fclj_lookup_ctx = fclj_lookup.execute(rni_ctx)
        fclj = fclj_interpolation.execute(fclj_lookup_ctx)
        fchs_lookup_ctx = fchs_lookup.execute(multi_ctx)
        fchs = fchs_interpolation.execute(fchs_lookup_ctx)
        fcpa = fcpa_lookup.execute(multi_ctx)
        fcuk = fcuk_lookup.execute(rni_ctx)

        cap_ctx = MultiDataContext()

        cap_ctx.add_dataset(
            name='RNI',
            lazyframe=rni_ctx.lf,
            join_key=[
                rni_ctx.linkid_col
            ]
        )
        
        cap_ctx.add_dataset(
            name='C0',
            lazyframe=c0.lf,
            join_key=[]
        )
        
        cap_ctx.add_dataset(
            name='FCLJ', 
            lazyframe=fclj.lf,
            join_key=[
                cap_ctx.linkid_col,
                cap_ctx.from_sta_col,
                cap_ctx.to_sta_col
            ]
        )

        cap_ctx.add_dataset(
            name='FCHS',
            lazyframe=fchs.lf,
            join_key=[
                cap_ctx.linkid_col,
                cap_ctx.from_sta_col,
                cap_ctx.to_sta_col,
                cap_ctx.dir_col
            ]
        )

        cap_ctx.add_dataset(
            name='FCPA',
            lazyframe=fcpa.lf,
            join_key=[
                cap_ctx.linkid_col,
                cap_ctx.from_sta_col,
                cap_ctx.to_sta_col
            ]
        )
        
        cap_ctx.add_dataset(
            name='FCUK',
            lazyframe=fcuk.lf,
            join_key=[
                cap_ctx.linkid_col,
                cap_ctx.from_sta_col,
                cap_ctx.to_sta_col
            ]
        )

        cap = final_cap.execute(cap_ctx)

        return cap 
    
    def execute_pce_steps(
            self,
            rni_ctx: PipelineContext,
            volh_ctx: PipelineContext
    ) -> PipelineContext:
        """
        Traffic volume calculation pipeline steps
        """
        from .pce import VolumePCECalculation
        from .pce import VolumePCELookup

        # RNI and VOLH context
        multi_ctx = MultiDataContext()

        multi_ctx.add_dataset(
            name='RNI',
            lazyframe=rni_ctx.lf,
            join_key=[
                rni_ctx.linkid_col
            ]
        )

        multi_ctx.add_dataset(
            name='VOLH',
            lazyframe=volh_ctx.lf,
            join_key=[
                volh_ctx.linkid_col
            ]
        )

        pce_lookup = VolumePCELookup()
        pce_calc = VolumePCECalculation()

        pce_lookup_ctx = pce_lookup.execute(multi_ctx)
        pce_ctx = pce_calc.execute(pce_lookup_ctx)

        return pce_ctx
    
    def execute(
            self, 
            refresh_data:bool=False, 
            sql_engine: Engine | None = None,
            rni_spatial_file: str = None
        ):
        """
        Execute all pipeline steps
        """
        from .rni import RNISegmentsExtractor, RNICombineSpatialJoin
        from .rtc import HourlyVolumeExtractor
        from .vcr import CalculateVCR, CalculateVCRSummary
        from .sk_mapping import RouteidSKMapping

        # Check and download the RNI data if not available
        if not (
            Path(f'{os.path.dirname(__file__)}/data/{self.rni_table.lower()}.parquet').is_file()
        ) or (
            refresh_data
        ):
            rni_extractor = RNISegmentsExtractor(
                sql_engine=sql_engine,
                table_name=self.rni_table,
                routes='ALL'
            )

            rni_ctx = rni_extractor.execute(PipelineContext())
        else:
            rni_ctx = PipelineContext(
                lf=pl.scan_parquet(
                    f'{os.path.dirname(__file__)}/data/{self.rni_table.lower()}.parquet'
                )
            )

        # Check and download the Volume data if not available
        if not (
            Path(f'{os.path.dirname(__file__)}/data/{self.rtc_table.lower()}.parquet').is_file()
        ) or (
            refresh_data
        ):
            volh_extractor = HourlyVolumeExtractor(
                sql_engine=sql_engine,
                table_name=self.rtc_table,
                routes='ALL'
            )

            volh_ctx = volh_extractor.execute(PipelineContext())
        else:
            volh_ctx = PipelineContext(
                lf=pl.scan_parquet(
                    f'{os.path.dirname(__file__)}/data/{self.rtc_table.lower()}.parquet'
                )
            )

        # Combine/join the RNI data with spatially query RNI (Kab/Kota and Population added)
        multi_rni_ctx = MultiDataContext()

        multi_rni_ctx.add_dataset(
            lazyframe=rni_ctx.lf,
            name='RNI',
            join_key=[
                multi_rni_ctx.linkid_col,
                multi_rni_ctx.from_sta_col,
            ]
        )

        multi_rni_ctx.add_dataset(
            lazyframe=pl.scan_parquet(
                f'{os.path.dirname(__file__)}/data/{rni_spatial_file}'
            ),
            name='RNI_SPATIAL_QUERY',
            join_key=[
                multi_rni_ctx.linkid_col,
                multi_rni_ctx.from_sta_col,
            ]
        )

        joined = RNICombineSpatialJoin()
        rni_combined_ctx = joined.execute(multi_rni_ctx)

         # Map LINKID if the data is below 2023
        if self.year < 2023:
            mapper = RouteidSKMapping(
                sql_engine=sql_engine,
                latest_reference_table='rni_2_2024'
            )

            volh_ctx = mapper.execute(volh_ctx)
            rni_combined_ctx = mapper.execute(rni_combined_ctx)

        # Calculate capacity
        cap_ctx = self.execute_capacity_steps(
            rni_ctx=rni_combined_ctx,
            volh_ctx=volh_ctx
        )

        # Calculate PCE
        pce_ctx = self.execute_pce_steps(
            rni_ctx=rni_combined_ctx,
            volh_ctx=volh_ctx
        )

        # Calculate VCR
        rni_pce_ctx = MultiDataContext()

        rni_pce_ctx.add_dataset(
            name='CAPACITY',
            lazyframe=cap_ctx.lf,
            join_key=[
                cap_ctx.linkid_col,
                cap_ctx.from_sta_col,
                cap_ctx.to_sta_col,
                cap_ctx.dir_col
            ]
        )

        rni_pce_ctx.add_dataset(
            name='PCE',
            lazyframe=pce_ctx.lf,
            join_key=[
                cap_ctx.linkid_col,
                cap_ctx.from_sta_col,
                cap_ctx.to_sta_col,
                cap_ctx.dir_col
            ]
        )

        final_vcr = CalculateVCR(year=self.year)

        vcr_ctx = final_vcr.execute(rni_pce_ctx)
        
        vcr_ctx.lf.collect().write_parquet(
            f'{os.path.dirname(__file__)}/data/vcr_{self.year}.parquet'
        )

        out_ctx = PipelineContext(
            lf = pl.scan_parquet(
                f'{os.path.dirname(__file__)}/data/vcr_{self.year}.parquet'
            )
        )

        return out_ctx
    