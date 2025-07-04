import unittest
from src.dag.vcr import (
    HourlyVolumeExtractor,
    PipelineContext,
    MultiDataContext,
    VolumePCELookup,
    VolumePCECalculation
)
from dotenv import load_dotenv
import os
import polars as pl
from sqlalchemy import create_engine


load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('SMD_USER')
PWD = os.getenv('SMD_PWD')
SCRATCH_FOLDER = os.getenv('SCRATCH_FOLDER')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")


class TestVolumePipeline(unittest.TestCase):
    def test_rtc_extractor(self):
        """
        Test data extraction from RTC table.
        """
        volh = HourlyVolumeExtractor(
            engine,
            table_name='rtc_2019',
            routes='ALL'
        )

        ctx_ = PipelineContext()
        ctx = volh.execute(ctx=ctx_)

        self.assertTrue(ctx.row_count > 0)

    def test_pce_lookup(self):
        multi_ctx_ = MultiDataContext()
        
        multi_ctx_.add_dataset(
            name='RNI',
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rni_2_2024.parquet'
            ),
            join_key=[
                multi_ctx_.linkid_col
            ]
        )

        multi_ctx_.add_dataset(
            name='VOLH',
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rtc_2024.parquet'
            ),
            join_key=[
                multi_ctx_.linkid_col
            ]
        )

        pce = VolumePCELookup()
        ctx = pce.execute(multi_ctx_)

        self.assertTrue(ctx.row_count > 0)
        self.assertTrue(
            ctx.lf.filter(
                pl.col('PCE_N').is_null().and_(
                    pl.col('PCE_O').is_null()
                ).and_(
                    pl.col('PCE').is_null()
                )
            ).collect().is_empty()
        )

    def test_all(self):
        multi_ctx_ = MultiDataContext()
        
        multi_ctx_.add_dataset(
            name='RNI',
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rni_2_2024.parquet'
            ),
            join_key=[
                multi_ctx_.linkid_col
            ]
        )

        multi_ctx_.add_dataset(
            name='VOLH',
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rtc_2024.parquet'
            ),
            join_key=[
                multi_ctx_.linkid_col
            ]
        )

        pce = VolumePCELookup()
        calc = VolumePCECalculation()

        ctx = pce.execute(multi_ctx_)
        ctx = calc.execute(ctx)

        self.assertTrue(ctx.row_count > 0)

        self.assertTrue(
            ctx.lf.filter(
                pl.col('TOTAL_PCE').is_null()
            ).collect().is_empty()
        )
        
        self.assertTrue(
            ctx.lf.filter(
                pl.col('TOTAL_PCE').is_nan()
            ).collect().is_empty()
        )

