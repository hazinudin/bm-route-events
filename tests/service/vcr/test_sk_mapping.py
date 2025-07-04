import unittest
from dotenv import load_dotenv
import polars as pl
from sqlalchemy import create_engine
import os
from src.dag.vcr import RouteidSKMapping, PipelineContext


load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('SMD_USER')
PWD = os.getenv('SMD_PWD')
SCRATCH_FOLDER = os.getenv('SCRATCH_FOLDER')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")


class TestSKMapping(unittest.TestCase):
    def test_rni_linkid_sk_mapping(self):
        """
        Test SK Mapping step
        """
        ctx = PipelineContext()
        ctx.lf = pl.scan_parquet(
            f'{SCRATCH_FOLDER}/rtc_2022.parquet'
        )

        mapping = RouteidSKMapping(
            sql_engine=engine,
            latest_reference_table='SMD.RNI_2_2024'
        )
        out_ctx = mapping.execute(ctx)

        df = out_ctx.lf.collect()

        self.assertFalse(df.is_empty())
