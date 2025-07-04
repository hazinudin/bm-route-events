import unittest
from src.dag.vcr import (
    PipelineContext,
    RNISpatialQuery
)
import polars as pl
from dotenv import load_dotenv
import os


load_dotenv('tests/dev.env')
SCRATCH_FOLDER = os.getenv('SCRATCH_FOLDER')


class TestRNISpatialQuery(unittest.TestCase):
    def test_spatial_query(self):
        ctx_ = PipelineContext()

        query = RNISpatialQuery(f'{SCRATCH_FOLDER}/rni_2_2024.parquet')
        out_ctx = query.execute(ctx_)

        self.assertTrue(True)