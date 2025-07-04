import unittest
from src.dag.vcr import (
    RNISegmentsExtractor,
    RNICombineSpatialJoin,
    PipelineContext,
    MultiDataContext,
    CapacityC0,
    CapacityFCLJLookup,
    CapacityFCLJInterpolation,
    CapacityFCHSLookup,
    CapacityFCHSInterpolation,
    CapacityFCPALookup,
    CapacityFCUK,
    FinalCapacityCalculation
)
from dotenv import load_dotenv
import os
import polars as pl
from sqlalchemy import create_engine


load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('SMD_USER')
PWD = os.getenv('SMD_PWD')
SCRATCH_FOLDER=os.getenv('SCRATCH_FOLDER')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")


class TestRNICapacityPipeline(unittest.TestCase):
    def test_rni_extractor(self):
        """
        Test RNI extractor query.
        """
        rni = RNISegmentsExtractor(
            engine,
            table_name='rni_2_2023',
            routes='ALL'
        )
        ctx_ = PipelineContext()
        ctx = rni.execute(ctx=ctx_)

        self.assertTrue(ctx.row_count > 0)

    def test_rni_combine_spatial_query(self):
        """
        Test RNI data join with spatial query result.
        """
        ctx = MultiDataContext()

        ctx.add_dataset(
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rni_2_2024.parquet'
            ),
            name='RNI',
            join_key=[
                ctx.linkid_col,
                ctx.from_sta_col,
                ctx.to_sta_col
            ]
        )

        ctx.add_dataset(
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rni_2_2024_spatial_query.parquet'
            ),
            name='RNI_SPATIAL_QUERY',
            join_key=[
                ctx.linkid_col,
                ctx.from_sta_col,
                ctx.to_sta_col
            ]
        )

        joined = RNICombineSpatialJoin()
        out_ctx = joined.execute(ctx)

        self.assertTrue('TOTAL_POP' in out_ctx.lf.collect_schema().names())
        
        self.assertTrue(
            out_ctx.lf.filter(
                pl.col('TOTAL_POP').is_null()
            ).collect().is_empty()
        )

        self.assertTrue(
            ctx.datas['RNI'].join(
                out_ctx.lf,
                on=ctx.join_key['RNI'],
                how='anti'
            ).collect().is_empty()
        )

    def test_capacity_c0(self):
        """
        Test RNI C0 (base capacity) data join.
        """
        ctx_ = PipelineContext(
            lf = pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rni_2_2024.parquet'
            )
        )

        c0 = CapacityC0()
        ctx = c0.execute(ctx=ctx_)

        df = ctx.lf.collect()

        self.assertFalse(df.is_empty())
        self.assertFalse(df['C0'].is_null().any())
        self.assertFalse(df['C0'].eq(0).any())

    def test_capacity_fclj_lookup(self):
        """
        Test FCLJ lookup value.
        """
        ctx_ = PipelineContext(
            lf = pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rni_2_2024.parquet'
            )
        )

        fclj = CapacityFCLJLookup()
        ctx = fclj.execute(ctx=ctx_)

        df = ctx.lf.collect()

        self.assertFalse(df.is_empty())
        self.assertFalse(df['FCLJ_STRUCT'].is_null().any())

    def test_capacity_fclj_interpolation(self):
        """
        Test FCLJ interpolation.
        """
        ctx_ = PipelineContext(
            lf = pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rni_2_2024.parquet'
            )
        )

        lookup = CapacityFCLJLookup()
        interpolation = CapacityFCLJInterpolation()

        ctx = lookup.execute(ctx=ctx_)
        ctx = interpolation.execute(ctx=ctx)

        df = ctx.lf.collect()

        self.assertFalse(df.is_empty())
        self.assertFalse(df['FCLJ'].is_nan().any())

    def test_capacity_fchs(self):
        """
        Test FCHS lookup
        """
        ctx_ = MultiDataContext()

        ctx_.add_dataset(
            name='RNI',
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rni_2_2024.parquet'
            ),
            join_key=[
                ctx_.linkid_col
            ]
        )

        ctx_.add_dataset(
            name='VOLH',
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rtc_2024.parquet'
            ),
            join_key=[
                ctx_.linkid_col
            ]
        )

        lookup = CapacityFCHSLookup()
        interpolate = CapacityFCHSInterpolation()

        ctx = lookup.execute(ctx_)

        ctx = interpolate.execute(ctx)
        df = ctx.lf.collect()

        self.assertFalse(df['FCHS'].is_nan().any())
        self.assertFalse(df.is_empty())

    def test_capacity_fcpa(self):
        """
        Test FCPA lookup
        """
        ctx_ = MultiDataContext()

        ctx_.add_dataset(
            name='RNI',
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rni_2_2021.parquet'
            ),
            join_key=[
                ctx_.linkid_col
            ]
        )

        ctx_.add_dataset(
            name='VOLH',
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rtc_2019.parquet'
            ),
            join_key=[
                ctx_.linkid_col
            ]
        )

        lookup = CapacityFCPALookup()

        ctx = lookup.execute(ctx_)
        df = ctx.lf.collect()

        self.assertFalse(df.is_empty())
        self.assertFalse(df['FCPA'].is_nan().any())
        self.assertTrue(df['FCPA'].gt(0).all())

    def test_fcuk(self):
        ctx = MultiDataContext()

        ctx.add_dataset(
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rni_2_2024.parquet'
            ),
            name='RNI',
            join_key=[
                ctx.linkid_col,
                ctx.from_sta_col,
                ctx.to_sta_col
            ]
        )

        ctx.add_dataset(
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rni_2_2024_spatial_query.parquet'
            ),
            name='RNI_SPATIAL_QUERY',
            join_key=[
                ctx.linkid_col,
                ctx.from_sta_col,
                ctx.to_sta_col
            ]
        )

        joined = RNICombineSpatialJoin()
        out_ctx = joined.execute(ctx)

        fcuk = CapacityFCUK()
        out_ctx = fcuk.execute(out_ctx)

        self.assertTrue(
            out_ctx.lf.filter(
                pl.col('FCUK').is_null().or_(
                    pl.col('FCUK').le(0)
                )
            ).collect().is_empty()
        )

    def test_all(self):
        rni_ctx_ = PipelineContext(
            lf = pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rni_2_2024.parquet'
            )
        )

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

        c0 = CapacityC0()
        fclj_lookup = CapacityFCLJLookup()
        fclj_interpolation = CapacityFCLJInterpolation()
        fchs_lookup = CapacityFCHSLookup()
        fchs_interpolation = CapacityFCHSInterpolation()
        fcpa_lookup = CapacityFCPALookup()
        final_cap = FinalCapacityCalculation()

        c0_result = c0.execute(rni_ctx_)
        
        fclj_lookup_result = fclj_lookup.execute(rni_ctx_)
        fclj = fclj_interpolation.execute(fclj_lookup_result)

        fchs_lookup_result = fchs_lookup.execute(multi_ctx_)
        fchs = fchs_interpolation.execute(fchs_lookup_result)

        fcpa = fcpa_lookup.execute(multi_ctx_)

        cap_ctx = MultiDataContext()

        cap_ctx.add_dataset(
            name='C0',
            lazyframe=c0_result.lf,
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

        cap = final_cap.execute(cap_ctx)

        df = cap.lf.join(
            rni_ctx_.lf,
            on=[
                cap_ctx.linkid_col,
                cap_ctx.from_sta_col,
                cap_ctx.to_sta_col
            ]
        ).collect()

        df.write_csv(f'{SCRATCH_FOLDER}/rni_cap_2_2024.csv')

        self.assertFalse(df.is_empty())