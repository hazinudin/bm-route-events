import unittest
import src.dag.vcr as vcr
import polars as pl
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from typing import List


load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('SMD_USER')
PWD = os.getenv('SMD_PWD')
SCRATCH_FOLDER = os.getenv('SCRATCH_FOLDER')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")


class TestVCRPipeline(unittest.TestCase):
    @staticmethod
    def rni_spatial_ctx(rni: str, spat_query: str) -> vcr.PipelineContext:
        multi_rni_ctx= vcr.MultiDataContext()

        multi_rni_ctx.add_dataset(
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/{rni}'
            ),
            name='RNI',
            join_key=[
                multi_rni_ctx.linkid_col,
                multi_rni_ctx.from_sta_col,
                multi_rni_ctx.to_sta_col
            ]
        )

        multi_rni_ctx.add_dataset(
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/{spat_query}'
            ),
            name='RNI_SPATIAL_QUERY',
            join_key=[
                multi_rni_ctx.linkid_col,
                multi_rni_ctx.from_sta_col,
                multi_rni_ctx.to_sta_col
            ]
        )

        joined = vcr.RNICombineSpatialJoin()
        rni_combined_ctx = joined.execute(multi_rni_ctx)

        return rni_combined_ctx
    
    @staticmethod
    def volh_ctx(rtc:str) -> vcr.PipelineContext:
        return vcr.PipelineContext(
            lf=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/{rtc}'
            )
        )
    
    def test_capacity_pipeline(self):
        """
        Test capacity pipeline
        """

        pipeline = vcr.VCRPipeline(
            year=2022,
            rni_table='',
            rtc_table=''
        )

        cap = pipeline.execute_capacity_steps(
            rni_ctx=self.rni_spatial_ctx(
                rni='rni_2_2022.parquet',
                spat_query='iri_2_2022_spatial_query.parquet'
            ),
            volh_ctx=self.volh_ctx(
                rtc='rtc_2022.parquet'
            )
        )

        self.assertTrue(
            self.volh_ctx(
                rtc='rtc_2022.parquet'
            ).lf.join(
                cap.lf,
                on='LINKID',
                how='anti'
            ).collect().is_empty()
        )

        self.assertFalse(cap.lf.collect().is_empty())

    def test_pce_pipeline(self):
        """
        Test PCE pipeline
        """
        pipeline = vcr.VCRPipeline(
            year=2022,
            rni_table='',
            rtc_table=''
        )

        pce = pipeline.execute_pce_steps(
            rni_ctx=self.rni_spatial_ctx(
                rni='rni_2_2022.parquet',
                spat_query='iri_2_2022_spatial_query.parquet'
            ),
            volh_ctx=self.volh_ctx(
                rtc='rtc_2022.parquet'
            )
        )

        self.assertTrue(
            self.volh_ctx(
                rtc='rtc_2022.parquet'
            ).lf.join(
                pce.lf,
                on='LINKID',
                how='anti'
            ).collect().is_empty()
        )

        self.assertFalse(pce.lf.collect().is_empty())

    def test_vcr_pipeline(self):
        """
        Test VCR pipeline
        """
        pipeline = vcr.VCRPipeline(
            year=2019,
            rni_table='rni_2_2021',
            rtc_table='rtc_2019'
        )

        pipeline.execute(
            refresh_data=False,
            sql_engine=engine,
            rni_spatial_file='iri_2_2021_spatial_query.parquet'
        )

        self.assertTrue(True)

    def test_vcr_from_default_pce(self):
        multi_rni_ctx= vcr.MultiDataContext()

        multi_rni_ctx.add_dataset(
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rni_2_2024.parquet'
            ),
            name='RNI',
            join_key=[
                multi_rni_ctx.linkid_col,
                multi_rni_ctx.from_sta_col,
                multi_rni_ctx.to_sta_col
            ]
        )

        multi_rni_ctx.add_dataset(
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rni_2_2024_spatial_query.parquet'
            ),
            name='RNI_SPATIAL_QUERY',
            join_key=[
                multi_rni_ctx.linkid_col,
                multi_rni_ctx.from_sta_col,
                multi_rni_ctx.to_sta_col
            ]
        )

        joined = vcr.RNICombineSpatialJoin()
        rni_combined_ctx = joined.execute(multi_rni_ctx)

        rni_ctx_ = vcr.PipelineContext(
            lf = rni_combined_ctx.lf
        )

        c0 = vcr.CapacityC0()
        fclj_lookup = vcr.CapacityFCLJLookup()
        fclj_interpolation = vcr.CapacityFCLJInterpolation()
        fcuk_lookup = vcr.CapacityFCUK()

        c0_result = c0.execute(rni_ctx_)
        fclj_lookup_result = fclj_lookup.execute(rni_ctx_)
        fclj = fclj_interpolation.execute(fclj_lookup_result)
        fcuk = fcuk_lookup.execute(rni_ctx_)

        cap_ctx = vcr.MultiDataContext()

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
            name='FCUK',
            lazyframe=fcuk.lf,
            join_key=[
                cap_ctx.linkid_col,
                cap_ctx.from_sta_col,
                cap_ctx.to_sta_col
            ]
        )

        cap_lf = c0_result.lf.join(
            cap_ctx.datas['FCLJ'],
            on=cap_ctx.join_key['FCLJ']
        ).join(
            cap_ctx.datas['FCUK'],
            on=cap_ctx.join_key['FCUK']
        ).with_columns(
            CAPACITY=pl.col(
                'C0'
            ).mul(
                pl.col('FCLJ')
            )
        )

        cap = vcr.PipelineContext(lf=cap_lf)

        # VCR

        vcr_ctx = vcr.MultiDataContext()

        vcr_ctx.add_dataset(
            name='CAPACITY',
            lazyframe=cap.lf,
            join_key=[
                cap_ctx.linkid_col,
                cap_ctx.from_sta_col,
                cap_ctx.to_sta_col,
                cap_ctx.dir_col
            ]
        )

        vcr_ctx.add_dataset(
            name='PCE',
            lazyframe=pl.scan_parquet(
                'src/service/vcr/data/pce_default.parquet'
            ),
            join_key=[
                cap_ctx.linkid_col
            ]
        )

        final_vcr = vcr.CalculateVCR(year=0)

        final_vcr_ctx = final_vcr.execute(vcr_ctx)

        final_vcr_ctx.lf.collect().with_columns(
            SURVEY_DATE=pl.lit(0),
            SURVEY_HOURS=pl.lit(0)
        ).write_parquet(
            f'{SCRATCH_FOLDER}/vcr_2023.parquet'
        )
        
        self.assertTrue(final_vcr_ctx.row_count > 0)

    def test_vcr(self):
        multi_rni_ctx= vcr.MultiDataContext()

        multi_rni_ctx.add_dataset(
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rni_2_2024.parquet'
            ),
            name='RNI',
            join_key=[
                multi_rni_ctx.linkid_col,
                multi_rni_ctx.from_sta_col,
                multi_rni_ctx.to_sta_col
            ]
        )

        multi_rni_ctx.add_dataset(
            lazyframe=pl.scan_parquet(
                f'{SCRATCH_FOLDER}/rni_2_2024_spatial_query.parquet'
            ),
            name='RNI_SPATIAL_QUERY',
            join_key=[
                multi_rni_ctx.linkid_col,
                multi_rni_ctx.from_sta_col,
                multi_rni_ctx.to_sta_col
            ]
        )

        joined = vcr.RNICombineSpatialJoin()
        rni_combined_ctx = joined.execute(multi_rni_ctx)

        rni_ctx_ = vcr.PipelineContext(
            lf = rni_combined_ctx.lf
        )

        multi_ctx_ = vcr.MultiDataContext()
        
        multi_ctx_.add_dataset(
            name='RNI',
            lazyframe=rni_combined_ctx.lf,
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

        c0 = vcr.CapacityC0()
        fclj_lookup = vcr.CapacityFCLJLookup()
        fclj_interpolation = vcr.CapacityFCLJInterpolation()
        fchs_lookup = vcr.CapacityFCHSLookup()
        fchs_interpolation = vcr.CapacityFCHSInterpolation()
        fcpa_lookup = vcr.CapacityFCPALookup()
        final_cap = vcr.FinalCapacityCalculation()
        fcuk_lookup = vcr.CapacityFCUK()

        c0_result = c0.execute(rni_ctx_)
        
        fclj_lookup_result = fclj_lookup.execute(rni_ctx_)
        fclj = fclj_interpolation.execute(fclj_lookup_result)

        fchs_lookup_result = fchs_lookup.execute(multi_ctx_)
        fchs = fchs_interpolation.execute(fchs_lookup_result)

        fcpa = fcpa_lookup.execute(multi_ctx_)

        fcuk = fcuk_lookup.execute(rni_ctx_)

        cap_ctx = vcr.MultiDataContext()

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

        # VOLUME

        pce_lookup = vcr.VolumePCELookup()
        pce_calc = vcr.VolumePCECalculation()

        ctx = pce_lookup.execute(multi_ctx_)
        pce_ctx = pce_calc.execute(ctx)

        # VCR

        vcr_ctx = vcr.MultiDataContext()

        vcr_ctx.add_dataset(
            name='CAPACITY',
            lazyframe=cap.lf,
            join_key=[
                cap_ctx.linkid_col,
                cap_ctx.from_sta_col,
                cap_ctx.to_sta_col,
                cap_ctx.dir_col
            ]
        )

        vcr_ctx.add_dataset(
            name='PCE',
            lazyframe=pce_ctx.lf,
            join_key=[
                cap_ctx.linkid_col,
                cap_ctx.from_sta_col,
                cap_ctx.to_sta_col,
                cap_ctx.dir_col
            ]
        )

        final_vcr = vcr.CalculateVCR()

        final_vcr_ctx = final_vcr.execute(vcr_ctx)

        final_vcr_ctx.lf.collect().write_parquet(
            f'{SCRATCH_FOLDER}/vcr_2_2024.parquet'
        )
        
        self.assertTrue(final_vcr_ctx.row_count > 0)
    
    def test_vcr_summary(self):
        ctx = vcr.PipelineContext(
            lf=pl.scan_parquet(f'{SCRATCH_FOLDER}/vcr_2020.parquet')
        )

        summary = vcr.CalculateVCRSummary()
        out_ctx = summary.execute(ctx)

        df = out_ctx.lf.collect()

        self.assertFalse(df.is_empty())

        df.write_csv('C:/Users/hazin/Downloads/vcr_2020_summary.csv')
    
    def test_latest_vcr_mean(self):
        lfs:List[pl.LazyFrame] = []

        for year in [2024, 2022, 2021, 2020, 2019, 2023]:
            if year == 2019:
                _year = 2021
                vcr_lf = pl.scan_parquet(
                    f'{SCRATCH_FOLDER}/vcr_{year}.parquet'
                )

            elif year == 2023:
                _year = 2024
                vcr_lf = pl.scan_parquet(
                    f'{SCRATCH_FOLDER}/vcr_{year}.parquet'
                ).with_columns(
                    pl.col('^VEH.*$').truediv(
                        pl.sum_horizontal(
                            pl.col('^VEH.*$')
                        ).truediv(
                            pl.col('TOTAL_PCE')
                        )
                    ).name.prefix('PCE_')
                )

            else:
                _year = year
                vcr_lf = pl.scan_parquet(
                    f'{SCRATCH_FOLDER}/vcr_{year}.parquet'
                )

            ctx = vcr.MultiDataContext().add_dataset(
                name='VCR',
                lazyframe=vcr_lf,
                join_key=['LINKID']
            ).add_dataset(
                name='RNI',
                lazyframe=pl.scan_parquet(
                    f'{SCRATCH_FOLDER}/rni_2_{_year}.parquet'
                ),
                join_key=['LINKID']
            )

            summary = vcr.CalculateVCRSummary(agg_method='mean', level='route')
            out_ctx = summary.execute(ctx)
            temp_file = f'C:/Users/hazin/Projects/temp_{year}.parquet'
            out_ctx.lf.collect().write_parquet(temp_file)

            lf_ = pl.scan_parquet(temp_file)

            lfs.append(lf_)
        
        vcr_ = pl.concat(lfs)

        vcr_latest = vcr_.group_by(
            'LINKID'
        ).agg(  
            pl.all().sort_by('YEAR').last()
        )

        vcr_latest.with_columns(
            pl.col('YEAR').cast(
                pl.String()
            ).replace(
                '0', 
                'Default'
            )
        ).collect(
            streaming=True
        ).write_csv(f'{SCRATCH_FOLDER}/vcr_latest_mean.csv')

        self.assertTrue(True)    

    def test_latest_vcr_max(self):
        lfs:List[pl.LazyFrame] = []

        for year in [2024, 2022, 2021, 2020, 2019, 2023]:
            if year == 2019:
                _year = 2021
                vcr_lf = pl.scan_parquet(
                    f'{SCRATCH_FOLDER}/vcr_{year}.parquet'
                )

            elif year == 2023:
                _year = 2024
                vcr_lf = pl.scan_parquet(
                    f'{SCRATCH_FOLDER}/vcr_{year}.parquet'
                ).with_columns(
                    pl.col('^VEH.*$').truediv(
                        pl.sum_horizontal(
                            pl.col('^VEH.*$')
                        ).truediv(
                            pl.col('TOTAL_PCE')
                        )
                    ).name.prefix('PCE_')
                )

            else:
                _year = year
                vcr_lf = pl.scan_parquet(
                    f'{SCRATCH_FOLDER}/vcr_{year}.parquet'
                )

            ctx = vcr.MultiDataContext().add_dataset(
                name='VCR',
                lazyframe=vcr_lf,
                join_key=['LINKID']
            ).add_dataset(
                name='RNI',
                lazyframe=pl.scan_parquet(
                    f'{SCRATCH_FOLDER}/rni_2_{_year}.parquet'
                ),
                join_key=['LINKID']
            )

            summary = vcr.CalculateVCRSummary(agg_method='max', level='route')
            out_ctx = summary.execute(ctx)
            temp_file = f'C:/Users/hazin/Projects/temp_{year}.parquet'
            out_ctx.lf.collect().write_parquet(temp_file)

            lf_ = pl.scan_parquet(temp_file)

            lfs.append(lf_)
        
        vcr_ = pl.concat(lfs)

        vcr_latest = vcr_.group_by(
            'LINKID'
        ).agg(  
            pl.all().sort_by('YEAR').last()
        )

        vcr_latest.with_columns(
            pl.col('YEAR').cast(
                pl.String()
            ).replace(
                '0', 
                'Default'
            )
        ).collect(
            streaming=True
        ).write_csv(f'{SCRATCH_FOLDER}/vcr_latest_max.csv')

        self.assertTrue(True)

    def test_segment_summary(self):
        lfs:List[pl.LazyFrame] = []

        for year in [2024, 2022, 2021, 2020, 2019, 2023]:
            if year == 2019:
                _year = 2021
                vcr_lf = pl.scan_parquet(
                    f'{SCRATCH_FOLDER}/vcr_{year}.parquet'
                )

            elif year == 2023:
                _year = 2024
                vcr_lf = pl.scan_parquet(
                    f'{SCRATCH_FOLDER}/vcr_{year}.parquet'
                ).with_columns(
                    pl.col('^VEH.*$').truediv(
                        pl.sum_horizontal(
                            pl.col('^VEH.*$')
                        ).truediv(
                            pl.col('TOTAL_PCE')
                        )
                    ).name.prefix('PCE_')
                )

            else:
                _year = year
                vcr_lf = pl.scan_parquet(
                    f'{SCRATCH_FOLDER}/vcr_{year}.parquet'
                )

            ctx = vcr.MultiDataContext().add_dataset(
                name='VCR',
                lazyframe=vcr_lf,
                join_key=['LINKID']
            ).add_dataset(
                name='RNI',
                lazyframe=pl.scan_parquet(
                    f'{SCRATCH_FOLDER}/rni_2_{_year}.parquet'
                ),
                join_key=['LINKID']
            )

            summary = vcr.CalculateVCRSummary(
                agg_method='mean', level='segment'
            )
            out_ctx = summary.execute(ctx)
            temp_file = f'{SCRATCH_FOLDER}/temp_{year}.parquet'
            out_ctx.lf.collect().write_parquet(temp_file)

            lf_ = pl.scan_parquet(temp_file)

            lfs.append(lf_)
        
        vcr_ = pl.concat(lfs)

        vcr_latest = vcr_.group_by(
            out_ctx.linkid_col,
            out_ctx.from_sta_col,
            out_ctx.to_sta_col
        ).agg(  
            pl.all().sort_by('YEAR').last()
        )

        vcr_latest.with_columns(
            pl.col('YEAR').cast(
                pl.String()
            ).replace(
                '0', 
                'Default'
            )
        ).collect(
            streaming=True
        ).write_parquet(
            f'{SCRATCH_FOLDER}/vcr_segment_latest.parquet',
        )

        self.assertTrue(True)

    def test_load_segment_to_db(self):
        """
        Calculate VCR segment summary and load the result to database
        """

        for year in [2024, 2022, 2021, 2020, 2019, 2023]:
            if year == 2019:
                _year = 2021
                vcr_lf = pl.scan_parquet(
                    f'{SCRATCH_FOLDER}/vcr_{year}.parquet'
                )

            elif year == 2023:
                _year = 2024
                vcr_lf = pl.scan_parquet(
                    f'{SCRATCH_FOLDER}/vcr_{year}.parquet'
                ).with_columns(
                    pl.col('^VEH.*$').truediv(
                        pl.sum_horizontal(
                            pl.col('^VEH.*$')
                        ).truediv(
                            pl.col('TOTAL_PCE')
                        )
                    ).name.prefix('PCE_')
                )

            else:
                _year = year
                vcr_lf = pl.scan_parquet(
                    f'{SCRATCH_FOLDER}/vcr_{year}.parquet'
                )

            ctx = vcr.MultiDataContext().add_dataset(
                name='VCR',
                lazyframe=vcr_lf,
                join_key=['LINKID']
            ).add_dataset(
                name='RNI',
                lazyframe=pl.scan_parquet(
                    f'{SCRATCH_FOLDER}/rni_2_{_year}.parquet'
                ),
                join_key=['LINKID']
            )

            summary = vcr.CalculateVCRSummary(
                agg_method='mean', level='segment'
            )

            out_ctx = summary.execute(ctx)
            temp_file = f'{SCRATCH_FOLDER}/temp_{year}.parquet'
            out_ctx.lf.collect().write_parquet(temp_file)

            out_ctx.lf = pl.scan_parquet(temp_file)

            loader = vcr.SegmentVCRLoader(
                sql_engine=engine,
                table_name='VCR_PKJI_SEGMENT'
            )

            loader.execute(out_ctx)

        self.assertTrue(True)
