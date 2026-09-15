import unittest
from src.service.segments.analysis.join import segments_join, CompareRNISegments, segments_coverage_join
from route_events import RouteSegmentEvents, RouteRNI, RouteRoughness, RoutePCI
from src.route_events.segments.pok import RoutePOKRepo, RoutePOK
import polars as pl
from sqlalchemy import create_engine
import json
import os
import shutil
import tempfile
from dotenv import load_dotenv


load_dotenv('tests/dev.env')
HOST = os.getenv('GDB_HOST')
USER = os.getenv('SMD_USER')
PWD = os.getenv('SMD_PWD')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")


class TestSegmentsCoverageJoin(unittest.TestCase):
    """
    PCI covering against RNI target on the same linkid (44058).
    The original PCI excel misses newer schema damage columns, so it is
    patched with null columns before loading.
    """
    PCI_EXCEL = 'tests/domain/route_segments/input_excels/pci_10_01-08-2025_022606_9382.xlsx'
    RNI_EXCEL = 'tests/domain/route_segments/input_excels/rni_10_21-08-2024_040521_7471.xlsx'
    LINKID = '44058'

    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.mkdtemp()
        cls.pci_excel = os.path.join(cls._tmpdir, 'pci_10_44058_patched.xlsx')

        with open('src/route_events/segments/pci/schema.json') as jf:
            schema = json.load(jf)['column_details']

        df = pl.read_excel(
            cls.PCI_EXCEL,
            engine='calamine',
            read_options={'dtypes': 'string'}
        ).rename(str.upper)

        missing = [
            col for col in schema
            if not schema[col].get('skip') and col not in df.columns
        ]

        patched = df.with_columns(
            [pl.lit(None).alias(col) for col in missing]
        )
        patched.write_excel(cls.pci_excel)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls._tmpdir)

    def _load(self):
        pci = RoutePCI.from_excel(
            self.pci_excel,
            linkid=self.LINKID,
            ignore_review=True
        )

        rni = RouteRNI.from_excel(
            self.RNI_EXCEL,
            linkid=self.LINKID,
            ignore_review=True
        )

        return pci, rni

    def test_join_target_agg(self):
        pci, rni = self._load()

        joined = segments_coverage_join(
            covering=pci,
            target=rni,
            target_select=['SURF_TYPE'],
            target_agg = [pl.col('SURF_TYPE').max()]
        )

        self.assertFalse(joined.is_empty())

    def test_join_target_no_agg(self):
        pci, rni = self._load()

        joined = segments_coverage_join(
            covering=pci,
            target=rni,
            target_select=['SURF_TYPE']
        )

        self.assertFalse(joined.is_empty())


class TestSegmentsJoin(unittest.TestCase):
    def test_join_agg(self):
        events = RouteSegmentEvents.from_excel(
            'tests/domain/route_segments/input_excels/balai_5_15010.xlsx',
            config_path='tests/domain/route_segments/input_config/rni_config.json',
            linkid='15010',
            ignore_review=True
        )

        joined = segments_join(
            left=events,
            right=events,
            l_select=['SURF_TYPE'],
            r_select=['LANE_WIDTH'],
            l_agg=[pl.col('SURF_TYPE').max()],
            r_agg=[pl.col('LANE_WIDTH').sum()]
        )

        self.assertTrue(joined.shape[0] == 924)

    def test_rni_segments_comparison(self):
        events = RouteRNI.from_excel(
            'tests/domain/route_segments/input_excels/balai_5_15010.xlsx',
            linkid='15010',
            ignore_review=True
        )

        comp = CompareRNISegments(
            rni=events,
            other=events
        )

        self.assertTrue(comp.rni_with_no_match().is_empty())

    def test_rni_segments_comparison_mismatch(self):
        rni = RouteRNI.from_excel(
            'tests/domain/route_segments/input_excels/balai_5_15010.xlsx',
            linkid='15010',
            ignore_review=True
        )

        other = RouteRoughness.from_excel(
            'tests/domain/route_segments/input_excels/iri_1_010362.xlsx',
            linkid='010362',
            ignore_review=True
        )

        comp = CompareRNISegments(
            rni=rni,
            other=other
        )

        self.assertFalse(comp.rni_with_no_match().is_empty())
        self.assertFalse(comp.other_with_no_match().is_empty())


class TestPCIRNISegmentsComparison(unittest.TestCase):
    """
    PCI vs RNI on the same linkid (06036), balai 3.
    PCI is a dynamic-length (coverage) event, RNI is fixed-length,
    so the coverage (STA-overlap) join is used.
    """
    PCI_EXCEL = 'tests/domain/route_segments/input_excels/pci_3_14-08-2026_063529_3497.xlsx'
    RNI_EXCEL = 'tests/domain/route_segments/input_excels/rni_3_30-07-2025_150436_5405.xlsx'
    LINKID = '06036'

    @classmethod
    def setUpClass(cls):
        cls.pci = RoutePCI.from_excel(
            cls.PCI_EXCEL,
            linkid=cls.LINKID,
            ignore_review=True
        )

        cls.rni = RouteRNI.from_excel(
            cls.RNI_EXCEL,
            linkid=cls.LINKID,
            ignore_review=True
        )

    def test_pci_rni_segments_comparison(self):
        comp = CompareRNISegments(
            rni=self.rni,
            other=self.pci
        )

        rni_no_match = comp.rni_with_no_match()
        other_no_match = comp.other_with_no_match()

        self.assertTrue(rni_no_match.is_empty())
        self.assertFalse(other_no_match.is_empty())
