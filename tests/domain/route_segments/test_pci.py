import unittest
from src.route_events.segments.pci import RoutePCI, RoutePCIRepo
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('SMD_USER')
PWD = os.getenv('SMD_PWD')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")


class TestPCI(unittest.TestCase):
    def test_from_excel(self):
        """
        Load data from Excel file with correct schema.
        """
        excel_path = "tests/domain/route_segments/input_excels/pci_1_01001.xlsx"

        pci = RoutePCI.from_excel(
            excel_path=excel_path,
            linkid = '01001',
            ignore_review=True
        )

        self.assertFalse(pci.pl_df.is_empty())
    
    def test_repo(self):
        """
        Test RoutePCI repository.
        """
        repo = RoutePCIRepo(engine)

        pci = repo.get_by_linkid('01001', 2024)

        self.assertFalse(pci.pl_df.is_empty())

    def test_invalid_pci_value(self):
        """
        Test RoutePCI invvalid_pci segment.
        """
        repo = RoutePCIRepo(engine)

        pci = repo.get_by_linkid('01001', 2024)

        pci.invalid_pci_value()

        self.assertTrue(True)        
