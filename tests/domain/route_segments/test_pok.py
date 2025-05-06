import unittest
from src.route_events.segments.pok import RoutePOK, RoutePOKRepo
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('SMD_USER')
PWD = os.getenv('SMD_PWD')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")


class TestRoutePOKRepo(unittest.TestCase):
    def test_get_by_comp_name(self):
        """
        Query POK using the comp name, budget year and route id.
        """
        repo = RoutePOKRepo(engine)

        pok = repo.get_by_comp_name(
            linkid='01001', 
            comp_name_keywords=['Pemeliharaan'], 
            budget_year=2024
        )

        self.assertFalse(pok.pl_df.is_empty())
        self.assertTrue(pok.sta_conversion == 1000)
