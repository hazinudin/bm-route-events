import unittest
from src.service.bridge.inventory_validation import BridgeInventoryValidation
from sqlalchemy import create_engine
import json
from dotenv import load_dotenv
import os


load_dotenv("tests/dev.env")
HOST = os.getenv("GDB_HOST")
USER = os.getenv("MISC_USER")
PWD = os.getenv("MISC_PWD")

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")


class TestBridgeInventoryValidationExcelPayloads(unittest.TestCase):
    def test_sups_update_payload_1(self):
        with open(
            "tests/service/bridge_inventory_validation/test_excel_payload_1.json"
        ) as jf:
            input_dict = json.load(jf)

        check = BridgeInventoryValidation(
            data=input_dict,
            validation_mode="UPDATE",
            lrs_grpc_host="localhost:50052",
            sql_engine=engine,
            dev=False,
            sups_only=True,
        )

        check.sups_only_update_check()

        self.assertTrue(True)

    def test_sups_update_payload_2(self):
        with open(
            "tests/service/bridge_inventory_validation/test_excel_payload_2.json"
        ) as jf:
            input_dict = json.load(jf)

        check = BridgeInventoryValidation(
            data=input_dict,
            validation_mode="UPDATE",
            lrs_grpc_host="localhost:50052",
            sql_engine=engine,
            dev=False,
            sups_only=True,
        )

        check.sups_only_update_check()

        self.assertTrue(True)

    def test_sups_update_payload_3(self):
        with open(
            "tests/service/bridge_inventory_validation/test_excel_payload_3.json"
        ) as jf:
            input_dict = json.load(jf)

        check = BridgeInventoryValidation(
            data=input_dict,
            validation_mode="UPDATE",
            lrs_grpc_host="localhost:50052",
            sql_engine=engine,
            dev=False,
            sups_only=True,
        )

        check.sups_only_update_check()

        self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()
