from src.service.photo import gs
from src.route_events.photo import SurveyPhoto
import unittest
from sqlalchemy import create_engine
import os
from google.cloud import storage
from dotenv import load_dotenv


photos = [
            SurveyPhoto(
                url= "https://storage.googleapis.com/sidako-bucket/road_defect_photos/2025/dev/01001/STA_430_L1.jpeg",
                sta_meters= 100,
                survey_year=2025,
                linkid='01001',
                latitude=0,
                longitude=0
            ),
            SurveyPhoto(
                url= "https://storage.googleapis.com/sidako-bucket/road_defect_photos/2025/dev/01001/STA_440_L1.jpg",
                sta_meters= 100,
                survey_year=2025,
                linkid='01001',
                latitude=0,
                longitude=0
            ),
            SurveyPhoto(
                url= "https://storage.googleapis.com/sidako-bucket/road_defect_photos/2025/dev/01001/STA_430_L2.jpeg",
                sta_meters= 100,
                survey_year=2025,
                linkid='01001',
                latitude=0,
                longitude=0
            ),
]
        

load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('SMD_USER')
PWD = os.getenv('SMD_PWD')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")

class TestSurveyPhotoStorage(unittest.TestCase):
    def test_list_all_blobs(self):
        client = storage.Client()
        sp = gs.SurveyPhotoStorage(bucket_name='sidako-bucket', sql_engine=engine, gs_client=client)

        self.assertTrue(len(sp.objects_name) != 0)

    def test_validate_photos_url(self):
        client = storage.Client()
        sp = gs.SurveyPhotoStorage(bucket_name='sidako-bucket', sql_engine=engine, gs_client=client)

        self.assertTrue(len(sp.validate_photos_url(photos)) == 1)

    def test_register_photo_url(self):
        client = storage.Client()
        sp = gs.SurveyPhotoStorage(bucket_name='sidako-bucket', sql_engine=engine, gs_client=client)
        sp.register_photos(photos=photos, validate=True)
