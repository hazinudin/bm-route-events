import unittest
from src.route_events.photo.repo import SurveyPhotoRepo
from src.route_events.photo.photo import SurveyPhoto
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine


load_dotenv('tests/dev.env')
HOST = os.getenv('DB_HOST')
USER = os.getenv('SMD_USER')
PWD = os.getenv('SMD_PWD')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")

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


class TestSurveyPhotoRepo(unittest.TestCase):
    def test_create_table(self):
        repo = SurveyPhotoRepo(engine)
        repo._create_table()
        self.assertTrue(True)

    def test_insert_photo(self):
        repo = SurveyPhotoRepo(engine)
        
        with repo.session() as session:
            repo.insert(photos, session=session, commit=True)

    def test_delete_photo(self):
        repo = SurveyPhotoRepo(engine)

        with repo.session() as session:
            repo.delete(photos, session=session, commit=True)
