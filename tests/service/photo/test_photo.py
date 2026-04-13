from src.service.photo.client import SurveyPhotoStorage
from src.route_events.photo import SurveyPhoto
import unittest
from unittest.mock import MagicMock
from bm_photo_client import BMPhotoClient


class TestSurveyPhotoStorage(unittest.TestCase):
    def test_validate_photos_url_returns_invalid(self):
        mock_client = MagicMock(spec=BMPhotoClient)
        summaries = [
            MagicMock(photo_id="photo-001"),
            MagicMock(photo_id="photo-002"),
        ]
        mock_client.browse_photos.return_value = MagicMock(photos=summaries)

        sp = SurveyPhotoStorage(
            photo_client=mock_client, route_id="010362", survey_year=2025
        )

        photos = [
            SurveyPhoto(
                photo_id="photo-001",
                url="",
                sta_meters=100,
                survey_year=2025,
                linkid="010362",
                latitude=0,
                longitude=0,
            ),
            SurveyPhoto(
                photo_id="photo-002",
                url="",
                sta_meters=200,
                survey_year=2025,
                linkid="010362",
                latitude=0,
                longitude=0,
            ),
            SurveyPhoto(
                photo_id="photo-999",
                url="",
                sta_meters=300,
                survey_year=2025,
                linkid="010362",
                latitude=0,
                longitude=0,
            ),
        ]

        invalid = sp.validate_photos_url(photos, return_invalid=True)

        self.assertEqual(len(invalid), 1)
        self.assertEqual(invalid[0].photo_id, "photo-999")

    def test_validate_photos_url_returns_valid(self):
        mock_client = MagicMock(spec=BMPhotoClient)
        summaries = [
            MagicMock(photo_id="photo-001"),
            MagicMock(photo_id="photo-002"),
        ]
        mock_client.browse_photos.return_value = MagicMock(photos=summaries)

        sp = SurveyPhotoStorage(
            photo_client=mock_client, route_id="010362", survey_year=2025
        )

        photos = [
            SurveyPhoto(
                photo_id="photo-001",
                url="",
                sta_meters=100,
                survey_year=2025,
                linkid="010362",
                latitude=0,
                longitude=0,
            ),
            SurveyPhoto(
                photo_id="photo-002",
                url="",
                sta_meters=200,
                survey_year=2025,
                linkid="010362",
                latitude=0,
                longitude=0,
            ),
            SurveyPhoto(
                photo_id="photo-999",
                url="",
                sta_meters=300,
                survey_year=2025,
                linkid="010362",
                latitude=0,
                longitude=0,
            ),
        ]

        valid = sp.validate_photos_url(photos, return_invalid=False)

        self.assertEqual(len(valid), 2)
        self.assertEqual(valid[0].photo_id, "photo-001")
        self.assertEqual(valid[1].photo_id, "photo-002")
