from typing import List
from route_events.photo import SurveyPhoto
from bm_photo_client import BMPhotoClient
from bm_photo_client._pagination import auto_paginate


class SurveyPhotoStorage(object):
    """
    Survey photo storage backed by the bm-photo service.

    Validates photo IDs against the bm-photo API and provides
    batch lookup of valid photo IDs for a given route and survey year.
    """

    def __init__(self, photo_client: BMPhotoClient, route_id: str, survey_year: int):
        self._client = photo_client
        self._route_id = route_id
        self._survey_year = survey_year
        self._valid_photo_ids: set = None

    @property
    def valid_photo_ids(self) -> set:
        """
        Lazily fetch and cache all valid photo IDs for the configured
        route and survey year from the bm-photo service.
        """
        if self._valid_photo_ids is None:
            summaries = auto_paginate(
                self._client.browse_photos,
                route_id=self._route_id,
                survey_year=self._survey_year,
            )
            self._valid_photo_ids = {s.photo_id for s in summaries}

        return self._valid_photo_ids

    def validate_photos_url(
        self, photos: List[SurveyPhoto], return_invalid=True
    ) -> List[SurveyPhoto]:
        """
        Validate photo IDs against the bm-photo service.

        Returns a list of invalid SurveyPhoto objects by default.
        Set return_invalid=False to return valid photos instead.
        """
        invalid = list()
        valid = list()
        valid_ids = self.valid_photo_ids

        for photo in photos:
            if photo.photo_id in valid_ids:
                valid.append(photo)
            else:
                invalid.append(photo)

        if return_invalid:
            return invalid
        else:
            return valid

    def update_photos(self, photos: List[SurveyPhoto]) -> None:
        """
        Update photo metadata (latitude, longitude, sta_value) in the bm-photo service
        for all valid photos in the list.

        Only updates photos whose photo_id exists in the bm-photo service.
        Photos not found in the service are silently skipped.

        Args:
            photos: List of SurveyPhoto objects containing photo_id and updated attributes.
        """
        valid_ids = self.valid_photo_ids

        for photo in photos:
            if photo.photo_id not in valid_ids:
                continue

            self._client.update_photo(
                photo_id=photo.photo_id,
                latitude=photo.latitude,
                longitude=photo.longitude,
                sta_value=photo.sta_meters,
            )
