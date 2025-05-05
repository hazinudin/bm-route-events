from typing import List
from route_events.photo import SurveyPhoto, SurveyPhotoRepo
from google.cloud import storage
from sqlalchemy import Engine
import re
    

class SurveyPhotoStorage(object):
    """
    Survey photo google cloud storage utility tools.
    """
    def __init__(
            self,
            gs_client: storage.Client, 
            bucket_name: str,
            sql_engine: Engine
    ):
        self.gs_client = gs_client
        self.bucket_name = bucket_name
        self._root_url = f"https://storage.googleapis.com/{bucket_name}/"
        self._root_url_re = rf"https://storage\.googleapis\.com/{bucket_name}/road_defect_photos"
        self._objects_name = None
        self._repo = SurveyPhotoRepo(sql_engine)

    @property
    def root_url(self) -> str:
        return self._root_url
    
    @property
    def objects_name(self) -> list:
        if self._objects_name is None:
            blobs = self.gs_client.list_blobs(self.bucket_name)
            self._objects_name = []

            for blob in blobs:
                self._objects_name.append(blob.name)

            return self._objects_name
        else:
            return self._objects_name
       
    def validate_photos_url(
            self, 
            photos: List[SurveyPhoto], 
            return_invalid=True
    ) -> List[SurveyPhoto]:
        """
        Validate the URL from a list of SurveyPhoto objects. Return lists of SurveyPhoto with invalid URL.
        """
        invalid = list()
        valid = list()
        bucket =  self.gs_client.bucket(self.bucket_name)

        for photo in photos:
            url_re = rf"{self._root_url_re}/{photo.survey_year}/[\w\-./?=&]*$"
            
            # None meaning the string does not match the regex
            if re.match(url_re, photo.url) is None:  
                invalid.append(photo)
                continue

            if not bucket.blob(photo.url.split(self._root_url)[1]).exists():
                invalid.append(photo)
            else:
                valid.append(photo)
        
        if return_invalid:
            return invalid
        else:
            return valid
    
    def register_photos(
            self, 
            photos: List[SurveyPhoto], 
            validate: bool = True
    ):
        """
        Register the photo along with its attribute to database.
        """
        if validate:
            photos = self.validate_photos_url(photos, return_invalid=False)

        with self._repo.session() as session:
            self._repo.delete(photos, session, commit=False)
            self._repo.insert(photos, session, commit=False)

            session.commit()
        
        return