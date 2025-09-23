from pydantic import BaseModel
from route_events import LRSRoute
from route_events_service import (
    RouteRNIValidation, 
    RouteRoughnessValidation, 
    RoutePCIValidation,
    RouteDefectsValidation
)
from route_events_service.photo import gs
from typing import List, Optional, Literal
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from abc import ABC, abstractmethod
from google.cloud import storage
from google.oauth2 import service_account


# Pydantic model
class PayloadSMD(BaseModel):
    file_name: str
    balai: str
    year: int
    semester: Optional[Literal[1,2]]
    routes: List[str]
    show_all_msg: Optional[bool] = False


load_dotenv(os.path.dirname(__file__) + '/.env')

DB_HOST = os.getenv('DB_HOST')
SMD_USER = os.getenv('SMD_USER')
SMD_PWD = os.getenv('SMD_PWD')

MISC_USER = os.getenv('MISC_USER')
MISC_PWD = os.getenv('MISC_PWD')

LRS_HOST = os.getenv('LRS_HOST')

SERVICE_ACCOUNT_JSON = os.getenv('SERVICE_ACCOUNT_JSON')

SMD_ENGINE = create_engine(f"oracle+oracledb://{SMD_USER}:{SMD_PWD}@{DB_HOST}:1521/geodbbm")
MISC_ENGINE = create_engine(f"oracle+oracledb://{MISC_USER}:{MISC_PWD}@{DB_HOST}:1521/geodbbm")

WRITE_VERIFIED_DATA = int(os.getenv('WRITE_VERIFIED_DATA'))


class SMDValidationHandler(ABC):
    def __init__(
            self,
            payload: PayloadSMD,
            job_id: str,
            validate: bool = True
    ):
        if validate:
            self.ignore_review=False
            self.force_write=False
        else:
            self.ignore_review=True
            self.force_write=True

        self.payload=payload
        self.job_id=job_id
        self._validate=validate

    def get_lrs(self) -> LRSRoute | None:
        """
        Get LRSRoute object from GRPC service.
        """
        return LRSRoute.from_feature_service(
            LRS_HOST,
            self.payload.routes[0]
        )
    
    @abstractmethod
    def validate(self)->str:
        pass
    

class RNIValidation(SMDValidationHandler):
    def __init__(
            self,
            payload: PayloadSMD,
            job_id: str,
            validate: bool=True
    ):
        SMDValidationHandler.__init__(self, payload, job_id, validate)

    def validate(self)->str:
        """
        Start validation
        """
        check = RouteRNIValidation.validate_excel(
            excel_path=self.payload.file_name,
            route=self.payload.routes[0],
            survey_year=self.payload.year,
            sql_engine=SMD_ENGINE,
            lrs=self.get_lrs(),
            ignore_review=self.ignore_review,
            force_write=self.force_write
        )

        if check.get_status() == 'rejected':
            return check._result.to_job_event(self.job_id)

        if self._validate:
            check.base_validation()

        if (check.get_status() == 'verified') and (WRITE_VERIFIED_DATA):
            check.put_data(semester=self.payload.semester)

        return check._result.to_job_event(self.job_id)
    

class IRIValidation(SMDValidationHandler):
    def __init__(
            self,
            payload: PayloadSMD,
            job_id: str,
            validate: bool=True
    ):
        SMDValidationHandler.__init__(self, payload, job_id, validate)

    def validate(self)->str:
        """
        Start validation
        """
        check = RouteRoughnessValidation.validate_excel(
            excel_path=self.payload.file_name,
            route=self.payload.routes[0],
            survey_year=self.payload.year,
            sql_engine=SMD_ENGINE,
            lrs=self.get_lrs(),
            ignore_review=self.ignore_review,
            force_write=self.force_write
        )

        if check.get_status() == 'rejected':
            return check._result.to_job_event(self.job_id)

        if self._validate:
            check.base_validation()

        if (check.get_status() == 'verified') and (WRITE_VERIFIED_DATA):
            check.put_data(semester=self.payload.semester)

        return check._result.to_job_event(self.job_id)
    

class PCIValidation(SMDValidationHandler):
    def __init__(
            self,
            payload: PayloadSMD,
            job_id: str,
            validate: bool=True
    ):
        SMDValidationHandler.__init__(self, payload, job_id, validate)

    def validate(self)->str:
        """
        Start validation
        """
        check = RoutePCIValidation.validate_excel(
            excel_path=self.payload.file_name,
            route=self.payload.routes[0],
            survey_year=self.payload.year,
            sql_engine=SMD_ENGINE,
            lrs=self.get_lrs(),
            ignore_review=self.ignore_review,
            force_write=self.force_write
        )

        if check.get_status() == 'rejected':
            return check._result.to_job_event(self.job_id)

        if self._validate:
            check.base_validation()

        if (check.get_status() == 'verified') and (WRITE_VERIFIED_DATA):
            check.put_data(semester=self.payload.semester)

        return check._result.to_job_event(self.job_id)


class DefectValidation(SMDValidationHandler):
    def __init__(
            self,
            payload: PayloadSMD,
            job_id: str,
            validate: bool=True
    ):
        SMDValidationHandler.__init__(self, payload, job_id, validate)

        # Google Cloud Storage client
        self.cred = service_account.Credentials.from_service_account_file(os.path.dirname(__file__) + '/' + SERVICE_ACCOUNT_JSON)

    def validate(self)->str:
        """
        Start validation
        """
        gs_client = storage.Client(credentials=self.cred)
        
        sp = gs.SurveyPhotoStorage(
            gs_client=gs_client,
            bucket_name='sidako-bucket',
            sql_engine=SMD_ENGINE
        )
        check = RouteDefectsValidation.validate_excel(
            excel_path=self.payload.file_name,
            route=self.payload.routes[0],
            survey_year=self.payload.year,
            sql_engine=SMD_ENGINE,
            lrs=self.get_lrs(),
            ignore_review=self.ignore_review,
            force_write=self.force_write,
            photo_storage=sp
        )

        if check.get_status() == 'rejected':
            return check._result.to_job_event(self.job_id)

        if self._validate:
            check.base_validation()

        if (check.get_status() == 'verified') and (WRITE_VERIFIED_DATA):
            check.put_data()
            check.put_photos()

        return check._result.to_job_event(self.job_id)
