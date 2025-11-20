from pydantic import BaseModel, Field, ConfigDict
from route_events import LRSRoute
from route_events_service import (
    RouteRNIValidation, 
    RouteRoughnessValidation, 
    RoutePCIValidation,
    RouteDefectsValidation,
    BridgeInventoryValidation,
    RouteRTCValidation,
    BridgeMasterValidation
)
from route_events_service.photo import gs
from typing import List, Optional, Literal
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from abc import ABC, abstractmethod
from google.cloud import storage
from google.oauth2 import service_account

from opentelemetry import trace
from opentelemetry.sdk.trace import StatusCode, Status


# Pydantic model
class PayloadSMD(BaseModel):
    file_name: str
    balai: str
    year: int
    semester: Optional[Literal[1,2]]
    routes: List[str]
    show_all_msg: Optional[bool] = False

class BridgeValidationParams(BaseModel):
    validate_length: Optional[bool] = Field(default=True, validation_alias="length_validate")
    validate_width: Optional[bool] = Field(default=True, validation_alias="width_validate")

class BridgeValidationPayloadFormat(BaseModel):
    model_config = ConfigDict(extra='allow')
    validation_params: Optional[BridgeValidationParams] = Field(default=BridgeValidationParams(), exclude=True)


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

tracer = trace.get_tracer(__name__)


class ValidationHandler(ABC):
    def __init__(
            self,
            payload: BridgeValidationPayloadFormat | PayloadSMD | dict,
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
    

class RNIValidation(ValidationHandler):
    def __init__(
            self,
            payload: PayloadSMD,
            job_id: str,
            validate: bool=True
    ):
        ValidationHandler.__init__(self, payload, job_id, validate)

    def validate(self)->str:
        """
        Start validation
        """
        with tracer.start_as_current_span('rni-validation-process') as span:
            check = RouteRNIValidation.validate_excel(
                excel_path=self.payload.file_name,
                route=self.payload.routes[0],
                survey_year=self.payload.year,
                sql_engine=SMD_ENGINE,
                lrs=self.get_lrs(),
                ignore_review=self.ignore_review,
                force_write=self.force_write
            )

            span.set_attribute("partial_update", check._events.is_partial)

            if check.get_status() == 'rejected':
                return check._result.to_job_event(self.job_id)

            if self._validate:
                check.base_validation()

            if (check.get_status() == 'verified') and (WRITE_VERIFIED_DATA):
                if check._events.is_partial:  # If its still partial, then merge with previous data
                    check.merge_previous_data()

                check.put_data(semester=self.payload.semester)

            # Set span attribute and status
            span.set_attribute("file_name", self.payload.file_name)
            span.set_attribute("route", self.payload.routes[0])
            span.set_attribute("validation.result.status", check.get_status())

            span.set_status(StatusCode.OK)
            
            return check._result.to_job_event(self.job_id)
    

class IRIValidation(ValidationHandler):
    def __init__(
            self,
            payload: PayloadSMD,
            job_id: str,
            validate: bool=True
    ):
        ValidationHandler.__init__(self, payload, job_id, validate)

    def validate(self)->str:
        """
        Start validation
        """
        with tracer.start_as_current_span('iri-validation-process') as span:        
            check = RouteRoughnessValidation.validate_excel(
                excel_path=self.payload.file_name,
                route=self.payload.routes[0],
                survey_year=self.payload.year,
                survey_semester=self.payload.semester,
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
                check.put_data()

            # Set span attribute and status
            span.set_attribute("file_name", self.payload.file_name)
            span.set_attribute("route", self.payload.routes[0])
            span.set_attribute("validation.result.status", check.get_status())

            span.set_status(StatusCode.OK)

            return check._result.to_job_event(self.job_id)
    

class PCIValidation(ValidationHandler):
    def __init__(
            self,
            payload: PayloadSMD,
            job_id: str,
            validate: bool=True
    ):
        ValidationHandler.__init__(self, payload, job_id, validate)

    def validate(self)->str:
        """
        Start validation
        """
        with tracer.start_as_current_span('pci-validation-process') as span:        
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

            # Set span attribute and status
            span.set_attribute("file_name", self.payload.file_name)
            span.set_attribute("route", self.payload.routes[0])
            span.set_attribute("validation.result.status", check.get_status())

            span.set_status(StatusCode.OK)

            return check._result.to_job_event(self.job_id)


class RTCValidation(ValidationHandler):
    def __init__(
            self,
            payload: PayloadSMD,
            job_id: str,
            validate: bool=True
    ):
        ValidationHandler.__init__(self, payload, job_id, validate)

    def validate(self)->str:
        """
        Start validation.
        """
        with tracer.start_as_current_span('rtc-validation-process') as span:
            check = RouteRTCValidation.validate_excel(
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
                check.put_data()
            
            ## Set span attributes and status
            span.set_attribute('file_name', self.payload.file_name)
            span.set_attribute('route', self.payload.routes[0])
            span.set_attribute('validation.result.status', check.get_status())

            return check._result.to_job_event(self.job_id)


class DefectValidation(ValidationHandler):
    def __init__(
            self,
            payload: PayloadSMD,
            job_id: str,
            validate: bool=True
    ):
        ValidationHandler.__init__(self, payload, job_id, validate)

        # Google Cloud Storage client
        self.cred = service_account.Credentials.from_service_account_file(os.path.dirname(__file__) + '/' + SERVICE_ACCOUNT_JSON)

    def validate(self)->str:
        """
        Start validation
        """
        with tracer.start_as_current_span('defect-validation-process') as span:        
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

            # Set span attribute and status
            span.set_attribute("file_name", self.payload.file_name)
            span.set_attribute("route", self.payload.routes[0])
            span.set_attribute("validation.result.status", check.get_status())

            span.set_status(StatusCode.OK)

            return check._result.to_job_event(self.job_id)
        

class BridgeMasterValidation_(ValidationHandler):
    def __init__(
            self,
            payload: BridgeValidationPayloadFormat,
            job_id: str,
            validate: bool=True,
    ):
        ValidationHandler.__init__(self, payload, job_id, validate)
        self.payload: BridgeValidationPayloadFormat
    
    def validate(self) -> str:
        """
        Start validation
        """
        with tracer.start_as_current_span('bridge.master-validation-process') as span:
            val_mode = self.payload.model_dump().get('mode')

            if val_mode is None:
                span.set_status(StatusCode.ERROR)
                raise KeyError("Input JSON does not contain 'mode'.")
            else:
                span.set_attribute("validation.mode", val_mode)

            check = BridgeMasterValidation(
                data=self.payload.model_dump(),
                validation_mode=str(val_mode).upper(),
                lrs_grpc_host=LRS_HOST,
                sql_engine=MISC_ENGINE,
                ignore_review=self.ignore_review,
                ignore_force=self.ignore_review
            )

            if check.get_status() in ['rejected', 'error']:
                return check._result.to_job_event(self.job_id)

            if self.validate:            
                if check.validation_mode == 'UPDATE':
                    check.update_check()

                if check.validation_mode == 'INSERT':
                    check.insert_check()

            if check.get_status() == 'verified':
                check.put_data()

            span.set_attribute("validation.result.status", check.get_status())
            span.set_status(StatusCode.OK)

        return check._result.to_job_event(self.job_id)

class BridgeInventoryValidation_(ValidationHandler):
    def __init__(
            self,
            payload: BridgeValidationPayloadFormat,
            job_id: str,
            validate: bool=True,
            popup: bool=False,
    ):
        ValidationHandler.__init__(self, payload, job_id, validate)
        self.payload: BridgeValidationPayloadFormat
        self._is_popup: bool = popup

    def validate(self) -> str:
        """
        Start validation
        """
        with tracer.start_as_current_span('bridge.inventory-validation-process') as span:
            val_mode = self.payload.model_dump().get('mode')

            if val_mode is None:
                span.set_status(StatusCode.ERROR)
                raise KeyError("Input JSON does not contain 'mode'.")
            else:
                span.set_attribute("validation.mode", val_mode)

            span.set_attribute("validation.length", self.payload.validation_params.validate_length)
            span.set_attribute("validation.width", self.payload.validation_params.validate_width)
            
            check = BridgeInventoryValidation(
                data=self.payload.model_dump(),
                validation_mode=str(val_mode).upper(),
                lrs_grpc_host=LRS_HOST,
                sql_engine=MISC_ENGINE,
                dev=False,
                popup=self._is_popup,
                ignore_review=self.ignore_review,
                ignore_force=self.force_write,
            )

            if self.validate:
                if check.get_status() == 'rejected':
                    span.set_attribute("validation.final_status", check.get_status())
                    return check._result.to_job_event(self.job_id)

                if check.validation_mode == 'UPDATE':
                    check.update_check()

                if check.validation_mode == 'INSERT':
                    check.insert_check(
                        validate_length=self.payload.validation_params.validate_length,
                        validate_width=self.payload.validation_params.validate_width
                    )
            
            if (
                check.get_status() == 'verified'
            ) and WRITE_VERIFIED_DATA and (
                self.payload.validation_params.validate_length
            ):
                check.merge_master_data()
                check.update_master_data()
            
            if check.get_status() == 'verified':    
                check.put_data()

            span.set_attribute("validation.result.status", check.get_status())
            span.set_status(StatusCode.OK)

        return check._result.to_job_event(self.job_id)


class BridgePopUpInventoryValidation(BridgeInventoryValidation_):
    def __init__(
        self,
        payload: BridgeValidationPayloadFormat,
        job_id: str,
        validate: bool=True,
    ):
        BridgeInventoryValidation_.__init__(self, payload, job_id, validate, popup=True)