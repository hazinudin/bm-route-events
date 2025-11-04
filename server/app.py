from ray import serve, init
from fastapi import FastAPI
from sqlalchemy import create_engine
from google.cloud import storage
from google.oauth2 import service_account
from pydantic import BaseModel, ConfigDict, Field
from route_events_service import (
    BridgeMasterValidation,
    BridgeInventoryValidation,
    RouteRNIValidation,
    RouteRoughnessValidation,
    RouteDefectsValidation,
    RoutePCIValidation,
)
from route_events_service.photo import gs
from route_events import LRSRoute
import json
import os
from dotenv import load_dotenv
from typing import Literal, Optional, List


init(address='auto')

app = FastAPI()

class BridgeValidationParams(BaseModel):
    validate_length: Optional[bool] = Field(default=False, validation_alias="length.validate")
    validate_width: Optional[bool] = Field(default=False, validation_alias="width.validate")

class BridgeValidationPayloadFormat(BaseModel):
    model_config = ConfigDict(extra='allow')
    validation_params: Optional[BridgeValidationParams] = BridgeValidationParams()

class BridgeValidationPayload(BaseModel):
    input_json: BridgeValidationPayloadFormat

class _Payload(BaseModel):
    file_name: str
    balai: str
    year: int
    semester: Optional[Literal[1,2]]
    routes: List[str]
    show_all_msg: Optional[bool] = False

class RoadSurveyValidationInput(BaseModel):
    input_json: _Payload

load_dotenv(os.path.dirname(__file__) + '/.env')

@serve.deployment(num_replicas=int(os.getenv('RAY_SERVE_NUM_REPLICAS')))
@serve.ingress(app)
class DataValidation:
    def __init__(self):
        load_dotenv(os.path.dirname(__file__) + '/.env')

        HOST = os.getenv('DB_HOST')
        SMD_USER = os.getenv('SMD_USER')
        SMD_PWD = os.getenv('SMD_PWD')

        MISC_USER = os.getenv('MISC_USER')
        MISC_PWD = os.getenv('MISC_PWD')

        LRS_HOST = os.getenv('LRS_HOST')

        SERVICE_ACCOUNT_JSON = os.getenv('GCLOUD_SERVICE_ACCOUNT_JSON')

        self.smd_engine = create_engine(f"oracle+oracledb://{SMD_USER}:{SMD_PWD}@{HOST}:1521/geodbbm")
        self.misc_engine = create_engine(f"oracle+oracledb://{MISC_USER}:{MISC_PWD}@{HOST}:1521/geodbbm")
        self.lrs_host = LRS_HOST
        
        # Google Storage client
        # Use service account JSON
        self.cred = service_account.Credentials.from_service_account_file(os.path.dirname(__file__) + '/' + SERVICE_ACCOUNT_JSON)
    
    @app.post('/bridge/master_validation')
    def validate_bridgemaster_data(
        self, 
        payload: BridgeValidationPayload,
        write: bool = False
    ):
        val_mode = payload.input_json.model_dump().get('mode')

        if "force" in payload.input_json.model_dump().get("val_history"):
            ignore_force = True
        else:
            ignore_force = False
        
        if "review" in payload.input_json.get("val_history"):
            ignore_review = True
        else:
            ignore_review = False

        if val_mode is None:
            return {"status": "Input JSON tidak memiliki MODE"}
        
        check = BridgeMasterValidation(
            data=payload.input_json,
            validation_mode=val_mode.upper(),
            lrs_grpc_host=self.lrs_host,
            sql_engine=self.misc_engine,
            ignore_force=ignore_force,
            ignore_review=ignore_review
        )

        if check.get_status() in ['rejected', 'error']:
            return check.invij_json_result(as_dict=True)

        if check.validation_mode == 'UPDATE':
            check.update_check()
        
        if check.validation_mode == 'INSERT':
            check.insert_check()

        if write and (check.get_status() == 'verified'):
            check.put_data()

        return check.invij_json_result(as_dict=True)
    
    @app.post('/bridge/inventory_validation')
    def validate_inventory_data(
        self, 
        payload: BridgeValidationPayload,
        popup: bool = False,
        write: bool = False
    ):
        val_mode = payload.input_json.get('mode')

        if "force" in payload.input_json.get("val_history"):
            ignore_force = True
        else:
            ignore_force = False
        
        if "review" in payload.input_json.get("val_history"):
            ignore_review = True
        else:
            ignore_review = False

        # For popup the params depends on the payload
        if popup:
            validate_length = payload.input_json.validation_params.validate_length
            validate_width = payload.input_json.validation_params.validate_width
        # For regular inventory the validation is always True
        else:
            validate_length = True
            validate_width = True

        if val_mode is None:
            return {"status": "Input JSON tidak memiliki MODE"}
        
        check = BridgeInventoryValidation(
            data=payload.input_json,
            lrs_grpc_host=self.lrs_host,
            validation_mode=val_mode.upper(),
            sql_engine=self.misc_engine,
            dev=False,
            popup=popup,
            ignore_review=ignore_review,
            ignore_force=ignore_force
        )

        if check.get_status() == 'rejected':
            return check.invij_json_result(as_dict=True)

        if check.validation_mode == 'UPDATE':
            check.update_check()

        if check.validation_mode == 'INSERT':
            check.insert_check(
                validate_length=validate_length,
                validate_width=validate_width
            )
        
        if write and (check.get_status() == 'verified'):
            check.put_data()

            # Merge and update the data
            check.merge_master_data()
            check.update_master_data()

        return check.invij_json_result(as_dict=True)

    @app.post('/road/rni/validation')
    def validate_rni(
        self,
        payload: RoadSurveyValidationInput,
        write: bool = False,
        ignore_force: bool = False,
        ignore_review: bool = False
    ):  
        lrs = LRSRoute.from_feature_service(
            self.lrs_host, 
            payload.input_json.routes[0]
        )

        check = RouteRNIValidation.validate_excel(
            excel_path=payload.input_json.file_name,
            route=payload.input_json.routes[0],
            survey_year=payload.input_json.year,
            sql_engine=self.smd_engine,
            lrs=lrs,
            ignore_review=ignore_review,
            force_write=ignore_force
        )

        if check.get_status() == 'rejected':
            return check.smd_output_msg(
                show_all_msg=payload.input_json.show_all_msg,
                as_dict=True
            )

        check.base_validation()

        if write and (check.get_status() == 'verified'):
            check.put_data(semester=payload.input_json.semester)

        return check.smd_output_msg(
            show_all_msg=payload.input_json.show_all_msg,
            as_dict=True
        )
            
        return
    
    @app.post('/road/roughness/validation')
    def validate_iri(
            self,
            payload: RoadSurveyValidationInput,
            write: bool = False,
            ignore_force: bool = False,
            ignore_review: bool = False
    ):
        lrs = LRSRoute.from_feature_service(
            self.lrs_host,
            payload.input_json.routes[0]
        )

        check = RouteRoughnessValidation.validate_excel(
            excel_path=payload.input_json.file_name,
            route=payload.input_json.routes[0],
            survey_year=payload.input_json.year,
            survey_semester=payload.input_json.semester,
            sql_engine=self.smd_engine,
            lrs=lrs,
            ignore_review=ignore_review,
            force_write=ignore_force
        )

        if check.get_status() == 'rejected':
            return check.smd_output_msg(
                show_all_msg=payload.input_json.show_all_msg,
                as_dict=True
            )
        
        check.base_validation()
        
        if write and (check.get_status() == 'verified'):
            check.put_data()
        
        return check.smd_output_msg(
            show_all_msg=payload.input_json.show_all_msg,
            as_dict=True
        )

    @app.post('/road/defects/validation')
    def validate_defects(
        self,
        payload: RoadSurveyValidationInput,
        write: bool= False,
        ignore_force: bool = False,
        ignore_review: bool = False
    ):
        lrs = LRSRoute.from_feature_service(
            self.lrs_host,
            payload.input_json.routes[0]
        )

        gs_client = storage.Client(credentials=self.cred)

        sp = gs.SurveyPhotoStorage(
            gs_client=gs_client,
            bucket_name='sidako-bucket',
            sql_engine=self.smd_engine
        )

        check = RouteDefectsValidation.validate_excel(
            excel_path=payload.input_json.file_name,
            route=payload.input_json.routes[0],
            survey_year=payload.input_json.year,
            sql_engine=self.smd_engine,
            lrs=lrs,
            ignore_review=ignore_review,
            force_write=ignore_force,
            photo_storage=sp
        )

        if check.get_status() == 'rejected':
            return check.smd_output_msg(
                show_all_msg=payload.input_json.show_all_msg,
                as_dict=True
            )
        
        check.lrs_distance_check()
        check.lrs_sta_check()
        check.route_has_rni_check()
        check.sta_not_in_rni_check()
        check.survey_photo_url_check()

        if write and (check.get_status() == 'verified'):
            check.put_data()
            check.put_photos()
        
        del(gs_client)

        return check.smd_output_msg(
            show_all_msg=payload.input_json.show_all_msg,
            as_dict=True
        )

    @app.post('/road/pci/validation')
    def validate_pci(
        self,
        payload: RoadSurveyValidationInput,
        write: bool= False,
        ignore_force: bool = False,
        ignore_review: bool = False
    ):
        lrs = LRSRoute.from_feature_service(
            self.lrs_host,
            payload.input_json.routes[0]
        )

        check = RoutePCIValidation.validate_excel(
            excel_path=payload.input_json.file_name,
            route=payload.input_json.routes[0],
            survey_year=payload.input_json.year,
            sql_engine=self.smd_engine,
            lrs=lrs,
            ignore_review=ignore_review,
            force_write=ignore_force
        )

        if check.get_status() == 'rejected':
            return check.smd_output_msg(
                show_all_msg=payload.input_json.show_all_msg,
                as_dict=True
            )

        check.base_validation()

        if write and (check.get_status() == 'verified'):
            check.put_data(semester=payload.input_json.semester)
        
        return check.smd_output_msg(
            show_all_msg=payload.input_json.show_all_msg,
            as_dict=True
        )


serve.run(DataValidation.bind(), route_prefix='/bm')
