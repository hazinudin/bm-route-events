from ray import serve, init
from fastapi import FastAPI
from sqlalchemy import create_engine
from pydantic import BaseModel
from route_events_service import (
    BridgeMasterValidation,
    BridgeInventoryValidation,
    RouteRNIValidation,
    RouteRoughnessValidation,
    RouteDefectsValidation,
    RoutePCIValidation
)
from route_events import LRSRoute
import json
import os
from dotenv import load_dotenv
from typing import Literal, Optional, List


init()

app = FastAPI()

class BridgeValidationPayload(BaseModel):
    input_json: dict

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

        self.smd_engine = create_engine(f"oracle+oracledb://{SMD_USER}:{SMD_PWD}@{HOST}:1521/geodbbm")
        self.misc_engine = create_engine(f"oracle+oracledb://{MISC_USER}:{MISC_PWD}@{HOST}:1521/geodbbm")
    
    @app.post('/bridge/master_validation')
    def validate_bridgemaster_data(
        self, 
        payload: BridgeValidationPayload,
        write: bool = False
    ):
        val_mode = payload.input_json.get('mode')

        if val_mode is None:
            return {"status": "Input JSON tidak memiliki MODE"}
        
        check = BridgeMasterValidation(
            data=payload.input_json,
            validation_mode=val_mode.upper(),
            lrs_grpc_host='localhost:50052',
            sql_engine=self.misc_engine
        )

        if check.get_status() == 'rejected':
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

        if val_mode is None:
            return {"status": "Input JSON tidak memiliki MODE"}
        
        check = BridgeInventoryValidation(
            data=payload.input_json,
            lrs_grpc_host='localhost:50052',
            validation_mode=val_mode.upper(),
            sql_engine=self.misc_engine,
            dev=True,
            popup=popup
        )

        if check.get_status() == 'rejected':
            return check.invij_json_result(as_dict=True)

        if check.validation_mode == 'UPDATE':
            check.update_check()

        if check.validation_mode == 'INSERT':
            check.insert_check()
        
        if write and (check.get_status() == 'verified'):
            check.put_data()

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
            'localhost:50052', 
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
            'localhost:50052',
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
        check.kemantapan_comparison_check()
        check.rni_segments_comparison()
        check.route_has_rni_check()
        check.pok_iri_check()
        
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
            'localhost:50052',
            payload.input_json.routes[0]
        )

        check = RouteDefectsValidation.validate_excel(
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
        
        check.lrs_distance_check()
        check.lrs_sta_check()
        check.route_has_rni_check()
        check.sta_not_in_rni_check()

        if write and (check.get_status() == 'verified'):
            check.put_data()
            check.put_photos()
        
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
            'localhost:50052',
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
