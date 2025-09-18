from pydantic import BaseModel
from route_events import LRSRoute
from route_events_service import RouteRNIValidation
from typing import List, Optional, Literal
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine


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

SERVICE_ACCOUNT_JSON = os.getenv('GCLOUD_SERVICE_ACCOUNT_JSON')

SMD_ENGINE = create_engine(f"oracle+oracledb://{SMD_USER}:{SMD_PWD}@{DB_HOST}:1521/geodbbm")
MISC_ENGINE = create_engine(f"oracle+oracledb://{MISC_USER}:{MISC_PWD}@{DB_HOST}:1521/geodbbm")


def validate_rni(
        payload: PayloadSMD, 
        job_id: str, 
        validate: bool=True
    ) -> str:
    """
    RNI validation handler function.
    """
    if validate or (validate is None):
        ignore_review=False
        force_write=False
    else:
        ignore_review=True
        force_write=True

    lrs = LRSRoute.from_feature_service(
            LRS_HOST, 
            payload.routes[0]
        )

    check = RouteRNIValidation.validate_excel(
        excel_path=payload.file_name,
        route=payload.routes[0],
        survey_year=payload.year,
        sql_engine=SMD_ENGINE,
        lrs=lrs,
        ignore_review=ignore_review,
        force_write=force_write
    )

    if check.get_status() == 'rejected':
        return check._result.to_job_event(job_id)

    if validate:
        check.base_validation()

    if (check.get_status() == 'verified'):
        check.put_data(semester=payload.semester)

    return check._result.to_job_event(job_id)
