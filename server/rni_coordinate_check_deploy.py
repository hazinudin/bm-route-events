from ray import serve
from src.service.segments.validation.base import RouteSegmentEventsValidation
from src.route_events.segments.base.repo import RouteSegmentEventsRepo
from src.route_events import LRSRoute
from src.service.validation_result.result import ValidationResult
from fastapi import FastAPI
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


app = FastAPI()

@serve.deployment(num_replicas=8)
@serve.ingress(app)
class RNICoordinateValidation:
    def __init__(self):
        load_dotenv(os.path.dirname(__file__) + '/.env')

        HOST = os.getenv('DB_HOST')
        SMD_USER = os.getenv('SMD_USER')
        SMD_PWD = os.getenv('SMD_PWD')

        self.smd_engine = create_engine(f"oracle+oracledb://{SMD_USER}:{SMD_PWD}@{HOST}:1521/geodbbm")
        self.repo = RouteSegmentEventsRepo(self.smd_engine, table_name='rni_2_2024')

    @app.post('/rni_rerun/')
    def validate_rni_coordinate(self, route: str):
        lrs = LRSRoute.from_feature_service('localhost:50052', route)
        events = self.repo.get_by_linkid(route)
        results = ValidationResult(route)

        check = RouteSegmentEventsValidation(
            events=events,
            lrs=lrs,
            sql_engine=self.smd_engine,
            results = results
        )

        check.lrs_distance_check()
        check.lrs_monotonic_check()
        check.lrs_direction_check()
        check.max_sta_check()
        # check.lrs_segment_length_check()
        # check.lrs_sta_check()

        if check._result.status != 'verified':
            check._result.get_all_messages().write_csv(
                f'C:/Users/hazin/Projects/bm-route-events/scratch/rni_rerun/{route}.csv'
                )
        else:
            check._result.get_all_messages().write_csv(
                f'C:/Users/hazin/Projects/bm-route-events/scratch/rni_rerun/verified/{route}.csv'
                )


if __name__ == '__main__':
    serve.run(RNICoordinateValidation.bind(), route_prefix='/bm')
