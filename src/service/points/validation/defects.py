from .base import RoutePointEventsValidation
from src.route_events import (
    RouteDefects,
    RouteDefectsRepo,
    LRSRoute,
    SurveyPhoto
)
from sqlalchemy import Engine
from typing import List
from ...validation_result.result import ValidationResult
import polars as pl


class RouteDefectsValidation(RoutePointEventsValidation):
    """
    Route Defects Events validation class.
    """
    def __init__(
            self,
            route: str,
            events: RouteDefects,
            lrs: LRSRoute,
            sql_engine: Engine,
            results: ValidationResult,
            survey_year: int = None
    ):
        super().__init__(
            events=events,
            lrs=lrs,
            sql_engine=sql_engine,
            results=results,
            route=route,
            survey_year=survey_year
        )

        self._repo = RouteDefectsRepo(self._engine)

        # Survey photos
        self._photos = None
    
    @property
    def survey_photos(self) -> List[SurveyPhoto]:
        """
        Return a list containing SurveyPhoto with STA referenced to LRS.
        """
        if self._photos is None:
            self._photos = [
                SurveyPhoto(**_) for _ in self._events.pl_df.join(
                    self.df_lrs_mv,
                    on=[
                        self._events._linkid_col,
                        self._events._sta_col,
                        self._events._lane_code_col
                    ]
                ).select(
                    pl.col(self._events._photo_url_cols).alias('url'),
                    pl.col('m_val').alias('sta_meters'),
                    pl.lit(self._survey_year).alias('survey_year'),
                    pl.col(self._events._linkid_col).alias('linkid'),
                    pl.col(self._events._lat_col).alias('latitude'),
                    pl.col(self._events._long_col).alias('longitude')
                ).rows(named=True) 
            ]
            return self._photos
        else:
            return self._photos
        
    def validate_photo_url(self):
        """
        Validate photo URL or filename which is stored in the repository.
        """
        return