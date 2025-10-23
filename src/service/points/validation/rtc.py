from .base import RoutePointEventsValidation
from ...photo import gs
from route_events import (
    RouteRTCRepo,
    RouteRTC,
    LRSRoute,
)
from ..analysis import segments_points_join
from sqlalchemy import Engine
from typing import List
from ...validation_result.result import ValidationResult
import polars as pl
from pydantic import ValidationError


class RouteRTCValidation(RoutePointEventsValidation):
    """
    Route Defects Events validation class.
    """
    @classmethod
    def validate_excel(
        cls,
        excel_path: str,
        route: str,
        survey_year: int,
        sql_engine: Engine,
        lrs: LRSRoute,
        linkid_col: str = 'LINKID',
        ignore_review: bool = False,
        force_write: bool = False,
    ):
        """
        Validate Defects data in Excel file.
        """
        ignored_tag = []

        if force_write:
            ignored_tag.append('force')
        
        if ignore_review:
            ignored_tag.append('review')

        result = ValidationResult(route, ignore_in=ignored_tag)

        obj = None

        try:
            events = RouteRTC.from_excel(
                excel_path=excel_path,
                linkid=route,
                linkid_col=linkid_col,
                ignore_review=ignore_review,
                data_year=survey_year
            )

            obj = cls(
                route=route,
                events=events,
                lrs=lrs,
                sql_engine=sql_engine,
                results=result,
                survey_year=survey_year,
            )

            return obj
        
        except ValidationError as e:
            for error in e.errors():
                if 'review' in error['type']:
                    result.add_message(error['msg'], 'review', 'review')
                else:
                    result.add_message(error['msg'], 'rejected')
        
            if result.status == 'rejected':
                obj = cls(
                    route=route,
                    events=pl.DataFrame(),
                    lrs=None,
                    sql_engine=sql_engine,
                    results=result,
                    survey_year=survey_year,
                )

                return obj
            
            else:
                events = RouteRTC.from_excel(
                    excel_path=excel_path,
                    linkid=route,
                    linkid_col=linkid_col,
                    ignore_review=True,
                    data_year=survey_year
                )

                obj = cls(
                    route=route,
                    events=events,
                    lrs=lrs,
                    sql_engine=sql_engine,
                    results=result,
                    survey_year=survey_year,
                )

                return obj
        
        except IndexError:
            result.add_message(f"File '{excel_path}' tidak dapat ditemukan.", 'rejected')

            obj = cls(
                route=route,
                events=pl.DataFrame(),
                lrs=lrs,
                sql_engine=sql_engine,
                results=result,
                survey_year=survey_year,
             )

            return obj

    def __init__(
            self,
            route: str,
            events: RouteRTC,
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
            survey_year=survey_year,
        )

        self._repo = RouteRTCRepo(self._engine)
        self._events: RouteRTC

        # Survey photos
        self._photos = None

    def base_validation(self):
        """
        Base validation function.
        """
        self.invalid_interval_check()
        self.invalid_survey_duration()
        self.lrs_distance_check()
        self.route_has_rni_check()

    def invalid_interval_check(self):
        """
        Check for rows with invalid interval from the previous survey timestamp.
        """
        interval = 15  # Interval in minutes

        msg = self._events.invalid_interval(interval=interval).select(
            pl.format(
                "Data survey pada {} arah {} tidak berjarak {} menit dari input sebelumnya.",
                pl.col(self._events._timestamp_col),
                pl.col(self._events._surv_dir_col),
                pl.lit(interval)
            )
        )

        self._result.add_messages(msg, 'error')

        return
    
    def invalid_survey_duration(self):
        """
        Check if the RTC survey duration is at least 3 days.
        """
        if self._events.survey_duration() < 3:
            msg = "Durasi survey kurang dari 3 hari."

            self._result.add_message(msg, "error")

        return
