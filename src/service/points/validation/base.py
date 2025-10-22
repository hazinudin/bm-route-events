from typing import Type, Literal, Union
from route_events import (
    RoutePointEvents, 
    LRSRoute,
    RouteRNIRepo,
    RouteRNI
)
from ...validation_result.result import ValidationResult
from ..analysis import segments_points_join
from sqlalchemy import Engine
import polars as pl


class RoutePointEventsValidation(object):
    """
    Route point events validation
    """
    def __init__(
            self,
            events: Type[RoutePointEvents],
            lrs: LRSRoute,
            sql_engine: Engine,
            results: ValidationResult,
            route: str = None,
            survey_year: int = None,
            survey_semester: Literal[1,2] = None
    ):
        self._events = events
        self._lrs = lrs
        self._engine = sql_engine
        self._result = results
        self._route = route

        self._survey_year = survey_year
        self._survey_semester = survey_semester

        # M Value DataFrame
        self._df_lrs_mv = None

        # Distance to LRS DataFrame
        self._df_lrs_dist = None

        # RNI repo
        self._rni = None
        self._rni_repo = RouteRNIRepo(self._engine)
        self._get_rni_cols = '*'

    def get_all_messages(self) -> pl.DataFrame:
        """
        Get validation result messages.
        """
        return self._result.get_all_messages()
    
    def get_status(self) -> str:
        """
        Get validation process status.
        """
        return self._result.status
    
    @property
    def fetched_rni_columns(self):
        return self._get_rni_cols
    
    @fetched_rni_columns.setter
    def fetched_rni_columns(self, columns: list):
        self._get_rni_cols = columns
    
    @property
    def rni(self) -> RouteRNI:
        """
        Current year RNI
        """
        if self._rni is None:
            self._rni = self._rni_repo.get_by_linkid(
                self._route,
                year=self._survey_year,
                raise_if_table_does_not_exists=True,
                columns=self._get_rni_cols
            )

            return self._rni
        else:
            return self._rni
    
    @property
    def df_lrs_mv(self) -> pl.DataFrame:
        """
        Calculate M-Value for every input points.
        """
        if self._df_lrs_mv is None:

            self._df_lrs_mv = self._lrs.get_points_m_value(
                self._events.points_lambert
            )

            return self._df_lrs_mv
        else:
            return self._df_lrs_mv
        
    def lrs_distance_check(self, threshold=30):
        """
        Check every points nearest distance to LRS geometry, if the distance exceeds the threshold (in meters).
        Then an error message will be created.
        """
        if self._events.has_sta() and self._events.lane_data:
            format_args = [
                "Titik {} {} pada ruas {} berjarak lebih dari {}m dari geometri LRS, yaitu {}m",
                pl.col(self._events._sta_col),
                pl.col(self._events._lane_code_col),
                pl.col(self._events._linkid_col),
                pl.lit(threshold),
                pl.col('dist')
            ]
        elif self._events.has_sta():
            format_args = [
                "Titik {} pada ruas {} berjarak lebih dari {}m dari geometri LRS, yaitu {}m",
                pl.col(self._events._sta_col),
                pl.col(self._events._linkid_col),
                pl.lit(threshold),
                pl.col('dist')
            ]
        elif not self._events.has_sta():
            format_args = [
                "Titik pada ruas {} berjarak lebih dari {}m dari geometri LRS.",
                pl.col(self._events._linkid_col),
                pl.lit(threshold),
            ]

        errors = self.df_lrs_mv.filter(
            pl.col('dist') >= threshold
        ).with_columns(
            msg= pl.format(*format_args)
        )

        self._result.add_messages(
            errors.select('msg'),
            'review',
            'force'
        )
        
        return self
    
    def lrs_sta_check(self, tolerance: int = 30):
        """
        Compare survey point M-Value with its STA
        """
        if self._events.lane_data:
            format_args = [
                "Titik {} {} memiliki nilai STA LRS yang tidak cocok, yaitu {}.",
                pl.col(self._events._sta_col),
                pl.col(self._events._lane_code_col),
                pl.col('m_val')
            ]
        else:
            format_args = [
                "Titik {} memiliki nilai STA LRS yang tidak cocok, yaitu {}.",
                pl.col(self._events._sta_col),
                pl.col('m_val')
            ]

        errors = self.df_lrs_mv.filter(
            pl.col(self._events._sta_col).mul(
                self._events.sta_conversion
            ).gt(
                pl.col('m_val').add(tolerance)
            ) |
            pl.col(self._events._sta_col).mul(
                self._events.sta_conversion
            ).lt(
                pl.col('m_val').sub(tolerance)
            )
        ).select(
            msg = pl.format(*format_args)
        )

        self._result.add_messages(
            errors,
            'error',
            'force'
        )

    def route_has_rni_check(self):
        """
        Check if route has RNI data in the same year.
        """
        if self.rni is None:
            msg = f"Data RNI untuk ruas {self._route} belum tervalidasi"

            self._result.add_message(
                msg,
                'error'
            )
        
        return
    
    def sta_not_in_rni_check(self):
        """
        Check for STA value which does not have match with RNI segments.
        """
        errors = segments_points_join(
            segments=self.rni,
            points=self._events,
            how='anti'
        ).select(
            msg=pl.format(
                "Titik {} {} tidak memiliki padanan segmen RNI.",
                pl.col(self._events._sta_col).truediv(self._events.sta_conversion).cast(pl.Int32()),
                pl.col(self._events._lane_code_col)
            )
        )

        self._result.add_messages(
            errors,
            'error'
        )

        return
    
    def smd_output_msg(self, show_all_msg: bool = False, as_dict: bool = True) -> Union[str | dict]:
        """
        Return SMD output message format in either Python dictionary or JSON formatted string.
        """
        return self._result.to_smd_format(
            show_all_msg=show_all_msg,
            as_dict=as_dict
        )
    