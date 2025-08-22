from .base import RouteSegmentEventsValidation
from ..analysis import segments_join, CompareRNISegments, segments_coverage_join
from ..summary import Kemantapan
from route_events import (
    RouteRoughness, 
    LRSRoute, 
    RouteRoughnessRepo,
    RouteRNI,
    RouteRNIRepo,
    RoutePOKRepo,
    RoutePOK
)
from ...validation_result.result import ValidationResult
from typing import Type, Literal
from sqlalchemy import Engine
import polars as pl
from pydantic import ValidationError


class RouteRoughnessValidation(RouteSegmentEventsValidation):
    """
    Route Roughness Events validation class.
    """
    @classmethod
    def validate_excel(
        cls,
        excel_path: str,
        route: str,
        survey_year: int,
        survey_semester: Literal[1,2],
        sql_engine: Engine,
        lrs: LRSRoute, 
        linkid_col: str = 'LINKID',
        ignore_review: bool = False,
        force_write: bool = False
    ):
        """
        Validate RNI data in excel file.
        """
        if force_write:
            result = ValidationResult(route, ignore_in=['force'])
        elif force_write and ignore_review:
            result = ValidationResult(route, ignore_in=['force', 'review'])
        else:
            result = ValidationResult(route)

        obj = None
        try:
            events = RouteRoughness.from_excel(
                excel_path=excel_path,
                linkid=route,
                linkid_col=linkid_col,
                ignore_review=ignore_review,
                data_year=survey_year,
                data_semester=survey_semester
            )

            obj = cls(
                route=route,
                events=events,
                lrs=lrs,
                sql_engine=sql_engine,
                results=result,
                survey_year=survey_year,
                survey_semester=survey_semester
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
                    survey_semester=survey_semester
                )

                return obj
            else:
                events = RouteRoughness.from_excel(
                    excel_path=excel_path,
                    linkid=route,
                    linkid_col=linkid_col,
                    ignore_review=True,
                    data_year=survey_year,
                    data_semester=survey_semester
                )
                
                obj = cls(
                    route=route,
                    events=events,
                    lrs=lrs,
                    sql_engine=sql_engine,
                    results=result,
                    survey_year=survey_year,
                    survey_semester=survey_semester
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
                survey_semester=survey_semester
            )

            return obj

    def __init__(
            self,
            route: str,
            events: RouteRoughness,
            lrs: LRSRoute,
            sql_engine: Engine,
            results: ValidationResult,
            survey_year: int = None,
            survey_semester: Literal[1,2] = None
    ):
        super().__init__(
            events,
            lrs,
            sql_engine,
            results,
            survey_year=survey_year,
            survey_semester=survey_semester,
            route=route
        )

        self._events = events
        self._prev_data = None
        self._prev_rni = None
        self._rni = None
        self._pok = None

        self._repo = RouteRoughnessRepo(self._engine)
        self._rni_repo = RouteRNIRepo(self._engine)
        self._pok_repo = RoutePOKRepo(self._engine)

    @property
    def prev_data(self) -> RouteRoughness:
        """
        Previous semester data
        """
        if self._prev_data is None:
            if self._survey_sem == 2:
                prev_year = self._survey_year
                prev_sem = 1
            else:
                prev_year = self._survey_year-1
                prev_sem = 2

            self._prev_data = self._repo.get_by_linkid(
                self._route,
                year=prev_year,
                semester=prev_sem,
                raise_if_table_does_not_exists=True
            )

            self._prev_data.sta_unit='km'

            return self._prev_data
        else:
            return self._prev_data
        
    @property
    def prev_rni(self) -> RouteRNI:
        """
        Previous year RNI
        """
        if self._prev_rni is None:
            self._prev_rni = self._rni_repo.get_by_linkid(
                self._route, 
                year=self._survey_year-1,
                raise_if_table_does_not_exists=True
            )

            return self._prev_rni

        else:
            return self._prev_rni
        
    @property
    def rni(self) -> RouteRNI:
        """
        Current year RNI
        """
        if self._rni is None:
            self._rni = self._rni_repo.get_by_linkid(
                self._route, 
                year=self._survey_year,
                raise_if_table_does_not_exists=True
            )

            return self._rni

        else:
            return self._rni
        
    @property
    def pok(self) -> RoutePOK:
        """
        Previous year POK
        """
        if self._pok is None:
            self._pok = self._pok_repo.get_by_comp_name(
                self._route,
                self._survey_year-1,
                comp_name_keywords=[
                    'rehabilitasi'
                ]
            )

            return self._pok
        
        else:
            return self._pok

    def kemantapan_comparison_check(self, grade_changes=2):
        """
        Check for segments which has kemantapan degradation compared to previous year data.
        """
        k = Kemantapan(
            self._events,
            self.rni
        ).segment('iri_kemantapan', eager=False)

        k_prev = Kemantapan(
            self.prev_data,
            self.prev_rni
        ).segment('iri_kemantapan', eager=False)

        errors = k.join(
            k_prev,
            on=[
                self._events._linkid_col,
                self._events._from_sta_col,
                self._events._to_sta_col
            ],
            suffix='_prev'
        ).filter(
            # The delta between current and previous is greather than required.
            pl.col('grade').sub(
                pl.col('grade_prev')
            ).ge(grade_changes)
        ).select(
            msg=pl.format(
                "Segmen {}-{} mengalami penurunan kemantapan sebanyak {} tingkat jika dibandingkan dengan data tahun lalu",
                pl.col(self._events._from_sta_col),
                pl.col(self._events._to_sta_col),
                pl.lit(grade_changes)
            )
        )

        self._result.add_messages(
            errors.collect(),
            'review',
            'review'
        )

        return

    def rni_segments_comparison(self):
        """
        Check if all RNI segments match with the input events.
        """
        comp = CompareRNISegments(
            rni=self.rni,
            other=self._events
        )

        errors = comp.rni_with_no_match().select(
            msg = pl.format(
                "Segmen {}-{} {} ada pada data RNI, namun tidak ada pada data ini.",
                pl.col(self.rni._from_sta_col).truediv(self.rni.sta_conversion),
                pl.col(self.rni._to_sta_col).truediv(self.rni.sta_conversion),
                pl.col(self.rni._lane_code_col)
            )
        )

        self._result.add_messages(
            errors,
            'error'
        )

        return
    
    def route_has_rni_check(self):
        """
        Check if route has RNI data in the same year
        """
        if self.rni is None:
            msg = f"Data RNI untuk ruas {self._route} belum tervalidasi"
            
            self._result.add_message(
                msg,
                'error'
            )
        
        return
    
    def pok_iri_check(self, iri_threshold:int = 3):
        """
        Check for segments which covered by POK, the IRI value should not exceeds 3.
        """
        errors = segments_coverage_join(
            covering=self.pok,
            target=self._events,
            covering_select=[self.pok._comp_col],
            target_select=[self._events._iri_col]
        ).filter(
            pl.col(self._events._iri_col).gt(iri_threshold)
        ).select(
            msg=pl.format(
                "Segmen {}-{} {} mendapatkan {}, namun nilai IRI lebih besar dari {}, yaitu {}",
                pl.col(self._events._from_sta_col).truediv(self._events.sta_conversion).round(3),
                pl.col(self._events._to_sta_col).truediv(self._events.sta_conversion).round(3),
                pl.col(self._events._lane_code_col),
                pl.col(self.pok._comp_col),
                pl.lit(iri_threshold),
                pl.col(self._events._iri_col)
            )
        )

        self._result.add_messages(
            errors,
            'review',
            'review'
        )

        return
    
    def put_data(self):
        """
        Delete and insert events data to geodatabase table.
        """
        self._repo.put(self._events, year=self._survey_year, semester=self._survey_sem)

    def base_validation(self):
        super().base_validation()

        self.survey_date_year_check()
        self.segment_length_check(tolerance=0.005)
        self.kemantapan_comparison_check()
        self.rni_segments_comparison()
        self.route_has_rni_check()
        self.pok_iri_check()