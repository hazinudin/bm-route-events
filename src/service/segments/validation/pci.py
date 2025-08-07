from .base import RouteSegmentEventsValidation
from ..analysis import segments_join
from ...points.analysis import segments_points_join
from route_events import (
    RoutePCI, 
    LRSRoute, 
    RoutePCIRepo,
    RouteDefectsRepo,
    RouteDefects
)
from ...validation_result.result import ValidationResult
from typing import Type
from sqlalchemy import Engine
from sqlalchemy.exc import NoSuchTableError
import polars as pl
from pydantic import ValidationError


class RoutePCIValidation(RouteSegmentEventsValidation):
    """
    Route PCI Events validation class.
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
        force_write: bool = False
    ):
        """
        Validate PCI data in excel file.
        """
        if force_write:
            result = ValidationResult(route, ignore_in=['force'])
        elif force_write and ignore_review:
            result = ValidationResult(route, ignore_in=['force', 'review'])
        else:
            result = ValidationResult(route)
            
        obj = None
        try:
            events = RoutePCI.from_excel(
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
                survey_year=survey_year
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
                    survey_year=survey_year
                )

                return obj
            
            else:
                events = RoutePCI.from_excel(
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
                    survey_year=survey_year
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
                survey_year=survey_year
            )

            return obj

    
    def __init__(
            self,
            route: str,
            events: RoutePCI,
            lrs: LRSRoute,
            sql_engine: Engine,
            results: ValidationResult,
            survey_year: int = None
    ):
        super().__init__(
            events,
            lrs,
            sql_engine,
            results,
            survey_year=survey_year,
            route=route
        )

        self._events = events
        self._repo = RoutePCIRepo(self._engine)
        self._defects_repo = RouteDefectsRepo(self._engine)

        # Defects data
        self._defects = None

    @property
    def defects(self) -> RouteDefects:
        """
        RouteDefects object
        """
        if self._defects is None:
            self._defects = self._defects_repo.get_by_linkid(
                linkid=self._route,
                year=self._survey_year,
                raise_if_table_does_not_exists=True
            )

            return self._defects
        else:
            return self._defects
        
    def invalid_pci_check(self):
        """
        Check for segments with invalid PCI score compared to its damage value.
        """
        msg_args = [
            pl.col(self._events._from_sta_col),
            pl.col(self._events._to_sta_col),
            pl.col(self._events._lane_code_col),
            pl.col(self._events._pci_col)
        ]
        errors = self._events.invalid_pci_value().select(
            msg=pl.when(
                pl.col(self._events._pci_col).eq(self._events._pci_max)
            ).then(
                pl.format(
                    "Segmen {}-{} {} memiliki nilai PCI={}, namun segmen memiliki kerusakan.",
                    *msg_args
                )
            ).otherwise(
                pl.format(
                    "Segmen {}-{} {} memiliki nilai PCI={}, namun segmen tidak memiliki kerusakan.",
                    *msg_args
                )
            )
        )

        self._result.add_messages(
            errors,
            'error'
        )

        return
    
    def has_defect_data_check(self):
        """
        Make sure the route has defects data.
        """
        try:
            if self.defects.no_data:
                self._result.add_message("Data defect tidak tersedia untuk dibandingkan.", "error")
                return
        
        except NoSuchTableError:
            self._result.add_message(
                "Data defect tidak tersedia untuk dibandingkan.", 
                "error"
            )
            return
        
    def defects_point_check(self):
        """
        Compare the damage data with the defect data. The listed damage should match the damage in defect data.
        """
        try:
            if self.defects.pl_df.is_empty():
                self._result.add_message("Data defect tidak tersedia untuk dibandingkan.", "error")
                return 
            
            pivot = segments_points_join(
                segments=self._events,
                points=self.defects,
                point_select=[
                    self.defects._defects_type_col,
                    self.defects._defects_dimension_col
                ]
            ).with_columns(
                **{
                    self.defects._defects_type_col: pl.format(
                        "VOL_RDD_{}",
                        pl.col(self.defects._defects_type_col)
                    )
                }
            ).pivot(
                on=self.defects._defects_type_col,
                index=[
                    self._events._linkid_col,
                    self._events._from_sta_col,
                    self._events._to_sta_col,
                    self._events._lane_code_col
                ],
                values=self.defects._defects_dimension_col,
                aggregate_function=pl.element().sum()
            ).join(
                self._events.pl_df.with_columns(
                    pl.col(self._events._from_sta_col).mul(self._events.sta_conversion).cast(pl.Int32),
                    pl.col(self._events._to_sta_col).mul(self._events.sta_conversion).cast(pl.Int32)
                ),
                on=[
                    self._events._linkid_col,
                    self._events._from_sta_col,
                    self._events._to_sta_col,
                    self._events._lane_code_col
                ],
                how='right'
            )

        except NoSuchTableError:
            self._result.add_message("Data defect tidak tersedia untuk dibandingkan.", "error")
            return

        ldf = []  # For errors LazyFrame

        for dmg in self._events.all_damages:
            pci_col = f"{self._events._dvol}{dmg}"
            rdd_col = f"VOL_RDD_{dmg}"

            if (
                pivot[pci_col].is_not_null().any() and pivot[pci_col].ne(pl.lit(0))
            ) and (rdd_col not in pivot.columns):
                
                msg = f"Data PCI memiliki kerusakan {dmg} namun tidak pada data defects."
                self._result.add_message(
                    msg,
                    'error'
                )

            elif rdd_col in pivot.columns:
                errors = pivot.lazy().filter(
                    pl.col(pci_col).ne(
                        pl.col(rdd_col)
                    ).or_(
                        pl.col(pci_col).is_null().or_(pl.col(pci_col).eq(0)) & (pl.col(rdd_col).is_not_null())
                    ).or_(
                        pl.col(pci_col).is_not_null().and_(pl.col(pci_col).gt(0)).and_(pl.col(rdd_col).is_null())
                    )
                ).select(
                    msg=pl.format(
                        "Segmen {}-{} {} memiliki volume kerusakan {} sebesar {}, namun data Defect memiliki volume {}.",
                        pl.col(self._events._from_sta_col),
                        pl.col(self._events._to_sta_col),
                        pl.col(self._events._lane_code_col),
                        pl.lit(dmg),
                        pl.when(pl.col(pci_col).is_null()).then(pl.lit('null')).otherwise(pl.col(pci_col)),
                        pl.when(pl.col(rdd_col).is_null()).then(pl.lit('null')).otherwise(pl.col(rdd_col))
                    )
                )

                ldf.append(errors)
        
        # If there is an LazyFrame for error messages.
        if len(ldf) > 0:
            self._result.add_messages(
                pl.concat(ldf, parallel=True).collect(),
                'error'
            )

        return 
    
    def put_data(self, semester: int=2):
        """
        Delete and insert events data to geodatabase table.
        """
        self._repo.put(self._events, year=self._survey_year, semester=semester)

    def base_validation(self):
        super().base_validation()
        self.has_defect_data_check()

        self.invalid_pci_check()
        self.defects_point_check()

