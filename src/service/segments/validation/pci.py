from .base import RouteSegmentEventsValidation
from ..analysis import segments_join, segments_coverage_join
from ...points.analysis import segments_points_join
from route_events import (
    RouteRNI,
    RouteRNIRepo,
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

        self._rni = None

        self._events = events
        self._repo = RoutePCIRepo(self._engine)
        self._rni_repo = RouteRNIRepo(self._engine)
        self._defects_repo = RouteDefectsRepo(self._engine)

        # Defects data
        self._defects = None
    
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
        
    def rni_surf_type_comparison(self):
        """
        Compare the surface type (from RNI data) with the
        """
        joined = segments_coverage_join(
            covering=self.rni,
            target=self._events,
            covering_select=[self.rni._surf_type_col],
            target_select=self._events.all_severity + self._events.all_volume,
            covering_agg=[pl.col(self.rni._surf_type_col).max()]
        )

        error = joined.filter(
            # Rigid
            pl.col(self.rni._surf_type_col).eq(21).and_(
                pl.all_horizontal(pl.col('^VOL_AS.*$').is_not_null()) |
                pl.all_horizontal(pl.col('^VOL_AS.*$').gt(0))
            ) |
            # Asphal
            pl.col(self.rni._surf_type_col).ne(21).and_(
                pl.col(self.rni._surf_type_col).is_in([1,2]).not_()
            ).and_(
                pl.all_horizontal(pl.col('^VOL_RG.*$').is_not_null()) |
                pl.all_horizontal(pl.col('^VOL_RG.*$').gt(0))
            ) |
            # Unpaved
            pl.col(self.rni._surf_type_col).is_in([1,2]).and_(
                pl.all_horizontal(pl.col('^VOL_AS.*$').is_not_null()) |
                pl.all_horizontal(pl.col('^VOL_AS.*$').gt(0)) |
                pl.all_horizontal(pl.col('^VOL_RG.*$').is_not_null()) |
                pl.all_horizontal(pl.col('^VOL_RG.*$').gt(0))
            )
        ).select(
            msg = pl.when(
                pl.col(self.rni._surf_type_col).eq(21)
            ).then(
                pl.format(
                    "Segmen {}-{} {} memiliki tipe perkerasan rigid namun memiliki kerusakan aspal.",
                    pl.col(self._events._from_sta_col).truediv(self._events.sta_conversion),
                    pl.col(self._events._to_sta_col).truediv(self._events.sta_conversion),
                    pl.col(self._events._lane_code_col)
                )
            ).when(
                pl.col(self.rni._surf_type_col).ne(21) & pl.col(self.rni._surf_type_col).is_in([1,2]).not_()
            ).then(
                pl.format(
                    "Segmen {}-{} {} memiliki tipe perkerasan aspal namum memiliki kerusakan rigid.",
                    pl.col(self._events._from_sta_col).truediv(self._events.sta_conversion),
                    pl.col(self._events._to_sta_col).truediv(self._events.sta_conversion),
                    pl.col(self._events._lane_code_col)
                )
            ).when(
                pl.col(self.rni._surf_type_col).is_in([1,2])
            ).then(
                pl.format(
                    "Segmen {}-{} {} memiliki tipe perkerasan tanah namun memiliki nilai kerusakan.",
                    pl.col(self._events._from_sta_col).truediv(self._events.sta_conversion),
                    pl.col(self._events._to_sta_col).truediv(self._events.sta_conversion),
                    pl.col(self._events._lane_code_col)
                )
            )
        )

        self._result.add_messages(
            error,
            'error'
        )

        return

    def rni_surf_type_segment_length_check(self, tolerance=0.005):
        """
        Compare the surface type (from defects data) with the PCI segment length. Rigid surface should have segment length of 0.1km
        and asphal with 0.05km segment length.
        """
        joined = segments_coverage_join(
            covering=self.rni,
            target=self._events,
            covering_select=[self.rni._surf_type_col],
            target_select=self._events.all_severity + self._events.all_volume + [self._events._seg_len_col],
            covering_agg=[pl.col(self.rni._surf_type_col).max()]
        )

        error = joined.filter(
            # Rigid
            pl.col(self.rni._surf_type_col).eq(21).and_(
                pl.col(self._events._seg_len_col).lt(0.1 - tolerance) |
                pl.col(self._events._seg_len_col).gt(0.1 + tolerance)
            ).and_(
                pl.col(self._events._from_sta_col + '_r').ne(self._events.last_segment.from_sta*self._events.sta_conversion)
            ) |
            # Asphal
            pl.col(self.rni._surf_type_col).ne(21).and_(
                pl.col(self.rni._surf_type_col).is_in([1,2]).not_()
            ).and_(
                pl.col(self._events._seg_len_col).lt(0.05 - tolerance) |
                pl.col(self._events._seg_len_col).gt(0.05 + tolerance)
            ).and_(
                pl.col(self._events._from_sta_col + '_r').ne(self._events.last_segment.from_sta*self._events.sta_conversion)
            )
        ).select(
            msg = pl.when(
                pl.col(self.rni._surf_type_col).eq(21)
            ).then(
                pl.format(
                    "Segmen {}-{} {} memiliki tipe perkerasan rigid, namun memiliki panjang segmen yang bukan 0.1km.",
                    pl.col(self._events._from_sta_col + '_r').truediv(self._events.sta_conversion),
                    pl.col(self._events._to_sta_col + '_r').truediv(self._events.sta_conversion),
                    pl.col(self._events._lane_code_col)
                )
            ).when(
                pl.col(self.rni._surf_type_col).ne(21) & pl.col(self.rni._surf_type_col).is_in([1,2]).not_()
            ).then(
                pl.format(
                    "Segmen {}-{} {} memiliki tipe perkerasan aspal, namun tidak memiliki panjang segmen 0.05km.",
                    pl.col(self._events._from_sta_col + '_r').truediv(self._events.sta_conversion),
                    pl.col(self._events._to_sta_col + '_r').truediv(self._events.sta_conversion),
                    pl.col(self._events._lane_code_col)
                )
            )
        )

        self._result.add_messages(
            error,
            'error'
        )

        return
    
    def defect_surf_type_segment_length_check(self, tolerance=0.005):
        """
        Compare the surface type (from defects data) with the PCI segment length. Rigid surface should have segment length of 0.1km
        and asphal with 0.05km segment length.
        """
        try:
            if self.defects.no_data:
                self._result.add_message("Data defect tidak tersedia untuk dibandingkan.", "error")
                return
            
            pci_defect = segments_points_join(
                segments=self._events,
                points=self.defects,
                point_select=[self.defects._surf_type_col],
                segment_select=[self._events._seg_len_col]
            )

            error_ = pci_defect.filter(
                # 21 equals to rigid
                pl.col(self._defects._surf_type_col).eq(21).and_(
                    pl.col(self._events._seg_len_col).lt(0.1 - tolerance) |
                    pl.col(self._events._seg_len_col).gt(0.1 - tolerance)
                ) |
                # Other than 21 or not a rigid surface
                pl.col(self._defects._surf_type_col).ne(21).and_(
                    pl.col(self._events._seg_len_col).lt(0.05 - tolerance) |
                    pl.col(self._events._seg_len_col).gt(0.05 - tolerance)
                )
            ).select(
                msg = pl.format(
                    "Segmen {}-{} memiliki tipe perkerasan {} namun memmiliki panjang segmen {}km.",
                    pl.col(self._events._from_sta_col),
                    pl.col(self._events._to_sta_col),
                    pl.col(self.defects._surf_type_col),
                    pl.col(self._events._seg_len_col)
                )
            )

            if not error_.is_empty():
                self._result.add_messages(
                    error_,
                    'error'
                )
            
            return
        
        except NoSuchTableError:
            # self._result.add_message("Data defect tidak tersedia untuk dibandingkan.", "error")
            return
        
    def damage_severity_check(self):
        """
        Check inconsistency between damage volume and its severity. 0/None volume should also come with NA/None severity.
        Greater than 0 volume should come with other than NA severity.
        """
        errors_ = self._events.invalid_volume_with_severity()

        if len(errors_) == 0:
            return
        
        msg = pl.DataFrame(errors_).select(
            msg = pl.format(
                "Segmen {}-{} {} memiliki volume dan tingkat kerusakan {} yang tidak cocok.",
                pl.col('from_sta'),
                pl.col('to_sta'),
                pl.col('lane_code'),
                pl.col('damage_column')
            )
        )

        self._result.add_messages(
            msg,
            'error'
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
            # self._result.add_message("Data defect tidak tersedia untuk dibandingkan.", "error")
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
        self.invalid_pci_check()
        self.rni_surf_type_comparison()
        self.rni_surf_type_segment_length_check()
        self.damage_severity_check()
        # self.has_defect_data_check()
        
        self.defect_surf_type_segment_length_check()
        self.defects_point_check()

