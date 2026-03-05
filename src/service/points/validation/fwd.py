from .base import RoutePointEventsValidation
from route_events import (
    RouteFWD,
    RouteFWDRepo,
    LRSRoute,
)
from ..analysis import segments_points_join
from sqlalchemy import Engine
from ...validation_result.result import ValidationResult
import polars as pl
from pydantic import ValidationError


class RouteFWDValidation(RoutePointEventsValidation):
    """
    Route FWD Events validation class.
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
        Validate FWD data in Excel file.
        """
        ignored_tag = []

        if force_write:
            ignored_tag.append('force')

        if ignore_review:
            ignored_tag.append('review')

        result = ValidationResult(route, ignore_in=ignored_tag)

        obj = None

        try:
            events = RouteFWD.from_excel(
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
                events = RouteFWD.from_excel(
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
            events: RouteFWD,
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

        self._repo = RouteFWDRepo(self._engine)
        self._events: RouteFWD

    def surface_thickness_check(self):
        """
        Validate surface thickness values based on surface type from RNI.
        - Rigid (surf_type=21): 150-320 mm
        - Asphalt (category='paved' and surf_type != 21): 70-350 mm
        """
        suffix = '_r' if self._events._surf_thickness_col == self.rni._surf_thickness_col else ''

        # Get valid ranges
        valid_ranges = self._events.valid_surface_thickness()
        rigid_lower, rigid_upper = valid_ranges.rigid.lower, valid_ranges.rigid.upper
        asphalt_lower, asphalt_upper = valid_ranges.asphalt.lower, valid_ranges.asphalt.upper

        # Join FWD points with RNI segments to get surface type
        joined = segments_points_join(
            segments=self.rni,
            points=self._events,
            how='inner',
            point_select=[self._events._surf_thickness_col],
            segment_select=[self.rni._surf_type_col],
            suffix='_r'
        ).join(
            self.rni.surface_types_mapping,
            left_on=self.rni._surf_type_col + suffix,
            right_on='surf_type',
            how='left'
        )

        # Classify surfaces and filter for invalid thickness
        errors = joined.filter(
            pl.col(self._events._surf_thickness_col).is_not_null()
        ).with_columns(
            surface_type=pl.when(
                pl.col(self.rni._surf_type_col + suffix) == 21
            ).then(
                pl.lit('rigid')
            ).when(
                (pl.col('category') == 'paved') & (pl.col(self.rni._surf_type_col + suffix) != 21)
            ).then(
                pl.lit('asphal')
            ).otherwise(None)
        ).filter(
            (pl.col('surface_type') == 'rigid') & (
                (pl.col(self._events._surf_thickness_col) < rigid_lower) |
                (pl.col(self._events._surf_thickness_col) > rigid_upper)
            ) |
            (pl.col('surface_type') == 'asphal') & (
                (pl.col(self._events._surf_thickness_col) < asphalt_lower) |
                (pl.col(self._events._surf_thickness_col) > asphalt_upper)
            )
        ).select(
            msg=pl.format(
                "Ketebalan perkerasan pada STA {} {} tidak sesuai dengan tipe perkerasan, yaitu {} sedangkan rentang yang valid untuk {} adalah {}-{}",
                pl.col(self._events._sta_col).truediv(self._events.sta_conversion).cast(pl.Int64),
                pl.col(self._events._lane_code_col),
                pl.col(self._events._surf_thickness_col),
                pl.col('surface_type'),
                pl.when(pl.col('surface_type') == 'rigid')
                    .then(pl.lit(rigid_lower))
                    .otherwise(pl.lit(asphalt_lower)),
                pl.when(pl.col('surface_type') == 'rigid')
                    .then(pl.lit(rigid_upper))
                    .otherwise(pl.lit(asphalt_upper))
            )
        )

        self._result.add_messages(
            errors,
            'error',
            'force'
        )

        return

    def median_direction_check(self):
        """
        Ensure survey points at the same STA have both directions when the segment has a median.
        """
        # Join FWD points with RNI segments to get median information
        joined = segments_points_join(
            segments=self.rni,
            points=self._events,
            how='inner',
            segment_select=[self.rni._med_width_col],
            suffix='_r'
        ).with_columns(
            direction=pl.col(self._events._lane_code_col).str.head(1)
        )

        # Group by LINKID and STA to count directions
        med_width_col_r = self.rni._med_width_col + '_r'
        errors = joined.group_by(
            self._events._linkid_col,
            self._events._sta_col
        ).agg(
            med_width=pl.col(med_width_col_r).max(),
            direction_count=pl.col('direction').n_unique(),
            directions=pl.col('direction').unique()
        ).filter(
            (pl.col('med_width') > 0) & (pl.col('direction_count') == 1)
        ).select(
            msg=pl.format(
                "Titik survey pada STA {} seharusnya memiliki data kedua arah karena segmen ini memiliki median",
                pl.col(self._events._sta_col).truediv(self._events.sta_conversion).cast(pl.Int64)
            )
        )

        self._result.add_messages(
            errors,
            'error',
        )

        return

    def d0_surface_check(self):
        """
        Validate D0 deflection values based on surface type from RNI.
        - Rigid (surf_type=21): 90-350
        - Asphalt (category='paved' and surf_type != 21): 0-5000
        """
        suffix = '_r' if self._events._d0_col == self.rni._surf_type_col else ''

        # Get valid ranges
        valid_ranges = self._events.valid_d0_range()
        rigid_lower, rigid_upper = valid_ranges.rigid.lower, valid_ranges.rigid.upper
        asphalt_lower, asphalt_upper = valid_ranges.asphalt.lower, valid_ranges.asphalt.upper

        # Join FWD points with RNI segments to get surface type
        joined = segments_points_join(
            segments=self.rni,
            points=self._events,
            how='inner',
            point_select=[self._events._d0_col],
            segment_select=[self.rni._surf_type_col],
            suffix='_r'
        ).join(
            self.rni.surface_types_mapping,
            left_on=self.rni._surf_type_col + suffix,
            right_on='surf_type',
            how='left'
        )

        # Classify surfaces and filter for invalid D0 values
        errors = joined.filter(
            pl.col(self._events._d0_col).is_not_null()
        ).with_columns(
            surface_type=pl.when(
                pl.col(self.rni._surf_type_col + suffix) == 21
            ).then(
                pl.lit('rigid')
            ).when(
                (pl.col('category') == 'paved') & (pl.col(self.rni._surf_type_col + suffix) != 21)
            ).then(
                pl.lit('asphal')
            ).otherwise(None)
        ).filter(
            (pl.col('surface_type') == 'rigid') & (
                (pl.col(self._events._d0_col) < rigid_lower) |
                (pl.col(self._events._d0_col) > rigid_upper)
            ) |
            (pl.col('surface_type') == 'asphal') & (
                (pl.col(self._events._d0_col) < asphalt_lower) |
                (pl.col(self._events._d0_col) > asphalt_upper)
            )
        ).select(
            msg=pl.format(
                "Nilai D0 pada STA {} {} tidak sesuai dengan tipe perkerasan, yaitu {} sedangkan rentang yang valid untuk {} adalah {}-{}",
                pl.col(self._events._sta_col).truediv(self._events.sta_conversion).cast(pl.Int64),
                pl.col(self._events._lane_code_col),
                pl.col(self._events._d0_col),
                pl.col('surface_type'),
                pl.when(pl.col('surface_type') == 'rigid')
                    .then(pl.lit(rigid_lower))
                    .otherwise(pl.lit(asphalt_lower)),
                pl.when(pl.col('surface_type') == 'rigid')
                    .then(pl.lit(rigid_upper))
                    .otherwise(pl.lit(asphalt_upper))
            )
        )

        self._result.add_messages(
            errors,
            'error',
            'force'
        )

        return
    
    def base_validation(self):
        """
        Base validation function
        """
        self.d0_surface_check()
        self.surface_thickness_check()
        self.median_direction_check()
        self.lrs_distance_check()
        self.lrs_sta_check()
        self.route_has_rni_check()
        self.sta_not_in_rni_check()
        