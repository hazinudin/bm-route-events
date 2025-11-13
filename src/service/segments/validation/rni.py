from .base import RouteSegmentEventsValidation
from ..analysis import segments_join
from route_events import RouteRNI, LRSRoute, RouteRNIRepo
from ...validation_result.result import ValidationResult
from typing import Type
from sqlalchemy import Engine
import polars as pl
from pydantic import ValidationError


PARTIAL_UPDATE_KEYWORDS = 'partial_update'


class RouteRNIValidation(RouteSegmentEventsValidation):
    """
    Route RNI Events validation class.
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
        Validate RNI data in excel file.
        """
        ignored_tag = []

        if force_write:
            ignored_tag.append('force')
        
        if ignore_review:
            ignored_tag.append('review')

        result = ValidationResult(route, ignore_in=ignored_tag)
        
        obj = None
        try:
            events = RouteRNI.from_excel(
                excel_path=excel_path,
                linkid=route,
                linkid_col=linkid_col,
                ignore_review=ignore_review,
                data_year=survey_year,
                filter=pl.col('DETIL_IVR').eq(PARTIAL_UPDATE_KEYWORDS)
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
                    lrs=lrs,
                    sql_engine=sql_engine,
                    results=result,
                    survey_year=survey_year
                )

                return obj
            else:
                events = RouteRNI.from_excel(
                    excel_path=excel_path,
                    linkid=route,
                    linkid_col=linkid_col,
                    ignore_review=True,
                    data_year=survey_year,
                    filter=pl.col('DETIL_IVR').eq(PARTIAL_UPDATE_KEYWORDS)
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
            events: RouteRNI,
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
        self._prev_data = None
        self._prev_sem_data = None
        self._repo = RouteRNIRepo(self._engine)

    @property
    def prev_data(self) -> RouteRNI:
        """
        Previous year data.
        """
        if self._prev_data is None:
            self._prev_data = self._repo.get_by_linkid(
                self._route, 
                year=self._survey_year-1,
                raise_if_table_does_not_exists=True
            )

            return self._prev_data

        else:
            return self._prev_data
        
    @property
    def prev_sem_data(self) -> RouteRNI:
        """
        Data from previous semester, only if the current data is from second semester (semester = 2).
        """
        if self._prev_sem_data is None and self._events.semester == 2:
            self._prev_sem_data = self._repo.get_by_linkid(
                self._route,
                year=self._survey_year,
                raise_if_table_does_not_exists=True,
                semester=1
            )
            
            return self._prev_sem_data
        
        else:
            return self._prev_data 

    def side_columns_check(self, current_year_only: bool = False):
        if current_year_only:
            errors_ = self._events.incorrect_side_columns(survey_year=self._survey_year)
        else:
            errors_ = self._events.incorrect_side_columns()

        if len(errors_) == 0:
            return self
        
        errors = pl.DataFrame(
            errors_,
            infer_schema_length=None
        )

        na_msg = pl.format(
            "Segmen {}-{} pada sisi {} tidak memiliki nilai {}.",
            pl.col('from_sta'),
            pl.col('to_sta'),
            pl.col('side'),
            pl.col('column')
        )

        wrong_side_msg = pl.format(
            "Segmen {}-{} pada sisi {} tidak seharusnya memiliki nilai {}.",
            pl.col('from_sta'),
            pl.col('to_sta'),
            pl.col('side'),
            pl.col('column')
        )

        single_val_msg = pl.format(
            "Segmen {}-{} memiliki nilai unik {} lebih dari 1.",
            pl.col('from_sta'),
            pl.col('to_sta'),
            pl.col('column')
        )

        type_val_msg = pl.format(
            "Segmen {}-{} memiliki nilai {} yang tidak cocok dengan tipe pada {}.",
            pl.col('from_sta'),
            pl.col('to_sta'),
            pl.col('column'),
            pl.col('type_column')
        )

        error_msg_column = pl.when(
            pl.col('na')
        ).then(
            na_msg
        ).when(
            pl.col('wrong_side')
        ).then(
            wrong_side_msg
        ).when(
            pl.col('single_value').not_()
        ).then(
            single_val_msg
        ).when(
            pl.col('wrong_value_type')
        ).then(
            type_val_msg
        )

        messages = errors.with_columns(
            msg=error_msg_column
        ).select(
            'msg'
        )

        self._result.add_messages(
            messages,
            'error'
        )

        return self
    
    def road_type_spec_check(self):
        """
        Check segment with incorrect road type specification.
        """
        errors_ = self._events.incorrect_road_type_spec()

        if len(errors_) == 0:
            return self
        
        errors = pl.DataFrame(
            errors_
        ).select(
            msg=pl.format(
                "Segmen {}-{} memiliki spesifikasi yang tidak cocok dengan tipe jalan {}.",
                pl.col('from_sta'),
                pl.col('to_sta'),
                pl.col('road_type')
            )
        )

        self._result.add_messages(
            errors,
            'error',
            'force'
        )

        return self
    
    def inner_shoulder_check(self, current_year_only: bool = True):
        """
        Check segment with incorrect median and inner shoulder combination.
        """
        if current_year_only:
            errors_ = self._events.incorrect_inner_shoulder(survey_year=self._survey_year)
        else:
            errors_ = self._events.incorrect_inner_shoulder()

        if len(errors_) == 0:
            return self
        
        errors = pl.DataFrame(
            errors_
        ).select(
            msg=pl.when(
                pl.col('has_median')
            ).then(
                pl.format(
                    "Segmen {}-{} memiliki median, namun tidak memiliki bahu dalam.",
                    pl.col('from_sta'),
                    pl.col('to_sta')
                )
            ).otherwise(
                pl.format(
                    "Segmen {}-{} tidak memiliki median, namun memiliki bahu dalam",
                    pl.col('from_sta'),
                    pl.col('to_sta')
                )
            )
        )

        self._result.add_messages(
            errors,
            'error',
            'force'
        )

        return self
    
    def surface_width_check(self, width_delta: int = 2):
        """
        Check segment with surface width that does not match the total lane width.
        """
        errors_ = self._events.incorrect_surface_width(width_delta=width_delta)

        if len(errors_) == 0:
            return self
        
        errors = pl.DataFrame(
            errors_
        ).select(
            msg = pl.when(
                pl.col('has_median').not_()
            ).then(
                pl.format(
                    "Segmen {}-{} tidak memiliki median dan memiliki lebar perkerasan (surface width) yang tidak cocok dengan total lebar jalur.",
                    pl.col('from_sta'),
                    pl.col('to_sta')
                )
            ).otherwise(
                pl.format(
                    "Segmen {}-{} memiliki median dan memiliki lebar perkerasan (surface width) yang tidak cocok dengan total lebar jalur satu sisi.",
                    pl.col('from_sta'),
                    pl.col('to_sta')
                )
            )
        )

        self._result.add_messages(
            errors,
            'error',
            'force'
        )

        return self
    
    def single_value_attribute_check(self, column: str):
        """
        Check for segment with attribute N-Unique count above 1.
        """
        errors_ = self._events.segment_attribute_n_unique([column], filter=('gt', 1))

        if len(errors_) == 0:
            return self
        
        errors = pl.DataFrame(
            errors_
        ).select(
            msg=pl.format(
                "Segmen {}-{} memiliki nilai unik {} lebih dari 1.",
                pl.col('from_sta'),
                pl.col('to_sta'),
                pl.lit(column)
            )
        )

        self._result.add_messages(
            errors,
            'error',
            'force'
        )

        return self
    
    def decreasing_lane_width_check(self, tolerance=0.01):
        """
        Check segment with decreasing lane widths.
        """
        lane_w_col = self._events._lane_width_col

        joined = segments_join(
            left = self._events,
            right = self.prev_data,
            l_select=[lane_w_col],
            r_select=[lane_w_col]
        )

        errors = joined.filter(
            pl.col(lane_w_col).lt(pl.col(lane_w_col+'_r').sub(tolerance))
        ).select(
            msg=pl.format(
                "Segmen {}-{} {} mengalami penurunan lebar jalur dari {} ke {} dibandingkan data tahun lalu.",
                pl.col(self._events._from_sta_col).truediv(self._events.sta_conversion),
                pl.col(self._events._to_sta_col).truediv(self._events.sta_conversion),
                pl.col(self._events._lane_code_col),
                pl.col(lane_w_col + '_r'),
                pl.col(lane_w_col)
            )
        )

        self._result.add_messages(
            errors,
            'error',
            'force'
        )

        return self
    
    def decreasing_surf_width_check(self, tolerance=0.01):
        """
        Check segment with decreasing surface width if compared to previous year data.
        """
        surf_w_col = self._events._surf_width_col
        med_w_col = self._events._med_width_col

        rni_agg = [
            pl.col(surf_w_col).max().alias('surf_w_max'),
            pl.col(surf_w_col).sum().alias('surf_w_sum'),
            pl.col(med_w_col).max().gt(0).alias('has_median')
        ]

        joined = segments_join(
            left = self._events,
            right = self._prev_data,
            l_select = [surf_w_col, med_w_col],
            r_select = [surf_w_col, med_w_col],
            l_agg = rni_agg,
            r_agg = rni_agg
        )

        errors = joined.filter(
            pl.when(
                pl.col('has_median')
            ).then(
                pl.col('surf_w_sum').lt(pl.col('surf_w_sum' + '_r').sub(tolerance))
            ).otherwise(
                pl.col('surf_w_max').lt(pl.col('surf_w_max'+'_r').sub(tolerance))
            )
        ).select(
            msg=pl.when(
                pl.col('has_median')
            ).then(
                pl.format(
                    "Segmen {}-{} memiliki median dan mengalami penurunan lebar perkerasan dari {} ke {} dibandingkan data tahun lalu.",
                    pl.col(self._events._from_sta_col).truediv(self._events.sta_conversion),
                    pl.col(self._events._to_sta_col).truediv(self._events.sta_conversion),
                    pl.col('surf_w_sum'+'_r'),
                    pl.col('surf_w_sum')
                )
            ).otherwise(
                pl.format(
                    "Segmen {}-{} tidak memiliki median dan mengalami penurunan lebar perkerasan dari {} ke {} dibandingkan data tahun lalu.",
                    pl.col(self._events._from_sta_col).truediv(self._events.sta_conversion),
                    pl.col(self._events._to_sta_col).truediv(self._events.sta_conversion),
                    pl.col('surf_w_max'+'_r'),
                    pl.col('surf_w_max')
                )
            )
        )

        self._result.add_messages(
            errors,
            'error',
            'force'
        )

        return self
    
    def decreasing_lane_count(self):
        """
        Check for segment with decreasing number of lanes if compared to previous year data.
        """
        lane_code_col = self._events._lane_code_col

        joined = segments_join(
            left = self._events,
            right = self._prev_data,
            l_agg = [pl.col(lane_code_col).n_unique()],
            r_agg = [pl.col(lane_code_col).n_unique()]
        )

        errors = joined.filter(
            pl.col(lane_code_col).lt(pl.col(lane_code_col+'_r'))
        ).select(
            msg = pl.format(
                "Segmen {}-{} mengalami penurunan jumlah jalur dari {} ke {} dibandingkan data tahun lalu.",
                pl.col(self._events._from_sta_col).truediv(self._events.sta_conversion),
                pl.col(self._events._to_sta_col).truediv(self._events.sta_conversion),
                pl.col(lane_code_col+'_r'),
                pl.col(lane_code_col)
            )
        )

        self._result.add_messages(
            errors,
            'error',
            'force'
        )

        return self
    
    def paved_to_unpaved_check(self):
        """
        Check for segment which has surface type degradation compared to previous year data.
        """
        surf_type_col = self._events._surf_type_col

        # Combine available surface type from current and previous data.
        surf_mapping = pl.concat([
            self.prev_data.surface_types_mapping.select(
                ['surf_type', 'category']
            ),
            self._events.surface_types_mapping.select(
                ['surf_type', 'category']
            ).filter(
                pl.col('surf_type').is_in(
                    self.prev_data.surface_types_mapping['surf_type']
                ).not_()
            )
        ], how='vertical').to_dict()

        # Create dictionary for surface type mapping to category ('paved' or 'unpaved')
        surf_mapping_dict = {
            str(surf_mapping['surf_type'][_]): surf_mapping['category'][_] 
            for _ in range(len(surf_mapping['surf_type']))
        }

        # Start filtering error segments
        errors = segments_join(
            left = self._events,
            right = self.prev_data,
            l_select = [surf_type_col],
            r_select = [surf_type_col],
            l_agg = [pl.col(surf_type_col).max()],
            r_agg = [pl.col(surf_type_col).max()]
        ).with_columns(
            # The surface type needed to be casted to string
            current_cat=pl.col(surf_type_col).cast(pl.String).replace(surf_mapping_dict),
            prev_cat=pl.col(surf_type_col+'_r').cast(pl.String).replace(surf_mapping_dict)
        ).filter(
            pl.col('prev_cat').eq('paved').and_(
                pl.col('current_cat').eq('unpaved')
            )
        ).select(
            msg = pl.format(
                "Segmen {}-{} mengalami perubahan tipe perkerasan dari paved ke unpaved jika dibandingkan dengan data tahun lalu.",
                pl.col(self._events._from_sta_col).truediv(self._events.sta_conversion),
                pl.col(self._events._to_sta_col).truediv(self._events.sta_conversion)
            )
        )

        self._result.add_messages(
            errors,
            'error',
            'force'
        )

        return self
    
    def surface_year_check(self):
        """
        Generate error message for route with incorrect surface year.
        """
        errors_ = self._events.incorrect_surf_year()

        if len(errors_) == 0:
            return
        
        errors = pl.DataFrame(errors_).select(
            msg = pl.format(
                "Segmen {}-{} {} memiliki surface year yang lebih besar dari tahun data yaitu {}",
                pl.col('from_sta'),
                pl.col('to_sta'),
                pl.col('lane_code'),
                pl.col(self._events._surf_year_col.lower())
            )
        )

        self._result.add_messages(
            errors,
            'error'
        )

    def put_data(self, semester: int=2):
        """
        Delete and insert events data to geodatabase table.
        """
        self._repo.put(self._events, self._survey_year, semester)

    def base_validation(self):
        super().base_validation()

        self.segment_length_check(tolerance=0.005)
        self.surface_year_check()
        # self.side_columns_check()  # TEMPORARY DISABLED
        self.road_type_spec_check()
        self.inner_shoulder_check(current_year_only=True)  # TEMPORARY ONLY CHECK CURRENT YEAR DATA
        self.surface_width_check()
        self.single_value_attribute_check('VER_ALIGNMENT')
        self.single_value_attribute_check('HOR_ALIGNMENT')
        self.decreasing_lane_width_check()
        self.decreasing_surf_width_check()
        self.decreasing_lane_count()
        self.paved_to_unpaved_check()
