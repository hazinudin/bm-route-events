from route_events.segments import RouteSegmentEvents
from route_events import LRSRoute
from ...validation_result.result import ValidationResult
from sqlalchemy import Engine
from typing import Type, Literal, Union
import polars as pl
from numpy import isclose


class RouteSegmentEventsValidation(object):
    """
    Route segment events validation
    """
    def __init__(
            self,
            events: Type[RouteSegmentEvents],
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
        self._survey_sem = survey_semester

        # M Value DataFrame
        self._df_lrs_mv = None

        # Distance to LRS DataFrame
        self._df_lrs_dist = None

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
    def df_lrs_mv(self) -> pl.DataFrame:
        """
        Calculate M-Value for every input point and segment M-Value diff to the next segment.
        """
        if self._df_lrs_mv is None:
            df = self._lrs.get_points_m_value(
                self._events._points_lambert
            ).sort(
                [
                    self._events._linkid_col,
                    self._events._from_sta_col,
                    self._events._to_sta_col,
                    self._events._lane_code_col
                ]
            )

            lanes = []

            for lane in self._events.lanes:
                lane_df = df.lazy().filter(
                    pl.col(self._events._lane_code_col) == lane
                ).select(
                    self._events._linkid_col,
                    self._events._from_sta_col,
                    self._events._to_sta_col,
                    self._events._lane_code_col,
                    'm_val',
                    'dist',
                    diff=pl.col('m_val').diff(),
                )

                lanes.append(lane_df)
        
            ldf = pl.concat(lanes, how='vertical')
            
            self._df_lrs_mv = ldf.collect()

            return self._df_lrs_mv
        else:
            return self._df_lrs_mv
        
    @property
    def df_lrs_dist(self) -> pl.DataFrame:
        """
        Calculate distance from input points to LRS geometry.
        """
        if self._df_lrs_dist is None:
            self._df_lrs_dist = self._lrs.distance_to_points(
                self._events.points_lambert
            )
            return self._df_lrs_dist
        else:
            return self._df_lrs_dist
        
    def lrs_validation(self):
        """
        Run all LRS validation functions.
        """
        self.lrs_distance_check()
        self.lrs_monotonic_check()
        self.lrs_direction_check()
        self.lrs_segment_length_check()
        self.lrs_sta_check()
        self.max_sta_check()

    def base_validation(self):
        """
        Run all validation function in this class.
        """
        self.duplicate_segment_check()
        self.lane_sequence_check()
        # self.segment_length_check(tolerance=0.005)
        self.sta_diff_check(tolerance=0.005)
        self.sta_gap_check()
        self.sta_overlap_check()
        self.survey_max_m_value_check()
        self.from_sta_start_from_zero()
        # self.survey_date_year_check()
        # self.data_semester_check()
        # self.data_year_check()

        # Run all LRS Validation
        self.lrs_validation()

    def survey_date_year_check(self):
        """
        Check if the year in survey date is consistent with the data year.
        """
        if not self._events.correct_survey_date_year():
            self._result.add_message(
                f"Data input memiliki tanggal survey ({self._events._survey_date_col}) dengan tahun yang bukan {self._events._data_year}",
                'error',
                'force'
            )
        
        return self

    def lrs_distance_check(self, threshold=30):
        """
        Check every segments nearest distance to LRS geometry, if the distance exceeds the threshold (in meters).
        Then an error message will be created.
        """
        errors = self.df_lrs_mv.filter(
            pl.col('dist') >= threshold
        ).with_columns(
            msg= pl.format(
                "Segmen {}-{} {} pada ruas {} berjarak lebih dari {}m dari geometri LRS, yaitu {}m",
                pl.col(self._events._from_sta_col),
                pl.col(self._events._to_sta_col),
                pl.col(self._events._lane_code_col),
                pl.col(self._events._linkid_col),
                pl.lit(threshold),
                pl.col('dist')
            )
        )

        self._result.add_messages(
            errors.select('msg'),
            'error',
            'force'
        )
        
        return self

    def lrs_monotonic_check(self):
        """
        Check every lane for monotonicity in M-Value compared to its STA.
        """
        df = self.df_lrs_mv
        
        # Check for non-monotonic errors
        m_errors = df.filter(
            pl.col('diff').lt(0)
        ).with_columns(
            msg=pl.format(
                "Segmen {}-{} {} tidak sesuai dengan arah geometri ruas.",
                pl.col(self._events._from_sta_col),
                pl.col(self._events._to_sta_col),
                pl.col(self._events._lane_code_col)
            )
        ).select(
            pl.col('msg')
        )

        # Append error messages
        self._result.add_messages(
            m_errors, 
            'error',
            'force'
        )

        return self

    def lrs_direction_check(self):
        """
        Check survey direction (the STA increment direction) compared to LRS.
        """
        # Check if all diff is negative, meaning the entire STA is flipped.
        df = self.df_lrs_mv

        dir_error = df.with_columns(
            pl.col('diff').fill_null(0)  # Fill null with zero, fix for lane with only 1 segment.
        ).group_by([
            self._events._linkid_col,
            self._events._lane_code_col
        ]).agg(
            wrong_dir=pl.col('diff').lt(0).all()
        ).filter(
            pl.col('wrong_dir')
        ).with_columns(
            msg=pl.format(
                "Ruas {} pada lane {} memiliki arah survey yang terbalik.",
                pl.col(self._events._linkid_col),
                pl.col(self._events._lane_code_col)
            )
        ).select(
            pl.col('msg')
        )

        # Append error messages
        self._result.add_messages(
            dir_error, 
            'error',
            'force'
        )

        return self
    
    def lrs_segment_length_check(self, tolerance=20):
        """
        Compare each segment M-Value with its FROM-TO STA difference.
        """
        df = self.df_lrs_mv

        errors = df.join(
            self._events.pl_df.select(
                [
                    self._events._linkid_col,
                    self._events._from_sta_col,
                    self._events._to_sta_col,
                    self._events._lane_code_col,
                    self._events._seg_len_col
                ]
            ),
            on=[
                self._events._linkid_col,
                self._events._from_sta_col,
                self._events._to_sta_col,
                self._events._lane_code_col
            ],
            how='left'
        ).filter(
            pl.col('diff').gt(pl.col(self._events._seg_len_col).mul(self._events.seg_len_conversion).add(tolerance)) |
            pl.col('diff').lt(pl.col(self._events._seg_len_col).mul(self._events.seg_len_conversion).sub(tolerance))
        ).with_columns(
            msg=pl.format(
                "Segmen {}-{} {} memiliki panjang segmen {}m, sedangkan jarak dengan koordinat sebelumnya {}m.",
                pl.col(self._events._from_sta_col),
                pl.col(self._events._to_sta_col),
                pl.col(self._events._lane_code_col),
                pl.col(self._events._seg_len_col).mul(self._events.seg_len_conversion),
                pl.col('diff')
            )
        ).select(
            'msg'
        )

        # Append error messages
        self._result.add_messages(
            errors,
            'error',
            'force'
        )

        return self
    
    def lrs_sta_check(
            self, 
            sta: Literal['from', 'to'] = 'to',
            tolerance: int = 30
        ):
        """
        Compare survey point M-Value with its STA
        """
        df = self.df_lrs_mv

        if sta == 'to':
            sta_col = self._events._to_sta_col
        elif sta == 'from':
            sta_col = self._events._from_sta_col
        else:
            raise ValueError(f"Only accept 'from' or 'to' sta type. Got {sta} instead.")

        errors = df.filter(
            pl.col(sta_col).mul(
                self._events.sta_conversion
            ).gt(
                pl.col('m_val').add(tolerance)
            ) |
            pl.col(sta_col).mul(
                self._events.sta_conversion
            ).lt(
                pl.col('m_val').sub(tolerance)
            )
        ).select(
            msg = pl.format(
                "Segmen {}-{} {} memiliki nilai STA LRS yang tidak cocok, yaitu {}.",
                pl.col(self._events._from_sta_col),
                pl.col(self._events._to_sta_col),
                pl.col(self._events._lane_code_col),
                pl.col('m_val')
            )
        )

        self._result.add_messages(
            errors,
            'error',
            'force'
        )

        return self
    
    def duplicate_segment_check(self):
        """
        Check for duplicate segment.
        """
        errors_ = self._events.is_duplicate_segment()

        if len(errors_) == 0:
            return self
        
        errors = pl.DataFrame(
            self._events.is_duplicate_segment()
        ).select(
            msg = pl.format(
                "Segmen {}-{} {} merupakan segmen dengan duplikat.",
                pl.col('from_sta'),
                pl.col('to_sta'),
                pl.col('lane')
            )
        )

        self._result.add_messages(
            errors,
            'error'
        )

        return self
    
    def lane_sequence_check(self):
        """
        Check for incorrect lane sequence. All segment should have lanes that start from L1 or R1,
        and has 1 increment.
        """
        errors_ = self._events.incorrect_lane_sequence()

        if len(errors_) == 0:
            return self
        
        errors = pl.DataFrame(
            errors_
        ).select(
            msg = pl.format(
                "Segmen {}-{} memiliki kode lajur yang tidak sesuai dengan aturan, yaitu {}",
                pl.col('from_sta'),
                pl.col('to_sta'),
                pl.col('lanes').cast(pl.List(pl.String)).list.join(", ")
            )
        )

        self._result.add_messages(
            errors,
            'error'
        )

        return self
    
    def segment_length_check(self, tolerance=0):
        """
        Check segment with incorrect segment length. Exclude the last segment, 
        because last segment could have short segment length.
        """
        errors_ = self._events.incorrect_segment_length(tolerance=tolerance)

        if len(errors_) == 0:
            return self
        
        errors = pl.DataFrame(
            errors_
        ).select(
            msg = pl.format(
                "Segmen {}-{} {} memiliki panjang segmen yang tidak sesuai dengan kriteria, yaitu {}",
                pl.col('from_sta'),
                pl.col('to_sta'),
                pl.col('lane'),
                pl.col(self._events._seg_len_col.lower())
            )
        )

        self._result.add_messages(
            errors,
            'error'
        )

        return self
    
    def sta_diff_check(self, tolerance=0):
        """
        Check segment with incorrect STA difference, compared to its segment length.
        """
        errors_ = self._events.incorrect_sta_diff(tolerance=tolerance)

        if len(errors_) == 0:
            return self
        
        errors = pl.DataFrame(
            errors_
        ).select(
            msg = pl.format(
                "Segmen {}-{} {} memiliki nilai FROM_STA yang lebih besar dari TO_STA, atau selisih FROM-TO yang tidak cocok dengan panjang segmen, yaitu {}",
                pl.col('from_sta'),
                pl.col('to_sta'),
                pl.col('lane'),
                pl.col(self._events._seg_len_col.lower())
            )
        )

        self._result.add_messages(
            errors,
            'error',
            'force'
        )

        return self
    
    def sta_gap_check(self):
        """
        Check if there is measurement gap in each lane.
        """
        errors_ = self._events.sta_gap()

        if len(errors_) == 0:
            return self
        
        errors = pl.DataFrame(
            errors_
        ).filter(
            # For main lane (R1 and L1) missing data is not negotiable
            pl.col('lane').is_in(['L1', 'R1', 'l1', 'r1'])
        ).select(
            msg = pl.format(
                "Tidak ditemukan data survey dari STA {} hingga {} pada lane {}.",
                pl.col('from_sta'),
                pl.col('to_sta'),
                pl.col('lane')
            )
        )

        self._result.add_messages(
            errors,
            'error',
            'force'
        )

        errors = pl.DataFrame(
            errors_
        ).filter(
            # For other lanes, missing data is negotiable
            pl.col('lane').is_in(['L1', 'R1', 'l1', 'r1']).not_()
        ).select(
            msg = pl.format(
                "Tidak ditemukan data survey dari STA {} hingga {} pada lane {}.",
                pl.col('from_sta'),
                pl.col('to_sta'),
                pl.col('lane')
            )
        )

        self._result.add_messages(
            errors,
            'error',
            'force'
        )

        return self
    
    def sta_overlap_check(self):
        """
        Check if there is overlapping segment.
        """
        errors_ = self._events.overlapping_segments()

        if len(errors_) == 0:
            return self
        
        errors = pl.DataFrame(
            errors_
        ).select(
            msg = pl.format(
                "Segmen {}-{} {} tumpang tindih dengan segmen {}-{} {}",
                pl.col('from_sta'),
                pl.col('to_sta'),
                pl.col('lane'),
                pl.col('overlapped').struct.field('from_sta'),
                pl.col('overlapped').struct.field('to_sta'),
                pl.col('overlapped').struct.field('lane')
            )
        )

        self._result.add_messages(
            errors,
            'error'
        )

        return self
    
    def max_sta_check(self):
        """
        Compare events max FROM_STA with LRS max M-Value.
        """
        if (
            self._events.max_from_sta*self._events.sta_conversion
            ) < self._lrs.max_m_value-100:
            msg = f"Tidak ditemukan data survey dari STA {self._events.max_to_sta*self._events.sta_conversion}m hingga {self._lrs.max_m_value}m"

            self._result.add_message(
                msg,
                'error',
                'force'
            )
        
        return self
        
    def survey_max_m_value_check(self, tolerance=30):
        """
        Compare events max M-Value with LRS max M-Value, to confirm survey is done until the end of a route.
        """
        if not isclose(
            self.df_lrs_mv['m_val'].max(),
            self._lrs.max_m_value,
            atol=tolerance
        ) and (
            self._df_lrs_mv['m_val'].max() < self._lrs.max_m_value
        ):
            msg = "Data survey tidak mencakup keseluruhan geometri LRS."

            self._result.add_message(
                msg,
                'error',
                'force'
            )

        return self
    
    def from_sta_start_from_zero(self):
        """
        Check if first segment starts from 0.
        """
        if self._events.min_from_sta != 0:
            msg = f"Data tidak dimulai dari FROM STA = 0, melainkan {self._events.min_from_sta}"

            self._result.add_message(
                msg,
                'error'
            )

        return self

    def data_year_check(self):
        """
        Check consistency between input data year and year in survey data.
        """
        if not self._events.correct_data_year():
            msg = f"Data survey memiliki tahun yang berbeda dengan {self._survey_year}"

            self._result.add_message(
                msg,
                'error'
            )

        return self
    
    def data_semester_check(self):
        """
        Check consistency between input data semester and semester in survey data.
        """
        if not self._events.correct_data_semester():
            msg = f"Data survey memiliki semester yang berbeda dengan {self._survey_sem}"

            self._result.add_message(
                msg,
                'error'
            )
        
        return self
    
    def smd_output_msg(self, show_all_msg: bool = False, as_dict: bool = True) -> Union[str | dict]:
        """
        Return SMD output message format in either Python dictionary or JSON formatted string.
        """
        return self._result.to_smd_format(
            show_all_msg=show_all_msg,
            as_dict=as_dict
        )
