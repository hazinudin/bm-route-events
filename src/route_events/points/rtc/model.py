from ..base import RoutePointEvents
from ...schema import RouteEventsSchema
from pydantic import TypeAdapter
import polars as pl
import os
from typing import List


# Default column names
VEH1_COL: str = "NUM_VEH1"
VEH2_COL: str = "NUM_VEH2"
VEH3_COL: str = "NUM_VEH3"
VEH4_COL: str = "NUM_VEH4"
VEH5A_COL: str = "NUM_VEH5A"
VEH5B_COL: str = "NUM_VEH5B"
VEH6A_COL: str = "NUM_VEH6A"
VEH7A_COL: str = "NUM_VEH7A"
VEH7B_COL: str = "NUM_VEH7B"
VEH7C_COL: str = "NUM_VEH7C"
VEH8_COL: str = "NUM_VEH8"


class RouteRTC(RoutePointEvents):
    """
    Model of route RTC (traffic counting)
    """
    @classmethod
    def from_excel(
        cls,
        excel_path: str,
        linkid: str | list = 'ALL',
        linkid_col: str = 'LINKID',
        ignore_review: bool = False,
        data_year: int = None,
    ):
        """
        Parse data from Excel file to Arrow format and load it into RouteRTC object.
        """
        if type(linkid) is not str:
            raise TypeError(f"Only accept single route in str type, not {linkid}")
        
        if linkid == 'ALL':
            route_filter = pl.col(linkid_col).is_not_null()
        else:
            route_filter = pl.col(linkid_col).eq(linkid)
        
        config_path = os.path.dirname(__file__) + '/schema.json'

        schema = RouteEventsSchema(
            file_path=config_path,
            ignore_review_err=ignore_review
        )

        df_str = pl.read_excel(
            excel_path,
            engine='calamine',
            infer_schema_length=None
        ).rename(
            str.upper
        ).cast(
            pl.String
        )

        ta = TypeAdapter(List[schema.model])
        df = pl.DataFrame(
            ta.validate_python(df_str.to_dicts()),
            infer_schema_length=None
        ).filter(
            route_filter
        )

        return cls(
            artable=df.to_arrow(),
            route=linkid,
            data_year=data_year,
            lane_data=True
        )
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Traffic data is not lane based data
        self.lane_data = False
        self._timestamp_col = '_timestamp'

        # Default columns
        self._hour_col: str = 'SURVEY_HOURS'
        self._min_col: str= 'SURVEY_MINUTE'

    @property
    def df_with_timestamp(self) -> pl.DataFrame:
        """
        Return the input data as Polars DataFrame with calculated survey timestamp (Polars Datetime)
        """
        return self.pl_df.with_columns(
            **{
                self._timestamp_col: pl.datetime(
                    year=pl.col(self._surv_date_col).dt.year(),
                    month=pl.col(self._surv_date_col).dt.month(),
                    day=pl.col(self._surv_date_col).dt.day(),
                ).dt.offset_by(
                    pl.format("{}h", pl.col(self._hour_col))
                ).dt.offset_by(
                    pl.format("{}m", pl.col(self._min_col))
                )
            }
        )
        
    def invalid_interval(self, interval:int=15):
        """
        Find rows with invalid survey interval, the default survey interval is 15min
        """
        return
    
    def invalid_timestamp(self):
        """
        Find rows with invalid timestamp.
        """
        return
    
    def survey_duration(self) -> int:
        """
        Get the duration of the surveys in minutes.
        """
        return
