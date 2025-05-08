from ..base import RoutePointEvents
import os
from ...schema import RouteEventsSchema
import polars as pl
from pydantic import TypeAdapter
from typing import List


class RouteDefects(RoutePointEvents):
    """
    Model of route defect points.
    """
    @classmethod
    def from_excel(
        cls,
        excel_path: str,
        linkid: str | list = 'ALL',
        linkid_col: str = 'LINKID',
        ignore_review: bool = False,
        data_year: int = None
    ):
        """
        Parse data from Excel file to Arrow format and load it into Route Defect object.
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
        ).with_columns(
            pl.col(pl.String).str.to_uppercase()
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
        
        # Columns
        self._photo_url_cols = 'URL_PHOTO'
        self._surf_type_col = 'SURF_TYPE'
        self._defects_type_col = 'DEFECTS_TYPE'
        self._defects_dimension_col = 'DEFECTS_DIMENSION'

        self.lane_data = True
