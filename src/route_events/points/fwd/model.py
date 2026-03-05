from ..base import RoutePointEvents
import os
from ...schema import RouteEventsSchema
from ...segments.rni import surface_types
import polars as pl
from pydantic import BaseModel, TypeAdapter
from typing import List


class SurfaceRange(BaseModel):
    """D0 validation range for a specific surface type."""
    upper: int
    lower: int


class ValidRange(BaseModel):
    """Valid D0 ranges by surface type (only rigid or asphalt)."""
    rigid: SurfaceRange
    asphalt: SurfaceRange


class RouteFWD(RoutePointEvents):
    """
    Model of route FWD points.
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
        Parse data from Excel file to Arrow format and load it into Route FWD object.
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
        self._d0_col:str = "FWD_D1"
        self._surf_thickness_col = 'SURF_THICKNESS'

    def valid_d0_range(self) -> ValidRange:
        """Return valid D0 ranges by surface type."""
        return ValidRange(
            rigid={'upper': 350, 'lower': 90},
            asphalt={'upper': 5000, 'lower': 0}
        )

    def valid_surface_thickness(self) -> ValidRange:
        """Return valid surface thickness ranges by surface type."""
        return ValidRange(
            rigid={'upper': 320, 'lower': 150},
            asphalt={'upper': 350, 'lower': 70}
        )

        