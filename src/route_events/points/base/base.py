import pyarrow as pa
from typing import Literal, Union
import polars as pl
from ...geometry import Points, LAMBERT_WKT
from ...segments.base.utils import to_meter


class RoutePointEvents(object):
    def __init__(
            self,
            artable: pa.Table,
            route: str = None,
            data_year: int = None,
            data_semester: Literal[1,2] = None,
            sta_unit: str = 'dm',
            lane_data: bool = False
    ):
        # Default columsn
        self._linkid_col = 'LINKID'
        self._sta_col = 'STA'
        self._lane_code_col = 'LANE_CODE'
        self._lat_col = 'STA_LAT'
        self._long_col = 'STA_LONG'
        self._year_col = 'YEAR'
        self._semester_col = 'SEMESTER'

        # Units
        self._sta_unit = sta_unit

        # Data
        self.artable = artable
        self.lane_data = lane_data
        self._pl_df = pl.from_arrow(artable)
        self._data_year = data_year
        self._data_semester = data_semester
        self._route_id = route

        # Geometry
        self._points_4326 = None
        self._points_lambert = None

    @property
    def route_id(self) -> str:
        """
        Return events routeid
        """
        return self._route_id

    @property
    def pl_df(self) -> pl.DataFrame:
        """
        Events data in Polars DataFrame.
        """
        return self._pl_df
    
    @property
    def ldf(self) -> pl.LazyFrame:
        """
        Events data in Polars LazyDataFrame.
        """
        return self.pl_df.lazy()
    
    @property
    def semester(self) -> int:
        """
        Data semester.
        """
        return self._data_semester
    
    @property
    def points_4326(self) -> Points:
        """
        Points object
        """
        if not self.lane_data:
            selection = [
                self._linkid_col,
                self._sta_col,
                self._long_col,
                self._lat_col
            ]

            ids_column = [
                self._linkid_col,
                self._sta_col
            ]

        else:
            selection = [
                self._linkid_col,
                self._sta_col,
                self._lane_code_col,
                self._long_col,
                self._lat_col
            ]

            ids_column = [
                self._linkid_col,
                self._sta_col,
                self._lane_code_col
            ]

        if self._points_4326 is None:
            self._points_4326 = Points(
                self.pl_df.select(selection),
                long_col = self._long_col,
                lat_col = self._lat_col,
                wkt='EPSG:4326',
                ids_column=ids_column
            )

            return self._points_4326
        else:
            return self._points_4326
        
    @property
    def points_lambert(self) -> Points:
        """
        Transformed Points object using LAMBERT WKT
        """
        if self._points_lambert is None:
            self._points_lambert = self._points_4326.transform(
                LAMBERT_WKT,
                invert=True
            )
        else:
            return self._points_lambert
        
    @property
    def no_data(self) -> bool:
        """
        Return True if data is empty.
        """
        return self.pl_df.is_empty()

    @property
    def sta_conversion(self):
        return to_meter[self._sta_unit]
    
    @property
    def max_sta(self) -> Union[int, float]:
        """
        Maximum STA number
        """
        return self.pl_df[self._sta_col].max()
    
    @property
    def min_sta(self) -> Union[int, float]:
        """
        Minimum STA number
        """
        return self.pl_df[self._sta_col].min()
    
    @property
    def sta_unit(self) -> str:
        """
        STA unit
        """
        return self._sta_unit
    
    @sta_unit.setter
    def sta_unit(self, unit: str):
        if unit not in to_meter:
            raise ValueError(f"Unit '{unit}' is invalid or unsupported.")
        
        self._sta_unit = unit

    def correct_data_year(self) -> bool:
        """
        Check if all row contain the same data year with the data year from __init__ argument.
        """
        return self.pl_df.filter(
            pl.col(self._year_col) != self._data_year
        ).is_empty()
    
    def correct_data_semester(self) -> bool:
        """
        Check if all row contain the same semester with the semester from __init__ argument.
        """
        return self.pl_df.filter(
            pl.col(self._semester_col) != self._data_semester
        ).is_empty()
