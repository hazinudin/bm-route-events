import duckdb
from route_events.geometry import LAMBERT_WKT
import copy
import pyarrow as pa
import polars as pl
from typing import Union, List

class Point(object):
    def __init__(self, long, lat, wkt:str, ddb=None):
        """
        Model of a point geometry
        """
        self.X = long
        self.Y = lat

        self.origin_wkt = wkt

        if ddb is None:
            self.dconn = duckdb.connect()
        else:
            self.dconn = ddb

        # self._pt = 'point_table'
        self._pt_col = 'point'
        self._lon = 'long'
        self._lat = 'lat'
        self.dconn.sql("install spatial; load spatial;")  # Load spatial extension

    @property
    def _pt(self):
        """
        Create select statement for point table.
        """
        return f"(select ST_Point({self.Y}, {self.X}) as {self._pt_col})"
    
    def _transform(self, target_wkt:str, invert=False):
        """
        Transformation.
        """
        transform = self.dconn.sql(f"""
                       with transformed as
                       (
                       select ST_Transform({self._pt_col}, '{self.origin_wkt}', '{target_wkt}') as point, *
                       from {self._pt}
                       )

                       -- select from transfomed
                       select ST_X(point) as {self._lon}, ST_Y(point) as {self._lat}, * from transformed
                       """)
        
        return transform
        
    def transform(self, target_wkt:str, invert=False):
        """
        Transform coordinate to target WKT.
        """
        transformed = self._transform(target_wkt, invert).fetchall()
        X = transformed[0][0]
        Y = transformed[0][1]

        if not invert:
            return Point(X, Y, target_wkt, ddb=self.dconn)
        else:
            return Point(Y, X, target_wkt, ddb=self.dconn)
    
    def invert(self):
        """
        Invert the lon, lat from (X,Y to Y,X and vice versa)
        """
        _y = copy.deepcopy(self.Y)
        _x = copy.deepcopy(self.X)

        self.X = _y
        self.Y = _x

        return self

    def create_buffer(self, radius: float)->str:
        """
        Create buffer geometry around the point at the target distance.
        Returns a GeoJSON representation of the buffer polygon.
        """
        buffer_query = f"""select ST_AsGeoJSON(ST_Buffer({self._pt_col}, {radius})) from {self._pt}"""
        buffer = self.dconn.sql(buffer_query)

        return buffer.fetchall()[0][0]
    
    def _calculate_distance(self, other_point):
        """
        Calculate distance
        """
        if type(other_point) != Point:
            raise TypeError("Only accepts Point type object.")
        
        distance_query = f"""select ST_Distance(ST_Point({other_point.Y}, {other_point.X}), {self._pt_col}) as dist, * from {self._pt}"""
        distance = self.dconn.sql(distance_query)

        return distance
    
    def distance_to(self, other_point)->float:
        """
        Calculate distance to other Point geometry object. Return distance in meters.
        """
        distance = self._calculate_distance(other_point)

        return distance.fetchall()[0][0]
    

class Points(Point):
    def __init__(
            self, 
            data: Union[pa.Table | pl.DataFrame], 
            long_col: str, 
            lat_col: str,
            wkt:str, 
            ddb=None,
            ids_column: List[str] = []
        ):
        """
        Model of multiple point geometry
        """
        super().__init__(long=long_col, lat=lat_col, wkt=wkt, ddb=ddb)
        self._rows = data
        self.__pt = 'points_table'
        self._ids = ids_column

        # Load H3 index extension
        self.dconn.sql("install h3 from community; load h3;")

        self.dconn.sql(
            f"""
            create TEMP TABLE {self.__pt} as 
            (
            select *, ST_Point({self.Y}, {self.X}) as {self._pt_col} 
            from data
            )
            """
        )

    def first(self) -> Point:
        """
        Fetch the first row as Point object.
        """
        return Point(
            self._rows[self.X][0],
            self._rows[self.Y][0],
            self.origin_wkt,
            self.dconn,
        )

        
    def h3_indexed(self, resolution: int, index_only=False) -> pl.DataFrame:
        """
        Create H3 grid ID column to the point table.
        """
        if index_only:
            query = f"""
            select
            h3_h3_to_string(
            h3_latlng_to_cell(
            ST_X({self._pt_col}), ST_Y({self._pt_col}), {resolution}
            )
            ) as h3
            from {self._pt}
            """
        else:
            query = f"""
            select
            ST_Y({self._pt_col}) as {self.X},
            ST_X({self._pt_col}) as {self.Y}, 
            h3_h3_to_string(
            h3_latlng_to_cell(
            ST_X({self._pt_col}), ST_Y({self._pt_col}), {resolution}
            )
            ) as h3
            from {self._pt}
            """

        return self.dconn.sql(query).pl()
    
    def transform(self, target_wkt:str, invert=False):
        """
        Transform coordinates to target WKT.
        """
        df = self._transform(target_wkt, invert).pl().select(
            self._ids + [self._lon, self._lat]
        ).rename(
            {
                self._lon: self.X, 
                self._lat: self.Y
            }
        )
        if invert:
            return Points(df, long_col=self.Y, lat_col=self.X, wkt=LAMBERT_WKT,
                          ids_column=self._ids)
        else:
            return Points(df, long_col=self.X, lat_col=self.Y, wkt=LAMBERT_WKT,
                          ids_column=self._ids)
    
    def distance_to_point(self, other_point: Point):
        """
        Calculate distance to other Point geometry object. Return distance in meters.
        """
        return self._calculate_distance(other_point)
    
    def nearest(self, other_point: Point):
        """
        Find nearest row from ```other_point```.
        """
        row = self._calculate_distance(other_point).pl().sort('dist').head(1)
        return row
    
    def create_buffer(self):
        raise AttributeError("Method 'create_buffer' not available.")
    
    @property
    def _pt(self):
        return self.__pt
    