import json
import route_events.route.repo.lrs_pb2 as lrs_pb2
import route_events.route.repo.lrs_pb2_grpc as lrs_pb2_grpc
import grpc
import pyarrow as pa
import duckdb
from route_events.geometry import LAMBERT_WKT
from ..geometry.point import Points
import polars as pl


class LRSRoute(object):
    @classmethod
    def from_feature_service(cls, grpc_host: str, route: str):
        """
        Get LRS features from GRPC service.
        """
        with grpc.insecure_channel(grpc_host, options=[
        ('grpc.max_send_message_length', 8188254),
        ('grpc.max_receive_message_length', 8188254),
        ]) as channel:
            stub = lrs_pb2_grpc.RoadNetworkStub(channel)
            request = lrs_pb2.RouteRequests(routes=[route])
            data = stub.GetByRouteId(request)
        
        # Check if the response is empty.
        features = json.loads(data.geojson)['features']

        if len(features) == 0:
            return None
        else:
            return cls.from_geojson(data.geojson)
        
    @classmethod
    def from_geojson_file(
        cls,
        json_file: str,
        linkid_col: str = "LINKID"
    ):
        """
        Load a GeoJSON file and deserialize it into LRSRoute object.
        """
        with open(json_file) as jf:
            return cls.from_geojson(
                jf.read(), 
                linkid_col=linkid_col
            )
        
    @classmethod
    def from_geojson(
        cls, 
        json_str: str,
        linkid_col: str = "LINKID"
    ):
        """
        Deserialize geojson into LRSRoute object.
        """
        gjson = json.loads(json_str)

        schema = pl.Schema({
            linkid_col: pl.String(),
            "LAT": pl.Float64(),
            "LONG": pl.Float64(),
            "MVAL": pl.Float64(),
            "VERTEX_SEQ": pl.Int64()
        })

        # Parse geojson into lists
        for feature in gjson['features']:
            geom_type = feature['geometry']['type']
            routeid = feature['properties'][linkid_col]
            geom_array = feature['geometry']['coordinates']

            linkid_rows = list()
            lat_rows = list()
            long_rows = list()
            m_rows = list()
            vertex_seq = list()

            if geom_type == 'MultiLineString':
                _seq = 0
                for part in geom_array:
                    for vertex in part:
                        linkid_rows.append(routeid)
                        long_rows.append(vertex[1])  # Get the first
                        lat_rows.append(vertex[0])  # Get the second
                        m_rows.append(vertex[-1])  # Get the last one, to prevent fetching the Z value.
                        vertex_seq.append(_seq)  # Append the vertex sequence in linestring

                        _seq = _seq + 1
            
            elif geom_type == 'LineString':
                _seq = 0
                for vertex in geom_array:
                    linkid_rows.append(routeid)
                    long_rows.append(vertex[1])
                    lat_rows.append(vertex[0])
                    m_rows.append(vertex[-1])
                    vertex_seq.append(_seq)  # Append the vertex sequence in linestring

                    _seq = _seq + 1
        
        # Polars DataFrame containing all vertex
        df = pl.DataFrame(
            [linkid_rows, long_rows, lat_rows, m_rows, vertex_seq],
            schema=schema
        )

        return cls(
            df=df,
            properties=gjson['features'][0]['properties'] 
        )
    
    def __init__(
            self,
            df: pl.DataFrame,
            properties: dict = None
        ):
        
        # Define class field name
        self.linkid_col = 'LINKID'
        self.lat_col = 'LAT'
        self.long_col = 'LONG'
        self.mval_col = 'MVAL'
        self.seq_col = 'VERTEX_SEQ'
        self.road_stat_col = 'ROAD_STATUS'
        self.road_fn_col = 'ROAD_FUNCTION'

        # LRS properties
        self._properties = properties

        self.df = df
        self.artable = df.to_arrow()

        # Load the data to duckdb table
        self.dconn = duckdb.connect()  # DuckDB connection
        _lrs_table = self.artable  # pointer
        self.lrs_line_table = 'lrs_line'
        self.lrs_point_table = 'lrs_point_table'

        self.dconn.sql("install spatial; load spatial;")  # Load spatial and H3 extension

        # Convert the M-Value from kilometers to meters.
        # Create line and point table in DuckDB

        # Point table which includes H3 index
        self._h3_res = 8

        self.dconn.sql(f"""
                       create temp table {self.lrs_point_table} as
                       select
                       {self.linkid_col},
                       {self.mval_col}*1000 as {self.mval_col},
                       {self.seq_col},  
                       ST_Transform(
                       ST_Point({self.lat_col}, {self.long_col}),
                       'EPSG:4326',
                       '{LAMBERT_WKT}'
                       ) as point
                       from _lrs_table
                       """)

        self.dconn.sql(f"""
                       create temp table {self.lrs_line_table} as 
                       select {self.linkid_col}, 
                       ST_MakeLine(
                       list(point)
                       ) as linestr
                       from {self.lrs_point_table} 
                       group by {self.linkid_col}"""
                       )

    @property
    def status(self):
        """
        Road status of the route.
        """
        return self._properties[self.road_stat_col]
    
    @property
    def function(self):
        """
        Road function of the route.
        """
        return self._properties[self.road_fn_col]
    
    @property
    def max_m_value(self) -> float:
        return self.df[self.mval_col].max()
    
    def distance_to_point(self, long: float, lat: float):
        """
        Calculate nearest distance (in meters) from input coordinate to LRS geometry.
        """        
        nearest_dist = self.dconn.sql(
            f"""
            select 
            ST_Distance(
                ST_Transform(ST_Point({lat}, {long}), 'EPSG:4326', '{LAMBERT_WKT}'), linestr
            )
            as dist, {self.linkid_col} 
            from {self.lrs_line_table}
            """
        )

        return nearest_dist.fetchall()[0][0]  # Return the distance in meters
    
    def distance_to_points(self, points: Points) -> pl.DataFrame:
        """
        Calculate nearest distance of Points to LRS geometry.
        """
        df = points._rows  # Pointer for duckdb

        distances = self.dconn.sql(
            f"""
            select
            a.*,  
            ST_Distance(
                ST_Point({points.Y}, {points.X}), linestr
            ) as dist
            from df a
            cross join {self.lrs_line_table}
            """
        )

        return distances.pl()
    
    def get_points_m_value(self, points: Points, unit='m'):
        """
        Get M value of input points on LRS route geometry.
        """
        points_row = pl.concat(
            [
                points._rows,
                pl.DataFrame([x for x in range(points._rows.shape[0])], schema={"point_id": pl.Int64})
            ],
            how='horizontal'
            )
        
        lrs_segment = self.dconn.sql(
            f'select * exclude(point), ST_X(point) as x, ST_Y(point) as y from {self.lrs_point_table}'
        ).pl().with_columns(
            x1=pl.col('x').shift(-1),
            y1=pl.col('y').shift(-1),
            MVAL1=pl.col('MVAL').shift(-1)
        ).filter(
            pl.col('x1').is_not_null()
        ).with_columns(
            m=(pl.col('y1')-pl.col('y'))/(pl.col('x1')-pl.col('x'))
        ).with_columns(
            c=pl.col('y')-(pl.col('m')*pl.col('x'))
        )

        vertex_on_line = self.dconn.sql(
            f"""
            -- SHORTEST LINE TO LRS LINE QUERY --
            with shortest_to_lrs as
            (
            select
            point_id,
            ST_ShortestLine(
                ST_Transform(ST_Point({points.Y}, {points.X}), '{points.origin_wkt}', '{LAMBERT_WKT}'), linestr
            ) as shortestline
            from points_row
            cross join
            {self.lrs_line_table}
            ),
            
            -- END POINT FROM SHORTESTLINE--
            point_on_line as
            (
            select
            point_id, 
            ST_EndPoint(shortestline) as lambert_vertex,
            ST_Length(shortestline) as dist,
            ST_Transform(ST_EndPoint(shortestline), '{LAMBERT_WKT}', 'EPSG:4326') as point_4326
            from shortest_to_lrs
            ),

            --NEAREST VERTEX TO ON-LINE POINT--
            nearest_vertex as
            (
            select point_id, dist as dist_to_line, lambert_vertex as on_line, MVAL, MVAL1, x, y, x1, y1
            from point_on_line a
            inner join lrs_segment b
            on ST_Y(lambert_vertex) <= greatest(y, y1) and ST_Y(lambert_vertex) >= least(y, y1)
            and ST_X(lambert_vertex) <= greatest(x, x1) and ST_X(lambert_vertex) >= least(x, x1)
            )

            select
            point_id,
            dist_to_line,
            (MVAL1 - MVAL) as m_delta,
            MVAL as first_m,
            (ST_Distance(ST_Point(x, y), ST_Point(x1, y1))) as dist,
            (ST_Distance(ST_Point(x, y), on_line)) as inter_dist
            from nearest_vertex
            """
        )

        df_m_val = vertex_on_line.pl().with_columns(
            m_val = (pl.col('m_delta')/pl.col('dist')*pl.col('inter_dist'))+pl.col('first_m')
        ).select(
            ['point_id', 'm_val', 'dist_to_line']
        ).rename(
            {'dist_to_line': 'dist'}
        )

        return points_row.join(
            df_m_val, on='point_id'
        )
    
    def get_point_m_value(self, long: float, lat: float, unit='m'):
        """
        Get M value of input point on LRS route geometry.
        """
        # Snapped vertex from the input point on the LRS geometry.
        vertex_on_line = self.dconn.sql(
            f"""
            with point_on_line as
            (
            select
            {self.linkid_col}, 
            ST_EndPoint
            (
            ST_ShortestLine(
                ST_Transform(ST_Point({lat}, {long}), 'EPSG:4326', '{LAMBERT_WKT}'), linestr
            )
            ) as shortestline
            from {self.lrs_line_table}
            )

            -- select from point_on_line
            select linkid, ST_X(shortestline) as x, ST_Y(shortestline) as y
            from point_on_line
            """
        )

        # Vertex on the LRS line.
        # Vertex that will be interpolated.
        vertex_x = vertex_on_line.fetchall()[0][1]
        vertex_y = vertex_on_line.fetchall()[0][2]

        # Nearest vertex to the new vertex
        # Get all data to interpolate the new vertex M-value
        input_var = self.dconn.sql(f"""
                       with nearest_seq as
                       (
                       select linkid, 
                       unnest(arg_min({self.seq_col}, ST_Distance(ST_Point({vertex_x}, {vertex_y}), point), 2))
                       as {self.seq_col}
                       from {self.lrs_point_table} group by linkid
                       )

                       select linkid,
                       ST_Distance(first(point), last(point)) as dist,
                       first({self.mval_col}) as first_m,
                       last({self.mval_col})-first({self.mval_col}) as m_delta,
                       ST_Distance(ST_Point({vertex_x}, {vertex_y}), first(point)) as inter_dist
                       from {self.lrs_point_table} where {self.seq_col} in (select {self.seq_col} from nearest_seq)
                       group by linkid
                       """).df().iloc[0]

        result = ((input_var['m_delta']/input_var['dist'])*input_var['inter_dist'])+(input_var['first_m'])

        if unit == 'm':
            return float(result)
        elif unit == 'km':
            return float(result/1000)
        else:
            raise ValueError(f"{unit} is invalid or unsupported unit conversion.")
