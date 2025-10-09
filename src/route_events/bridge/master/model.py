import pyarrow as pa
import pyarrow.compute as pc
import json
import duckdb
import polars as pl
from datetime import datetime, timedelta
from .schema import BridgeMasterSchema
from ...geometry import Point, LAMBERT_WKT
from .repo.bridge_master_pb2 import (
    Attributes as AttributesPB , 
    Point as PointPB, Bridge as BridgePB, 
    SpatialReference as SpatialReferencePB
)
from .events import (
    BridgeMasterLengthUpdated,
    BridgeMasterNumberUpdated, 
    BridgeEvents
)
from numpy import isclose


ARCGIS_STRFTIME = '%m/%d/%Y %H:%M:%S'

class BridgeMaster(object):
    """
    Model of Bridge Master data.
    """

    @classmethod
    def from_invij(cls, data: dict, ignore_review_err=False):
        """
        Load BridgeMaster data from a dictionary in INVIJ input format.
        """
        schema = BridgeMasterSchema(ignore_review_err)
        input_schema = schema.input_schema

        # Schema will try to serialize data using defined data type
        if type(data) == dict:
            # data = {k.upper():v for k,v in data.items()}  # Convert keys to uppercase
            data = json.loads(json.dumps(data).upper().replace("NULL", "null"))

            model = schema.model.model_validate(data)
            data = model.model_dump(by_alias=True)

            df = pl.DataFrame(data)
            artable = df.to_arrow()
            # artable = pa.Table.from_pylist([data], schema=input_schema)
            # artable = artable.rename_columns(schema.translate_mapping)

        else:
            raise TypeError(f"Only accepts dictionary type. {type(data)} is given.")
    
        return cls(artable, validate=False)

    def __init__(self, data: pa.Table, validate=True):
        """
        Initialization accept only Pyarrow Table.
        """
        self._events = []
        self._schema = BridgeMasterSchema()
        self.input_schema = self._schema.input_schema

        if validate:
            self._schema.model.model_validate(pl.from_arrow(data).row(0, named=True))
        
        # Data
        if type(data) == pa.Table:
            self.artable = data
            _artable = self.artable  # DuckDB Pointer
        else:
            raise TypeError(f"data is {type(data)} not a Pyarrow Table.")

        # Columns name
        self._bridge_id_col = 'BRIDGE_ID'
        self._bridge_num_col = 'BRIDGE_NUM'
        self._bridge_name_col = 'BRIDGE_NAME'
        self._linkid_col = 'LINKID'
        self._lon_col = 'LONGITUDE'
        self._lat_col = 'LATITUDE'
        self._prov_col = 'BM_PROV_ID'
        self._status_col = 'BRIDGE_STATUS'
        self._inv_date_col = 'LAST_INV_DATE'
        self._cons_year_col = 'CONS_YEAR'
        self._bridge_len_col = 'BRIDGE_LENGTH'

        # DuckDB Session
        self.ddb = duckdb.connect()

        # Geometry
        self._point_4326 = Point(
            long=self.artable[self._lon_col][0],
            lat=self.artable[self._lat_col][0],
            wkt='EPSG:4326',
            ddb=self.ddb
        )
        
        self._point_lambert = self._point_4326.transform(LAMBERT_WKT, invert=True)

        # Bridge number valid formats.
        format_1 = [2, 3, 3, 2, 2]  # 1st alternative
        format_2 = [2, 3, 3, 1, 2]  # 2nd alternative
        format_3 = [2, 3, 3, 2]  # 3rd alternative without suffix
        format_4 = [2, 3, 3, 1]  # 4th alternative without suffix
        format_5 = [2, 3, 3, 2, 1]  # 5th alternative
        format_6 = [2, 3, 3, 1, 1]  # 6th alternative

        self._valid_num_formats = [format_1, format_2, format_3, format_4, format_5, format_6]

        # Load the data as duckdb table
        # self.tbl = 'self'
        # self.ddb.sql(f"create table {self.tbl} as (select * from _artable)")
        
    @property
    def has_correct_num_format(self)->bool:
        """
        Check if the bridge number has the correct format.
        """
        num_split = self.number.split('.')
        split_len = [len(_x) for _x in num_split]
        any_match = any([split_len == _x for _x in self._valid_num_formats])

        return any_match
    
    @property
    def has_correct_prov_in_num(self)->bool:
        """
        Check if the bridge number has correct province number (compare it to the province column).
        """
        return self.number[:2] == self.province
    
    @property
    def lambert_wkt(self)->str:
        """
        Return Bina Marga Lambert Projection CRS.
        """
        return LAMBERT_WKT
    
    @property
    def id(self)->str:
        """
        Return Bridge ID
        """
        return self.artable[self._bridge_id_col][0].as_py()
    
    @property
    def number(self)->str:
        """
        Return bridge number.
        """
        return self.artable[self._bridge_num_col][0].as_py()
    
    @number.setter
    def number(self, number: str):
        """
        Set the bridge number.
        """
        old_number = self.number

        if old_number != number:
            self.artable = pl.from_arrow(self.artable).with_columns(
                **{self._bridge_num_col: pl.lit(number)}
            ).to_arrow()

            event = BridgeMasterNumberUpdated(
                bridge_id=self.id,
                old_number=old_number,
                new_number=number
            )

            self.add_events(event)
    
    @property
    def name(self)->str:
        """
        Return bridge name.
        """
        return self.artable[self._bridge_name_col][0].as_py()

    @property
    def province(self)->str:
        """
        Return province code.
        """
        return self.artable[self._prov_col][0].as_py()
    
    @property
    def linkid(self)->str:
        """
        Return bridge route ID (LINKID).
        """
        return self.artable[self._linkid_col][0].as_py()
    
    @property
    def status(self)->str:
        """
        Return bridge status.
        """
        return self.artable[self._status_col][0].as_py()
    
    @property
    def master_survey_year(self)->int:
        """
        Return bridge survey year.
        """
        return self.artable[self._inv_date_col][0].as_py().year
    
    @property
    def construction_year(self)->int:
        """
        Return bridge construction year.
        """
        return self.artable[self._cons_year_col][0].as_py()
    
    @property
    def lambert_x(self)->float:
        """
        Return bridge X coordinate in lambert.
        """
        return self._point_lambert.X
    
    @property
    def lambert_y(self)->float:
        """
        Return bridge Y coordinate in lambert.
        """
        return self._point_lambert.Y
    
    @property
    def length(self)->float:
        """
        Return bridge length.
        """
        return float(self.artable[self._bridge_len_col][0].as_py())
    
    @length.setter
    def length(self, length: float):
        """
        Set new length value and store length updated event.
        """
        old_length = self.length

        # If the length is different
        if not isclose(old_length, length):
            self.artable = pl.from_arrow(self.artable).with_columns(
                **{self._bridge_len_col: length}
            ).to_arrow()

            event = BridgeMasterLengthUpdated(
                bridge_id=self.id,
                old_length=old_length,
                new_length=length
            )

            self.add_events(event)

    def get_all_events(self) -> list:
        """
        Get all the events.
        """
        return self._events
    
    def get_latest_event(self) -> BridgeEvents:
        """
        Get the latest event in the event list.
        """
        return self._events[-1]  # Get the last appended event.
    
    def add_events(self, event: BridgeEvents):
        """
        Add events to the object.
        """
        self._events.append(event) 
    
    def as_pb(self)->BridgePB:
        """
        Return Protocol Buffer message.
        """
        lcase_artable = self.artable.rename_columns(self._schema.db_upper_to_lower_case)
        lcase_dict = lcase_artable.to_pylist()[0]

        # Convert date column to ArcGIS specified strftime format
        for date_col in self._schema.db_date_cols:
            col_lower = str(date_col).lower()
            timestmp = lcase_dict.get(col_lower)

            # Offset 7 hours to GMT
            timestmp = timestmp - timedelta(hours=7)
            
            lcase_dict[col_lower] = datetime.strftime(timestmp, ARCGIS_STRFTIME)
            # attr.__setattr__(col_lower, datetime.strftime(timestmp, ARCGIS_STRFTIME))

        # Protocol buffer object.
        attr = AttributesPB(**lcase_dict)
        sr = SpatialReferencePB(wkt=self._point_lambert.origin_wkt)

        # For some reason, the ESRI geometry accept inverted coordinates in lambert, even though the lambert is already inverted.
        geom = PointPB(y=self._point_lambert.X, x=self._point_lambert.Y, spatial_reference=sr)
        bridge = BridgePB(attributes=attr, geometry=geom)
        
        return bridge
    
    def get_out_of_range_columns(self):
        """
        Return column whose value is not within the correct range/domain.
        """
        # CASE WHEN for each column with range/domain configuration.
        cases = list()

        for i in range(len(self.artable.schema)):
            # Metadata in bstring
            metas = self.artable.schema.field(i).metadata[self._schema.metadata_keys]
            col_name = self.artable.schema.field(i).name
            meta_dict = json.loads(metas)

            # Create case for range
            if meta_dict['range'] is not None:
                range_conf = meta_dict['range']
                upper_bound = range_conf['upper']
                lower_bound = range_conf['lower']
                is_review = range_conf['review']
                
                if range_conf['eq_upper']:
                    upper_op = '>'
                else:
                    upper_op = '>='

                if range_conf['eq_lower']:
                    lower_op = '<'
                else:
                    lower_op = '<='

                # Return the value which sits outside the valid range/domain.
                range_case = f"case when {col_name} {lower_op} {lower_bound} or {col_name} {upper_op} {upper_bound} then {{val:{col_name}, is_review: {is_review}}} else null end as {col_name}"
                cases.append(range_case)

            elif meta_dict['domain'] is not None:
                domain_value = meta_dict['domain']
                domain_case = f"case when {col_name} not in ({str(domain_value).strip('[]')}) then {{val:{col_name}, is_review: false}} else null end as {col_name}"
                cases.append(domain_case)
        
        result = self.ddb.sql(f"select {', '.join(cases)} from {self.tbl}").to_df()

        return result.loc[0, result.notnull().any()].to_dict()
        
    def buffer_area_lambert(self, radius_meter: float):
        """
        Return a buffer polygon around the BridgeMaster coordinate at the target distance, at target radius.
        """
        buffer_polygon = self._point_lambert.create_buffer(radius_meter)

        return buffer_polygon
    
    def distance_to(self, other_bridge)->float:
        """
        Return distance to other BridgeMaster object.
        """
        if type(other_bridge) != BridgeMaster:
            raise TypeError(f"Only accepts BridgeMaster object. {type(other_bridge)} is given.")
        
        dist = self._point_lambert.distance_to(other_bridge._point_lambert)

        return dist
