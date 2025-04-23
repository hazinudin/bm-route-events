from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BridgeIdRequests(_message.Message):
    __slots__ = ("bridge_ids",)
    BRIDGE_IDS_FIELD_NUMBER: _ClassVar[int]
    bridge_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, bridge_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class ObjectIdRequests(_message.Message):
    __slots__ = ("objectids",)
    OBJECTIDS_FIELD_NUMBER: _ClassVar[int]
    objectids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, objectids: _Optional[_Iterable[int]] = ...) -> None: ...

class NameRequests(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, name: _Optional[_Iterable[str]] = ...) -> None: ...

class NumberRequests(_message.Message):
    __slots__ = ("number",)
    NUMBER_FIELD_NUMBER: _ClassVar[int]
    number: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, number: _Optional[_Iterable[str]] = ...) -> None: ...

class SpatialFilter(_message.Message):
    __slots__ = ("geojson", "crs")
    GEOJSON_FIELD_NUMBER: _ClassVar[int]
    CRS_FIELD_NUMBER: _ClassVar[int]
    geojson: str
    crs: str
    def __init__(self, geojson: _Optional[str] = ..., crs: _Optional[str] = ...) -> None: ...

class Result(_message.Message):
    __slots__ = ("objectid", "global_id", "success")
    OBJECTID_FIELD_NUMBER: _ClassVar[int]
    GLOBAL_ID_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    objectid: int
    global_id: int
    success: bool
    def __init__(self, objectid: _Optional[int] = ..., global_id: _Optional[int] = ..., success: bool = ...) -> None: ...

class SpatialReference(_message.Message):
    __slots__ = ("wkt",)
    WKT_FIELD_NUMBER: _ClassVar[int]
    wkt: str
    def __init__(self, wkt: _Optional[str] = ...) -> None: ...

class Point(_message.Message):
    __slots__ = ("x", "y", "spatial_reference")
    X_FIELD_NUMBER: _ClassVar[int]
    Y_FIELD_NUMBER: _ClassVar[int]
    SPATIAL_REFERENCE_FIELD_NUMBER: _ClassVar[int]
    x: float
    y: float
    spatial_reference: SpatialReference
    def __init__(self, x: _Optional[float] = ..., y: _Optional[float] = ..., spatial_reference: _Optional[_Union[SpatialReference, _Mapping]] = ...) -> None: ...

class EditResults(_message.Message):
    __slots__ = ("add_results", "update_results", "delete_results")
    ADD_RESULTS_FIELD_NUMBER: _ClassVar[int]
    UPDATE_RESULTS_FIELD_NUMBER: _ClassVar[int]
    DELETE_RESULTS_FIELD_NUMBER: _ClassVar[int]
    add_results: _containers.RepeatedCompositeFieldContainer[Result]
    update_results: _containers.RepeatedCompositeFieldContainer[Result]
    delete_results: _containers.RepeatedCompositeFieldContainer[Result]
    def __init__(self, add_results: _Optional[_Iterable[_Union[Result, _Mapping]]] = ..., update_results: _Optional[_Iterable[_Union[Result, _Mapping]]] = ..., delete_results: _Optional[_Iterable[_Union[Result, _Mapping]]] = ...) -> None: ...

class Attributes(_message.Message):
    __slots__ = ("bridge_id", "objectid", "bridge_name", "city_regency", "bridge_length", "bridge_width", "start_date", "end_date", "longitude", "latitude", "bridge_num", "bridge_status", "shore_dist", "adt", "aadt", "adt_year", "road_func", "rni_surf_width", "rni_year", "bm_prov_id", "linkid", "cons_year", "last_inv_date", "bridge_type", "bridge_str_type")
    BRIDGE_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECTID_FIELD_NUMBER: _ClassVar[int]
    BRIDGE_NAME_FIELD_NUMBER: _ClassVar[int]
    CITY_REGENCY_FIELD_NUMBER: _ClassVar[int]
    BRIDGE_LENGTH_FIELD_NUMBER: _ClassVar[int]
    BRIDGE_WIDTH_FIELD_NUMBER: _ClassVar[int]
    START_DATE_FIELD_NUMBER: _ClassVar[int]
    END_DATE_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    BRIDGE_NUM_FIELD_NUMBER: _ClassVar[int]
    BRIDGE_STATUS_FIELD_NUMBER: _ClassVar[int]
    SHORE_DIST_FIELD_NUMBER: _ClassVar[int]
    ADT_FIELD_NUMBER: _ClassVar[int]
    AADT_FIELD_NUMBER: _ClassVar[int]
    ADT_YEAR_FIELD_NUMBER: _ClassVar[int]
    ROAD_FUNC_FIELD_NUMBER: _ClassVar[int]
    RNI_SURF_WIDTH_FIELD_NUMBER: _ClassVar[int]
    RNI_YEAR_FIELD_NUMBER: _ClassVar[int]
    BM_PROV_ID_FIELD_NUMBER: _ClassVar[int]
    LINKID_FIELD_NUMBER: _ClassVar[int]
    CONS_YEAR_FIELD_NUMBER: _ClassVar[int]
    LAST_INV_DATE_FIELD_NUMBER: _ClassVar[int]
    BRIDGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    BRIDGE_STR_TYPE_FIELD_NUMBER: _ClassVar[int]
    bridge_id: str
    objectid: int
    bridge_name: str
    city_regency: str
    bridge_length: float
    bridge_width: float
    start_date: str
    end_date: str
    longitude: float
    latitude: float
    bridge_num: str
    bridge_status: str
    shore_dist: float
    adt: float
    aadt: float
    adt_year: float
    road_func: str
    rni_surf_width: float
    rni_year: int
    bm_prov_id: str
    linkid: str
    cons_year: int
    last_inv_date: str
    bridge_type: str
    bridge_str_type: str
    def __init__(self, bridge_id: _Optional[str] = ..., objectid: _Optional[int] = ..., bridge_name: _Optional[str] = ..., city_regency: _Optional[str] = ..., bridge_length: _Optional[float] = ..., bridge_width: _Optional[float] = ..., start_date: _Optional[str] = ..., end_date: _Optional[str] = ..., longitude: _Optional[float] = ..., latitude: _Optional[float] = ..., bridge_num: _Optional[str] = ..., bridge_status: _Optional[str] = ..., shore_dist: _Optional[float] = ..., adt: _Optional[float] = ..., aadt: _Optional[float] = ..., adt_year: _Optional[float] = ..., road_func: _Optional[str] = ..., rni_surf_width: _Optional[float] = ..., rni_year: _Optional[int] = ..., bm_prov_id: _Optional[str] = ..., linkid: _Optional[str] = ..., cons_year: _Optional[int] = ..., last_inv_date: _Optional[str] = ..., bridge_type: _Optional[str] = ..., bridge_str_type: _Optional[str] = ...) -> None: ...

class Bridge(_message.Message):
    __slots__ = ("attributes", "geometry")
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    GEOMETRY_FIELD_NUMBER: _ClassVar[int]
    attributes: Attributes
    geometry: Point
    def __init__(self, attributes: _Optional[_Union[Attributes, _Mapping]] = ..., geometry: _Optional[_Union[Point, _Mapping]] = ...) -> None: ...

class Bridges(_message.Message):
    __slots__ = ("bridges",)
    BRIDGES_FIELD_NUMBER: _ClassVar[int]
    bridges: _containers.RepeatedCompositeFieldContainer[Bridge]
    def __init__(self, bridges: _Optional[_Iterable[_Union[Bridge, _Mapping]]] = ...) -> None: ...
