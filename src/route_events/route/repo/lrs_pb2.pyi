from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class DownloadRequests(_message.Message):
    __slots__ = ("routes", "output_shp")
    ROUTES_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_SHP_FIELD_NUMBER: _ClassVar[int]
    routes: _containers.RepeatedScalarFieldContainer[str]
    output_shp: str
    def __init__(self, routes: _Optional[_Iterable[str]] = ..., output_shp: _Optional[str] = ...) -> None: ...

class RouteRequests(_message.Message):
    __slots__ = ("routes",)
    ROUTES_FIELD_NUMBER: _ClassVar[int]
    routes: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, routes: _Optional[_Iterable[str]] = ...) -> None: ...

class FilePath(_message.Message):
    __slots__ = ("path",)
    PATH_FIELD_NUMBER: _ClassVar[int]
    path: str
    def __init__(self, path: _Optional[str] = ...) -> None: ...

class Routes(_message.Message):
    __slots__ = ("geojson",)
    GEOJSON_FIELD_NUMBER: _ClassVar[int]
    geojson: str
    def __init__(self, geojson: _Optional[str] = ...) -> None: ...
