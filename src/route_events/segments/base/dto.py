from pydantic import BaseModel
from typing import Union, List, Optional


class Segment(BaseModel, extra='allow'):
    route_id:str
    from_sta: Union[float | int]
    to_sta: Union[float | int]
    lane: str


class OverlappingSegment(Segment, extra='allow'):
    overlapped: Optional[Segment] = None


class CenterlineSegment(BaseModel, extra='allow'):
    route_id:str
    from_sta: Union[float | int]
    to_sta: Union[float | int]
    lanes: Optional[List[str]] = []
