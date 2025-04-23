from dataclasses import dataclass
from typing import Literal

@dataclass
class _RoadType(object):
    road_type: int
    lane_count: int
    dir: Literal[1, 2]
    median: bool

_1 = _RoadType(
    road_type = 1,
    lane_count = 2,
    dir = 1,
    median = False
)

_2 = _RoadType(
    road_type = 2,
    lane_count = 2,
    dir = 2,
    median = False
)

_3 = _RoadType(
    road_type = 3,
    lane_count = 4,
    dir = 2,
    median = False
)

_4 = _RoadType(
    road_type = 4,
    lane_count = 4,
    dir = 2,
    median = True
)

_5 = _RoadType(
    road_type = 5,
    lane_count = 6,
    dir = 2,
    median = True
)

_6 = _RoadType(
    road_type = 6,
    lane_count = 3,
    dir = 1,
    median = False
)

_7 = _RoadType(
    road_type = 7,
    lane_count = 3,
    dir = 2,
    median = False
)

_8 = _RoadType(
    road_type = 8,
    lane_count = 3,
    dir = 2,
    median = True
)

_9 = _RoadType(
    road_type = 9,
    lane_count = 4,
    dir = 1,
    median = False
)

_10 = _RoadType(
    road_type = 10,
    lane_count = 4,
    dir = 1,
    median = True
)

_11 = _RoadType(
    road_type = 11,
    lane_count = 5,
    dir = 1,
    median = False
)

_12 = _RoadType(
    road_type = 12,
    lane_count = 5,
    dir = 1,
    median = True
)

_13 = _RoadType(
    road_type = 13,
    lane_count = 5,
    dir = 2,
    median = True
)

_14 = _RoadType(
    road_type = 14,
    lane_count = 6,
    dir = 1,
    median = False
)

_15 = _RoadType(
    road_type = 15,
    lane_count = 6,
    dir = 1,
    median = True
)

_16 = _RoadType(
    road_type = 16,
    lane_count = 7,
    dir = 1,
    median = False
)

_17 = _RoadType(
    road_type = 17,
    lane_count = 7,
    dir = 1,
    median = True
)

_18 = _RoadType(
    road_type = 18,
    lane_count = 7,
    dir = 2,
    median = False
)

_19 = _RoadType(
    road_type = 19,
    lane_count = 7,
    dir = 2,
    median = True
)

_20 = _RoadType(
    road_type = 20,
    lane_count = 8,
    dir = 2,
    median = True
)

_21 = _RoadType(
    road_type = 21,
    lane_count = 10,
    dir = 2,
    median = True
)

_22 = _RoadType(
    road_type = 22,
    lane_count = 2,
    dir = 2,
    median = True
)

_23 = _RoadType(
    road_type = 23,
    lane_count = 6,
    dir = 2,
    median = False
)

_24 = _RoadType(
    road_type = 24,
    lane_count = 5,
    dir = 2,
    median = False
)

_25 = _RoadType(
    road_type = 25,
    lane_count = 8,
    dir = 2,
    median = False
)

_26 = _RoadType(
    road_type = 26,
    lane_count = 3,
    dir = 1,
    median = True
)

_27 = _RoadType(
    road_type = 27,
    lane_count = 1,
    dir = 1,
    median = False
)

_28 = _RoadType(
    road_type = 28,
    lane_count = 9,
    dir = 2,
    median = True
)

_road_types = [
    _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14,
    _15, _16, _17, _18, _19, _20, _21, _22, _23, _24, _25, _26,
    _27, _28
]











