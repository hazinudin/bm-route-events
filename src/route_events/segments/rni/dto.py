from ..base import CenterlineSegment
from typing import Literal, Optional


class TypeSidedColumnError(CenterlineSegment):
    dir: Literal[1, 2]
    side: Literal['L', 'R']
    column: str
    na: bool
    wrong_side: bool
    single_value: bool


class ValueSidedColumnError(TypeSidedColumnError):
    type_column: str
    wrong_value_type: Optional[bool] = None

