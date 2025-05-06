from ..base import RouteSegmentEvents
import polars as pl


class RoutePOK(RouteSegmentEvents):
    """
    Route segment POK model.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._comp_col = 'COMP_NAME'
        self._budget_year_col = 'BUDGET_YEAR'
        self._from_sta_col = 'START_IND'
        self._to_sta_col = 'END_IND'
        
        # Default STA unit
        self.sta_unit = 'km'

        # Is not a lane based data
        self._lane_data = False