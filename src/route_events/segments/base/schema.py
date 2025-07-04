import pyarrow as pa
from ...schema import RouteEventsSchema


class RouteSegmentEventSchema(RouteEventsSchema):
    """
    Generate Pyarrow Schema object from schema JSON configuration file.
    """
    def __init__(self, config_path: str, ignore_review_err = False):
        RouteEventsSchema.__init__(
            self, config_path, ignore_review_err=ignore_review_err
        )
