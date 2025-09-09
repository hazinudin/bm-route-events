from .....schema import RouteEventsSchema
import os


class SuperstructureSchema(RouteEventsSchema):
    def __init__(self, ignore_review_err=False, popup:bool=False):
        # Differentiate between detailed and popup inventory schema.
        if not popup:
            schema_config = os.path.dirname(__file__) + '/sups_schema.json'
        else:
            schema_config = os.path.dirname(__file__) + '/popup_sups_schema.json'

        RouteEventsSchema.__init__(
            self, 
            file_path=schema_config,
            ignore_review_err=ignore_review_err
        )
