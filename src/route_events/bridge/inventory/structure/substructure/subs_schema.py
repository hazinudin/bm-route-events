from route_events.schema import RouteEventsSchema
import os


class SubstructureSchema(RouteEventsSchema):
    def __init__(self, ignore_review_err=False):
        RouteEventsSchema.__init__(
            self, 
            file_path=os.path.dirname(__file__) + '/subs_schema.json',
            ignore_review_err=ignore_review_err
        )
