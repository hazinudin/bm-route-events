from ....schema import RouteEventsSchema
import os


class SupsOnlyProfileSchema(RouteEventsSchema):
    def __init__(self, ignore_review_err=False):
        schema_config = os.path.dirname(__file__) + "/sups_only_schema.json"
        RouteEventsSchema.__init__(
            self, file_path=schema_config, ignore_review_err=ignore_review_err
        )
