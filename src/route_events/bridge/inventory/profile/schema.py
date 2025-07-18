from ....schema import RouteEventsSchema
import os


class InventoryProfileSchema(RouteEventsSchema):
    """
    Generate Pyarrow Schema object from schema.json configuration for Inventory Profile.
    """
    def __init__(self, ignore_review_err=False):
        RouteEventsSchema.__init__(
            self, 
            os.path.dirname(__file__) + '/schema.json', 
            ignore_review_err=ignore_review_err
        )
