from ....schema import RouteEventsSchema
import os


class InventoryProfileSchema(RouteEventsSchema):
    """
    Generate Pyarrow Schema object from schema.json configuration for Inventory Profile.
    """
    def __init__(self, ignore_review_err=False, popup:bool=False):
        # Differentiate between detailed and popup inventory schema.
        if not popup:
            schema_config = os.path.dirname(__file__) + '/schema.json'
        else:
            schema_config = os.path.dirname(__file__) + '/popup_schema.json'
        
        RouteEventsSchema.__init__(
            self, 
            schema_config, 
            ignore_review_err=ignore_review_err
        )
