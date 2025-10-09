from ..events import BridgeEvents
import json
from ...geometry import Point


MASTER_LENGTH_UPDATED = 'master.length_updated'
MASTER_NUMBER_UPDATED = 'master.number_updated'
MASTER_COORDINATE_UPDATED = 'master.coordinate_updated'


class BridgeMasterCoordinateUpdated(BridgeEvents):
    def __init__(self, bridge_id: str, old_geom: Point, new_geom: Point):
        BridgeEvents.__init__(self, MASTER_COORDINATE_UPDATED, bridge_id)
        self._old_geom = old_geom
        self._new_geom = new_geom

        # Distance between new and old coordinate.
        self._dist = old_geom.distance_to(new_geom)

    def serialize(self) -> str:
        """
        Serialize to JSON
        """
        event = {
            "name": self.name,
            "occurred_at": self.occurred_at,
            "event": {
                "coordinate.old": [self._old_geom.X, self._old_geom.Y],
                "coordinate.new": [self._new_geom.X, self._new_geom.Y],
                "coordinate.distance": self._dist
            }
        }

        return json.dumps(event)


class BridgeMasterLengthUpdated(BridgeEvents):
    def __init__(self, bridge_id: str, old_length: float, new_length: float):
        BridgeEvents.__init__(self, MASTER_LENGTH_UPDATED, bridge_id)
        self._old_len = old_length
        self._new_len = new_length

    def serialize(self) -> str:
        """
        Serialize to JSON
        """
        event = {
            "name": self.name,
            "occurred_at": self.occurred_at,
            "event": {
                "length.old": self._old_len,
                "length.new": self._new_len
            }
        }

        return json.dumps(event)
    

class BridgeMasterNumberUpdated(BridgeEvents):
    def __init__(self, bridge_id: str, old_number: str, new_number: str):
        BridgeEvents.__init__(self, MASTER_NUMBER_UPDATED, bridge_id)
        self._old_number = old_number
        self._new_number = new_number

    def serialize(self):
        """
        Serialize to JSON
        """
        event = {
            "name": self.name,
            "occurred_at": self.occurred_at,
            "event": {
                "number.old": self._old_number,
                "number.new": self._new_number
            }
        }

        return json.dumps(event)