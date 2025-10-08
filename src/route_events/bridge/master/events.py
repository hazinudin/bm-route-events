from ..events import BridgeEvents
import json


MASTER_LENGTH_UPDATED = 'master.length_updated'
MASTER_NUMBER_UPDATED = 'master.number_updated'


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