from abc import ABC, abstractmethod
from datetime import datetime


class BridgeEvents(ABC):
    def __init__(self, name: str, id: str):
        self._name = name
        self._id = id
        self._occurred_at = datetime.now()

    @property
    def name(self) -> str:
        return self._name
    
    @property
    def occurred_at(self) -> str:
        return self._occurred_at.strftime("%d/%b/%Y %I:%M:0%p")
    
    @property
    def id(self) -> str:
        return self._id
    
    @abstractmethod
    def serialize(self) -> str:
        """
        Serialize to JSON  string
        """
        pass