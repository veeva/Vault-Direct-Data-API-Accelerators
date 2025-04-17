from abc import ABC, abstractmethod
from typing import Any


class DatabaseConnection(ABC):
    def __init__(self):
        super().__init__()
        self.connected: bool = False
        self.con: Any = None
        self.cursor: Any = None

    @abstractmethod
    def open(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def execute_query(self, query: str):
        pass
