from abc import ABC, abstractmethod
from typing import Any


class DatabaseConnection(ABC):
    def __init__(self):
        super().__init__()
        self.connected: bool = False
        self.con: Any = None
        self.cursor: Any = None
        self.convert_to_parquet: bool

    @abstractmethod
    def open(self):
        pass

    def close(self):
        """
        Closes the database connection.
        """
        if self.connected:
            self.con.close()
            self.connected = False

    @abstractmethod
    def execute_query(self, query: str):
        pass

    def activate_cursor(self):
        """
        Activates the cursor for executing queries.
        """
        if self.connected:
            if self.cursor is None:
                self.cursor = self.con.cursor()

    def close_cursor(self):
        """
        Closes the cursor if it is open.
        """
        self.cursor.close()
