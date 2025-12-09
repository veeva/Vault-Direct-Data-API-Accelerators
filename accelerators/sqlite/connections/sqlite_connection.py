import sqlite3
from sqlite3 import Connection, Cursor

from common.connections.database_connection import DatabaseConnection
from common.utilities import log_message


class SqliteConnection(DatabaseConnection):

    def __init__(self, databases_folder: str, database: str):
        """
        This initializes the Sqlite Connector class with the given parameters.
        """
        super().__init__()
        self.databases_folder = databases_folder
        self.database = database
        self.connected = False
        self.con: Connection | None = None
        self.cursor: Cursor | None = None

    def open(self):
        """
        Connects to Sqlite.
        """
        try:
            if self.connected:
                return self.con

            # Use the Sqlite connection
            database_filepath: str = f"{self.databases_folder}/{self.database}"
            conn: Connection = sqlite3.connect(database_filepath)

            self.con = conn
            self.connected = True
            return conn

        except Exception as e:
            log_message(
                log_level='Exception',
                message=f"Error Connecting to Sqlite database: {self.database}",
                exception=e)
            self.connected = False
            return None

    def execute_query(self, query: str):
        if not self.connected:
            self.open()
            self.cursor = self.con.cursor()

        log_message(
            log_level='Info',
            message=f"Executing query: {query}")
        try:
            self.cursor.execute(query)
            if query.strip().upper().startswith("SELECT"):
                return self.cursor.fetchall()
            else:
                self.con.commit()
                return None
        except Exception as e:
            log_message(
                log_level='Exception',
                message=f"Executing query: {query}",
                exception=e)
            return []
