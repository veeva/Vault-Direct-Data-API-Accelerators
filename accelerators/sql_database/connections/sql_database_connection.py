import pyodbc
from pyodbc import Connection, Error, Cursor

from common.connections.database_connection import DatabaseConnection
from common.utilities import log_message


class SqlDatabaseConnection(DatabaseConnection):
    """
    TODO: Add docstring
    """

    def __init__(self, connection_string: str, database: str, server_name: str, username: str, user_password: str):
        """
        This initializes the Sql Server Connector class with the given parameters that allow the class to connect to an
        active Sql Server database.
        """
        super().__init__()
        self.server_name: str = server_name
        self.database: str = database
        self.user: str = username
        self.password: str = user_password
        self.connection_string: str = connection_string
        self.connected: bool = False
        self.con: Connection | None = None
        self.cursor: Cursor | None = None


    def open(self):
        """
        Connects to Sql Server using the database admin credentials.
        """
        try:
            if self.connected:
                return self.con

            # Use the Sql Server connection
            conn: Connection = pyodbc.connect(self.connection_string)


            self.con = conn
            self.connected = True
            return conn

        except Error as e:
            log_message(
                log_level='Error',
                message='Failed to connect to database',
                exception=e
            )
            raise e

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
                if hasattr(self.cursor, 'description') and self.cursor.description is not None:
                    return self.cursor.fetchall()
                else:
                    return []
            else:
                self.con.commit()
                return None
        except Exception as e:
            log_message(
                log_level='Exception',
                message=f"Executing query: {query}",
                exception=e)
            return []

    def activate_cursor(self):
        if self.connected:
            if self.cursor is None:
                self.cursor = self.con.cursor()

    def close_cursor(self):
        self.cursor.close()
