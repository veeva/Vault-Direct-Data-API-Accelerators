from azure.identity import DefaultAzureCredential
import pyodbc, struct
from pyodbc import Connection, Error, Cursor
from itertools import chain, repeat

from common.connections.database_connection import DatabaseConnection
from common.utilities import log_message


class FabricConnection(DatabaseConnection):
    """
    TODO: Add docstring
    """

    def __init__(self, connection_string: str, database: str, server_name: str,
                 credential: DefaultAzureCredential = None):
        """
        This initializes the Fabric Warehouse Connector class with the given parameters that allow the class to connect to an
        active Fabric Warehouse.
        """
        super().__init__()
        self.credential: DefaultAzureCredential = credential
        self.server_name: str = server_name
        self.database: str = database
        self.connection_string: str = connection_string
        self.connected: bool = False
        self.con: Connection | None = None
        self.cursor: Cursor | None = None

    def open(self):
        """
        Connects to Fabric Warehouse using the database admin credentials.
        """
        try:
            if self.connected:
                return self.con

            credential = DefaultAzureCredential()
            token_object  = credential.get_token("https://database.windows.net//.default")
            token_as_bytes = bytes(token_object.token, "UTF-8") # Convert the token to a UTF-8 byte string
            encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0)))) # Encode the bytes to a Windows byte string
            token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes # Package the token into a bytes object
            attrs_before = {1256: token_bytes}  # Attribute pointing to SQL_COPT_SS_ACCESS_TOKEN to pass access token to the driver

            conn: Connection = pyodbc.connect(self.connection_string,
                                              attrs_before=attrs_before,
                                              autocommit=True)

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
