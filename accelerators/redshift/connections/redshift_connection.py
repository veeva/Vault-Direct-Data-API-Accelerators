import psycopg2
from psycopg2 import OperationalError

from common.connections.database_connection import DatabaseConnection
from common.utilities import log_message


class RedshiftConnection(DatabaseConnection):
    """
    TODO: Add docstring
    """

    def __init__(self, database: str, hostname: str, port_number: int, username: str, user_password: str):
        """
        This initializes the Redshift Connector class with the given parameters that allow the class to connect to an
        active Redshift cluster database.
        """
        super().__init__()
        self.database = database
        self.host = hostname
        self.port = port_number
        self.user = username
        self.password = user_password
        self.connected = False
        self.con = None
        self.cursor = None

    def open(self):
        """
        Connects to Redshift using the database admin credentials.
        """
        try:
            if self.connected:
                return self.con

            # Use the Redshift connection
            conn = psycopg2.connect(
                database=self.database,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password
            )

            self.con = conn
            self.connected = True
            return conn

        except OperationalError as e:
            log_message(
                log_level='Error',
                message='Failed to connect to database',
                exception=e,
                context=None
            )
            raise e

    def close(self):
        if self.connected:
            self.con.close()
            self.connected = False

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
                if self.cursor.rowcount > 0:
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
