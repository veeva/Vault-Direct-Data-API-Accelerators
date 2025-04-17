from databricks import sql

from common.connections.database_connection import DatabaseConnection
from common.utilities import log_message


class DatabricksConnection(DatabaseConnection):
    """
        Initializes the Databricks connection with the given parameters.
    """

    def __init__(self, server_hostname: str, http_path: str, access_token: str, catalog: str):
        super().__init__()
        self.server_hostname: str = server_hostname
        self.http_path: str = http_path
        self.catalog: str = catalog
        self.access_token: str = access_token
        self.connected: bool = False

    def open(self):
        """
        Establishes a connection to Databricks using ODBC.
        """
        try:
            if self.connected:
                return self.con
            else:
                self.con = sql.connect(
                    server_hostname=self.server_hostname,
                    http_path=self.http_path,
                    access_token=self.access_token
                )
                self.connected = True
                return self.con

        except sql.Error as e:
            log_message(
                log_level='Error',
                message=f"Error connecting to Databricks: {e}")
            raise e

    def close(self):
        """
        Closes the database connection.
        """
        if self.connected:
            self.con.close()
            self.connected = False

    def execute_query(self, query: str):
        """
        Executes a given SQL query and fetches all results.
        """
        if not self.connected:
            self.open()
        cursor = self.con.cursor()

        log_message(
            log_level='Info',
            message=f"Executing query: {query}", )
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result
