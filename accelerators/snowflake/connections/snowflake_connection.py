import snowflake.connector
from cryptography.hazmat.primitives import serialization

from common.connections.database_connection import DatabaseConnection
from common.utilities import log_message


class SnowflakeConnection(DatabaseConnection):
    """
    TODO: Add docstring
    """

    def __init__(self, database: str, account: str, warehouse: str, schema: str, stage_name: str,
                 username: str, role:str, private_key: str, private_key_passphrase: str):
        super().__init__()
        self.account: str = account
        self.database: str = database
        self.warehouse: str = warehouse
        self.schema: str = schema
        self.stage_name: str = stage_name
        self.username: str = username
        self.role: str = role
        self.private_key: str = private_key
        self.private_key_passphrase: str = private_key_passphrase

    def open(self):
        """
        Connects to Snowflake using key pair authentication.
        """
        try:
            if self.connected:
                return self.con

            with open(self.private_key, 'rb') as key_file:
                private_key_data = key_file.read()

            # Load the encrypted private key using the passphrase
            if self.private_key_passphrase:
                private_key = serialization.load_pem_private_key(
                    private_key_data,
                    password=self.private_key_passphrase.encode(),
                )
            else:
                raise ValueError("Private key passphrase must be provided for encrypted key.")

            # Use the Snowflake connection
            conn = snowflake.connector.connect(
                user=self.username,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                private_key=private_key,
                authenticator='snowflake'
            )

            self.con = conn
            self.connected = True
            return conn

        except snowflake.connector.errors.OperationalError as e:
            raise Exception(f"Failed to connect to Snowflake: {e}")

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
            message=f"Executing query: {query}", )
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def activate_cursor(self):
        if self.connected:
            if self.cursor is None:
                self.cursor = self.con.cursor()

    def close_cursor(self):
        self.cursor.close()
