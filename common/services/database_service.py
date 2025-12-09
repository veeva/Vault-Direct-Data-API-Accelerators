from abc import ABC, abstractmethod
from common.connections.database_connection import DatabaseConnection

import pandas as pd
from pandas import DataFrame


class DatabaseService(ABC):
    def __init__(self, parameters: dict):
        self.convert_to_parquet: bool = parameters.get('convert_to_parquet', False)
        self.object_storage_root: str = parameters.get('object_storage_root', '')
        self.schema: str = parameters.get('schema', '')
        self.db_connection: DatabaseConnection | None = None

    @abstractmethod
    def get_connection(self):
        pass

    @staticmethod
    @abstractmethod
    def create_sql_str_column_definitions(table_df: DataFrame, is_picklist: bool = False, is_modify: bool = False, is_add: bool = False) -> str:
        pass

    @abstractmethod
    def check_if_schema_exists(self):
        """
        Checks if the specified schema exists in the database.
        """
        pass

    @abstractmethod
    def retrieve_column_info(self, table_name: str) -> dict:
        pass

    @abstractmethod
    def create_all_tables(self, starting_directory: str, metadata_table: pd.DataFrame):
        pass

    @abstractmethod
    def create_single_table(self, table_name: str, filtered_metadata: pd.DataFrame):
        pass

    @abstractmethod
    def add_columns_to_table(self, columns_to_add: pd.DataFrame, table_name: str):
        pass

    @abstractmethod
    def drop_tables_in_schema(self, tables: list | tuple):
        pass

    @abstractmethod
    def drop_table(self, table_name: str):
        pass

    @abstractmethod
    def drop_columns_from_table(self, table_name: str, columns: list):
        pass

    @abstractmethod
    def delete_data_from_table(self, starting_directory: str, manifest_table: pd.DataFrame):
        pass

    @abstractmethod
    def create_staging_table(self, staging_table_name: str, **kwargs):
        pass

    @abstractmethod
    def insert_into_target_table(self, table_name: str, **kwargs):
        pass

    @abstractmethod
    def load_full_or_log_data(self, table_name: str, object_path: str, headers: str = None):
        """
        Loads data into the specified table from the given object path.

        :param table_name:
        :param object_path:
        :param headers:
        :return:
        """
        pass

    @abstractmethod
    def load_incremental_data(self, table_name: str, object_path: str, headers: str = None):
        pass
