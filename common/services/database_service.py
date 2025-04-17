from abc import ABC, abstractmethod
from common.connections.database_connection import DatabaseConnection

import pandas as pd
from pandas import DataFrame


class DatabaseService(ABC):
    def __init__(self, parameters: dict):
        self.schema: str = parameters['schema']
        self.db_connection: DatabaseConnection | None = None

    @abstractmethod
    def get_connection(self):
        pass

    @staticmethod
    @abstractmethod
    def create_sql_str(table_df: DataFrame) -> str:
        pass

    @abstractmethod
    def check_if_table_exists(self, table: str):
        pass

    @abstractmethod
    def retrieve_column_info(self, table_name: str):
        pass

    @abstractmethod
    def create_all_tables(self, starting_directory: str, metadata_table: pd.DataFrame):
        pass

    @abstractmethod
    def create_single_table(self, table_name: str, filtered_metadata: pd.DataFrame):
        pass

    @abstractmethod
    def drop_tables_in_schema(self, tables: list | tuple):
        pass

    @abstractmethod
    def delete_data_from_table(self, starting_directory: str, file_format: str,
                               file_format_name: str, manifest_table: pd.DataFrame):
        pass
