import pandas as pd
from pandas import DataFrame
from pyarrow import ExtensionArray

from accelerators.databricks.connections.databricks_connection import DatabricksConnection
from common.services.database_service import DatabaseService
from common.utilities import log_message, update_table_name_that_starts_with_digit


class DatabricksService(DatabaseService):
    def __init__(self, parameters: dict):
        super().__init__(parameters)
        self.catalog = parameters['catalog']
        self.server_hostname = parameters['server_hostname']
        self.http_path = parameters['http_path']
        self.access_token = parameters['access_token']
        self.infer_schema = parameters['infer_schema']
        self.db_connection = self.get_connection()

    def get_connection(self) -> DatabricksConnection:
        conn = DatabricksConnection(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token,
            catalog=self.catalog
        )
        conn.execute_query(f"""USE {self.catalog}.{self.schema}""")
        return conn

    @staticmethod
    def create_sql_str_column_definitions(table_df: DataFrame, is_picklist: bool = False, is_modify: bool = False, is_add: bool = False) -> str:
        """
        Generates a SQL string for creating or modifying table columns in Databricks.

        :param is_add:
        :param is_modify:
        :param table_df: A DataFrame containing column details (column_name, type, length).
        :param is_picklist: A boolean indicating if the table is the picklist table.
        :return: A partial SQL string defining the table columns.
        """
        column_definitions = []

        for _, row in table_df.iterrows():
            column_name = row['column_name'].lower()
            column_def = f"{column_name} STRING"
            column_definitions.append(column_def)
        return ", ".join(column_definitions)

    def check_if_schema_exists(self):

        query: str = f"""
                        SELECT schema_name
                        FROM information_schema.schemata
                        WHERE 
                        schema_name = '{self.schema}'
                """
        try:
            schema_exists_result: list = self.db_connection.execute_query(query)
            if schema_exists_result and schema_exists_result[0][0]:
                log_message(log_level='Info',
                            message=f'{self.schema} exists.')
            else:
                log_message(log_level='Info',
                            message=f'{self.schema} does not exist. Creating new schema')
                create_schema_query: str = f"""
                            CREATE SCHEMA {self.schema};
                        """
                self.db_connection.execute_query(create_schema_query)

        except Exception as e:
            log_message(log_level='Error',
                        message=f'Error checking if schema {self.schema} exists',
                        exception=e)
            raise e

    def retrieve_column_info(self, table_name: str) -> dict:
        existing_columns_query = f"""
        SELECT column_name, data_type 
        FROM {self.catalog}.information_schema.columns 
        WHERE table_name = '{table_name.lower()}';
        """
        sql_result = self.db_connection.execute_query(existing_columns_query)
        columns = ['column_name', 'data_type']
        existing_columns_df = pd.DataFrame(sql_result, columns=columns)

        # Convert to dictionary for quick lookup
        return {
            row['column_name'].lower(): {
                "data_type": row["data_type"].lower()
            }
            for _, row in existing_columns_df.iterrows()
        }

    def create_all_tables(self, starting_directory: str, metadata_table: pd.DataFrame):

        unique_extract_values: ExtensionArray = metadata_table["extract"].unique()

        for extract in unique_extract_values:
            filtered_metadata = metadata_table[metadata_table["extract"] == extract]
            # Vault allows 255 characters for description__sys, but the extract metadata defines a length of 128.
            if extract == "Object.security_policy__sys":
                filtered_metadata.loc[
                    (filtered_metadata['column_name'] == 'description__sys'),
                    ['type', 'length']
                ] = ['String', 255]

            new_table_name = update_table_name_that_starts_with_digit(extract.split(".")[1])
            self.create_single_table(table_name=new_table_name, filtered_metadata=filtered_metadata)

        # Create metadata table
        column_definitions: dict = {col: ["STRING"] for col in metadata_table.columns}
        new_metadata_df = pd.DataFrame.from_dict(column_definitions).astype(str)
        new_metadata_df.loc["length"] = 1000
        new_metadata_df = new_metadata_df.T.reset_index()
        new_metadata_df.columns = ["column_name", "type", "length"]

        self.create_single_table(table_name="metadata", filtered_metadata=new_metadata_df)

    def create_single_table(self, table_name: str, filtered_metadata: pd.DataFrame):
        column_definitions: str = self.create_sql_str_column_definitions(
            table_df=filtered_metadata)

        self.db_connection.execute_query(f"""
                    CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.{table_name}({column_definitions})
                """)

    def add_columns_to_table(self, columns_to_add: pd.DataFrame, table_name: str):
        column_definitions: str = self.create_sql_str_column_definitions(
            table_df=columns_to_add, is_picklist=False,
            is_modify=False, is_add=True)
        alter_query: str = f"""
                        ALTER TABLE {self.schema}.{table_name}
                        ADD COLUMNS ({column_definitions})
                    """
        self.db_connection.execute_query(alter_query)

    def drop_tables_in_schema(self, tables: list | tuple):
        for (table_name,) in tables:
            drop_query: str = f'DROP TABLE {table_name};'
            self.db_connection.execute_query(drop_query)

        log_message(log_level='Info',
                    message=f"Tables dropped successfully")

    def drop_table(self, table_name: str):
        drop_query: str = f'DROP TABLE IF EXISTS {self.schema}.{table_name};'
        self.db_connection.execute_query(drop_query)

    def drop_columns_from_table(self, table_name: str, columns: list):
        enable_column_mapping_mode_query = f"""
            ALTER TABLE {self.schema}.{table_name} 
            SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
        """
        self.db_connection.execute_query(query=enable_column_mapping_mode_query)

        drop_column_query: str = f"""
            ALTER TABLE {self.schema}.{table_name} 
            DROP COLUMN {", ".join(f'{col}' for col in columns)}
        """

        self.db_connection.execute_query(drop_column_query)

    def delete_data_from_table(self, starting_directory: str,
                               manifest_table: pd.DataFrame):

        deletes_filter: DataFrame = manifest_table[(manifest_table["type"] == "deletes") & (manifest_table["records"] > 0)]

        for index, row in deletes_filter.iterrows():
            self.process_delete(row=row,
                                starting_directory=starting_directory)

    def create_staging_table(self, staging_table_name: str, **kwargs):
        file_format_name: str = "PARQUET" if self.convert_to_parquet else "CSV"
        create_staging_table_query: str = f"""
                            CREATE TEMP TABLE {staging_table_name}
                            USING {file_format_name}
                            OPTIONS (
                                path '{kwargs['object_path']}',
                                header 'true',
                                inferSchema 'false'
                            );
                        """

        self.db_connection.execute_query(query=create_staging_table_query)

    def insert_into_target_table(self, table_name: str, **kwargs):
        file_format_name: str = "PARQUET" if self.convert_to_parquet else "CSV"
        copy_into_query: str = f"""
                        COPY INTO {self.schema}.{table_name}
                        FROM '{kwargs['object_path']}'
                        FILEFORMAT = {file_format_name}
                        FORMAT_OPTIONS ('inferSchema' ='false',
                                        'delimiter' = ',',
                                        'header' = 'true')
                        COPY_OPTIONS ('inferSchema' ='false');
                    """
        self.db_connection.execute_query(query=copy_into_query)

        merge_into_query: str = f"""
                                MERGE INTO {table_name} AS target
                                USING {kwargs['staging_table_name']} AS source
                                ON {kwargs['on_condition']}
                                WHEN MATCHED THEN
                                    UPDATE SET * 
                                WHEN NOT MATCHED THEN
                                    INSERT  *; 
                            """

        self.db_connection.execute_query(merge_into_query)

    def process_delete(self, row: pd.Series, starting_directory: str):
        raw_table = row["extract"].split(".")[1]
        related_file = row["file"]
        if self.convert_to_parquet:
            related_file = related_file.replace(".csv", ".parquet")
            file_format = "PARQUET"
        else:
            file_format = "CSV"
        s3_file_uri = f"{self.object_storage_root}/{starting_directory}/{related_file}"
        table_name = update_table_name_that_starts_with_digit(raw_table.replace('_deletes', ''))
        if raw_table != "metadata_deletes":
            if table_name == 'picklist__sys':
                column_names = 'object, object_field, picklist_value_name'
            elif table_name == 'metadata':
                column_names = 'extract, column_name'
            else:
                column_names = 'id'

            # Split column names into a list
            column_names_list = column_names.split(', ')

            # Build the ON condition by dynamically inserting AND between column comparisons
            on_condition = ' AND '.join([f"target.{col} = source.{col}" for col in column_names_list])

            create_query = f"""
                    CREATE OR REPLACE TEMPORARY VIEW temp_{table_name}_deletes
                    USING {file_format}
                    OPTIONS (
                        path '{s3_file_uri}',
                        header 'true',
                        inferSchema 'true'
                    );
                """

            # Load the data from the CSV file into the temporary view
            self.db_connection.execute_query(create_query)

            # Delete the matching rows from the target table
            delete_query = f"""
                    MERGE INTO {self.catalog}.{self.schema}.{table_name} AS target
                    USING temp_{table_name}_deletes AS source
                    ON {on_condition}
                    WHEN MATCHED THEN DELETE;
                """

            self.db_connection.execute_query(delete_query)

    def load_full_or_log_data(self, table_name: str, object_path: str, headers: list = None):
        file_format_name: str = "PARQUET" if self.convert_to_parquet else "CSV"
        if self.convert_to_parquet:
            self.db_connection.execute_query(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}.{table_name}
                    USING DELTA
                    AS SELECT * FROM parquet.`{object_path}` 
                    LIMIT 0;
                """)

        self.db_connection.execute_query(f"""
                        COPY INTO {self.schema}.{table_name}
                        FROM '{object_path}'
                        FILEFORMAT = {file_format_name}
                        FORMAT_OPTIONS ('inferSchema' ='{self.infer_schema}',
                                        'delimiter' = ',',
                                        'header' = 'true')
                        COPY_OPTIONS ('inferSchema' ='{self.infer_schema}');
                    """)

    def load_incremental_data(self, table_name: str, object_path: str, headers: str = None):
        if table_name == 'picklist__sys':
            column_names = 'object, object_field, picklist_value_name'
        elif table_name == 'metadata':
            column_names = 'extract, column_name'
        else:
            column_names = 'id'

        column_names_list = column_names.split(', ')
        staging_table_name: str = f"{table_name}_staging"

        on_condition = ' AND '.join([f"target.{col} = source.{col}" for col in column_names_list])

        self.create_staging_table(staging_table_name=staging_table_name, object_path=object_path)
        self.insert_into_target_table(
            table_name=table_name,
            object_path=object_path,
            staging_table_name=staging_table_name,
            on_condition=on_condition
        )
