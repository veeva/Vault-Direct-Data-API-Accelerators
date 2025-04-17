import pandas as pd
from pandas import DataFrame

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
        self.external_storage = parameters['external_storage']
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
    def create_sql_str(table_df: DataFrame) -> str:
        """
        Generates a SQL string for creating or modifying table columns in Databricks.

        :param table_df: A DataFrame containing column details (column_name, type, length).
        :return: A partial SQL string defining the table columns.
        """
        column_definitions = []

        for _, row in table_df.iterrows():
            column_name = row['column_name'].lower()
            column_def = f"{column_name} STRING"
            column_definitions.append(column_def)
        return ", ".join(column_definitions)

    def check_if_table_exists(self, table) -> bool:
        query = f"""
        SELECT COUNT(*) FROM {self.catalog}.information_schema.tables 
        WHERE table_name = '{table.lower()}';
        """

        query_result = self.db_connection.execute_query(query)
        return query_result[0][0] > 0

    def retrieve_column_info(self, table_name):
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

        unique_extract_values = metadata_table["extract"].unique()

        for extract in unique_extract_values:
            filtered_metadata = metadata_table[metadata_table["extract"] == extract]
            new_table_name = update_table_name_that_starts_with_digit(extract.split(".")[1])
            self.create_single_table(table_name=new_table_name, filtered_metadata=filtered_metadata)

        # Create metadata table
        column_definitions = {col: ["STRING"] for col in metadata_table.columns}
        new_metadata_df = pd.DataFrame.from_dict(column_definitions).astype(str)
        new_metadata_df.loc["length"] = 1000
        new_metadata_df = new_metadata_df.T.reset_index()
        new_metadata_df.columns = ["column_name", "type", "length"]

        self.create_single_table(table_name="metadata", filtered_metadata=new_metadata_df)

    def create_single_table(self, table_name: str, filtered_metadata: pd.DataFrame):
        sql_string = self.create_sql_str(filtered_metadata)

        self.db_connection.execute_query(f"""
                    CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.{table_name}({sql_string})
                """)

    def drop_tables_in_schema(self, tables: list | tuple):
        for (table_name,) in tables:
            drop_query: str = f'DROP TABLE {table_name};'
            self.db_connection.execute_query(drop_query)

        log_message(log_level='Info',
                    message=f"Tables dropped successfully")

    def delete_data_from_table(self, starting_directory: str, file_extension: str,
                               file_format_name: str,
                               manifest_table: pd.DataFrame):
        deletes_filter = manifest_table[(manifest_table["type"] == "deletes") & (manifest_table["records"] > 0)]

        for index, row in deletes_filter.iterrows():
            self.process_delete(row=row, starting_directory=starting_directory,
                                file_extension=file_extension, file_format_name=file_format_name)

    def load_data_into_tables(self,
                              extract_type: str,
                              table_name: str,
                              temp_table_name: str,
                              file_format_name: str,
                              s3_uri: str):
        if extract_type == "incremental":
            if table_name == 'picklist__sys':
                column_names = 'object, object_field, picklist_value_name'
            elif table_name == 'metadata':
                column_names = 'extract, column_name'
            else:
                column_names = 'id'

            column_names_list = column_names.split(', ')

            on_condition = ' AND '.join([f"target.{col} = source.{col}" for col in column_names_list])
            temp_table_create_sql_string = f"""
                            CREATE TEMP TABLE {temp_table_name}
                            USING {file_format_name}
                            OPTIONS (
                                path '{s3_uri}',
                                header 'true',
                                inferSchema 'false'
                            );
                        """

            self.db_connection.execute_query(f"""
                        COPY INTO {self.schema}.{table_name}
                        FROM '{s3_uri}'
                        FILEFORMAT = {file_format_name}
                        FORMAT_OPTIONS ('inferSchema' ='false',
                                        'delimiter' = ',',
                                        'header' = 'true')
                        COPY_OPTIONS ('inferSchema' ='false');
                    """)

            self.db_connection.execute_query(temp_table_create_sql_string)

            self.db_connection.execute_query(f"""
                                MERGE INTO {table_name} AS target
                                USING {temp_table_name} AS source
                                ON {on_condition}
                                WHEN MATCHED THEN
                                    UPDATE SET * 
                                WHEN NOT MATCHED THEN
                                    INSERT  *; 
                            """)

        elif extract_type == "full" or "log":

            self.db_connection.execute_query(f"""
                        COPY INTO {self.schema}.{table_name}
                        FROM '{s3_uri}'
                        FILEFORMAT = {file_format_name}
                        FORMAT_OPTIONS ('inferSchema' ='false',
                                        'delimiter' = ',',
                                        'header' = 'true')
                        COPY_OPTIONS ('inferSchema' ='false');
                    """)

    def process_delete(self, row: pd.Series, starting_directory: str, file_extension: str, file_format_name: str):
        raw_table = row["extract"].split(".")[1]
        related_file = row["file"]
        if file_extension == ".parquet":
            related_file = related_file.replace(".csv", ".parquet")
            file_format = "PARQUET"
        else:
            file_format = "CSV"
        s3_file_uri = f"s3://{self.external_storage}/{starting_directory}/{related_file}"
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

    def process_manifest_row(self,
                             row: pd.Series,
                             starting_directory: str,
                             file_extension: str,
                             file_format_name: str,
                             extract_type: str, ):

        raw_table_name: str = row["extract"].split(".")[1]
        table_name: str = update_table_name_that_starts_with_digit(raw_table_name)
        s3_uri: str = f"s3://{self.external_storage}/{starting_directory}/{row['file']}"

        if file_extension == ".parquet":
            s3_uri = s3_uri.replace(".csv", ".parquet")
        if table_name != 'manifest' and not (extract_type == "incremental" and "metadata" in table_name):
            temp_table_name = f"{table_name}_temp"

            self.load_data_into_tables(extract_type=extract_type,
                                       table_name=table_name,
                                       temp_table_name=temp_table_name,
                                       file_format_name=file_format_name,
                                       s3_uri=s3_uri)
