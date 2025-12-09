import pandas as pd
from pandas import DataFrame
from pyarrow import ExtensionArray

from accelerators.sqlite.connections.sqlite_connection import SqliteConnection
from common.services.database_service import DatabaseService
from common.utilities import log_message, update_table_name_that_starts_with_digit


class SqliteService(DatabaseService):
    def __init__(self, parameters: dict):
        super().__init__(parameters)
        self.databases_folder = parameters['databases_folder']
        self.database = parameters['database']
        self.db_connection: SqliteConnection = self.get_connection()

    def get_connection(self) -> SqliteConnection:
        return SqliteConnection(
            databases_folder=self.databases_folder,
            database=self.database
        )

    @staticmethod
    def create_sql_str_column_definitions(table_df: DataFrame, is_picklist: bool = False, is_modify: bool = False,
                                          is_add: bool = False) -> str:
        """
        Generates a SQL string for creating table columns in Redshift.

        :param is_add:
        :param is_modify:
        :param table_df: A DataFrame containing column details (column_name, type, length).
        :param is_picklist: A boolean indicating if the table is the picklist table.
        :return: A partial SQL string defining the table columns.
        """
        column_definitions = []

        for _, row in table_df.iterrows():
            column_name = row['column_name'].lower()
            data_type = row['type'].lower()

            if  data_type == 'string':
                column_def = f'"{column_name}" TEXT'
            if data_type in ["datetime", "timestamp with time zone"]:
                column_def = f'"{column_name}" TEXT'
            elif data_type == "boolean":
                column_def = f'"{column_name}" INTEGER'
            elif data_type in ["number", "numeric"]:
                column_def = f'"{column_name}" INTEGER'
            else:
                column_def = f'"{column_name}" TEXT'

            column_definitions.append(column_def)

        if is_modify and not is_add:
            return "ALTER COLUMN " + ", ALTER COLUMN ".join(column_definitions)
        elif is_add and not is_modify:
            return "ADD COLUMN " + ", ADD COLUMN ".join(column_definitions)
        elif is_modify and is_add:
            raise Exception("Cannot modify and add columns at once")
        else:
            if is_picklist:
                column_definitions.append(
                    'CONSTRAINT picklist_primary_key PRIMARY KEY (object, object_field, picklist_value_name)'
                )
        return ", ".join(column_definitions)

    def check_if_schema_exists(self):

        query = f"""
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.schemata
                        WHERE 
                        schema_name = '{self.schema}'
                    )
                """
        try:
            schema_exists_result = self.db_connection.execute_query(query)
            if schema_exists_result and schema_exists_result[0][0]:
                log_message(log_level='Info',
                            message=f'{self.schema} exists.')
            else:
                log_message(log_level='Info',
                            message=f'{self.schema} does not exist. Creating new schema')
                create_schema_query = f"""
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
            SELECT
                name AS COLUMN_NAME,
                type AS DATA_TYPE
            FROM
                pragma_table_info('{table_name}');
        """
        sql_result = self.db_connection.execute_query(existing_columns_query)
        columns = ['COLUMN_NAME', 'DATA_TYPE']
        existing_columns_df = pd.DataFrame(sql_result, columns=columns)

        # Convert to dictionary for quick lookup
        return {
            row['COLUMN_NAME'].lower(): {
                "DATA_TYPE": row["DATA_TYPE"].lower()
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
        is_picklist = False

        if table_name == "picklist":
            is_picklist = True
        sql_string = self.create_sql_str_column_definitions(filtered_metadata, is_picklist=is_picklist, is_modify=False,
                                                            is_add=False)

        self.db_connection.execute_query(f"""
                CREATE TABLE IF NOT EXISTS {table_name} ({sql_string})
            """)

    def add_columns_to_table(self, columns_to_add: pd.DataFrame, table_name: str):
        column_definitions: str = self.create_sql_str_column_definitions(
            table_df=columns_to_add, is_picklist=False,
            is_modify=False, is_add=True)
        for column_def in column_definitions.split(", "):
            alter_query: str = f"""
                            ALTER TABLE {table_name} {column_def}
                        """
            self.db_connection.execute_query(alter_query)

    def drop_tables_in_schema(self, tables: list | tuple):
        for (table_name,) in tables:
            drop_query: str = f'DROP TABLE IF EXISTS {table_name};'
            log_message(log_level='Info',
                        message=f"Executing: {drop_query}")
            self.db_connection.execute_query(drop_query)

        log_message(log_level='Info',
                    message=f"Tables dropped successfully", )

    def drop_table(self, table_name: str):
        drop_query: str = f'DROP TABLE IF EXISTS {self.schema}.{table_name};'
        self.db_connection.execute_query(drop_query)

    def drop_columns_from_table(self, table_name: str, columns: list):
        for column_name in columns:
            drop_column_query: str = f"""
                ALTER TABLE {table_name}
                DROP COLUMN "{column_name}";
            """
            self.db_connection.execute_query(drop_column_query)

    def delete_data_from_table(self, starting_directory: str,
                               manifest_table: pd.DataFrame):

        deletes_filter: DataFrame = manifest_table[
            (manifest_table["type"] == "deletes") & (manifest_table["records"] > 0)]

        for index, row in deletes_filter.iterrows():
            self.process_delete(row=row,
                                starting_directory=starting_directory)

    def create_staging_table(self, staging_table_name: str, **kwargs):

        df_deletions: DataFrame = pd.read_csv(kwargs['object_path'])

        df_deletions.to_sql(
            staging_table_name,
            self.db_connection.con,
            if_exists='replace',
            index=False
        )

    def delete_duplicate_rows_from_table(self, table_name: str, staging_table_name: str, primary_keys: list):
        if len(primary_keys) == 1:
            # --- SINGLE PRIMARY KEY ---
            pk_col = primary_keys[0]
            delete_query = f"""
            DELETE FROM {table_name}
            WHERE {pk_col} IN (SELECT {pk_col} FROM {staging_table_name});
            """
        else:
            # --- COMPOSITE PRIMARY KEY ---
            pk_conditions = " AND ".join(
                [f"staging.{col} = {table_name}.{col}" for col in primary_keys]
            )
            delete_query = f"""
            DELETE FROM {table_name}
            WHERE EXISTS (
                SELECT 1 FROM {staging_table_name} AS staging
                WHERE {pk_conditions}
            );
            """

        # delete_duplicates_query = f"""
        #                 DELETE FROM {table_name}
        #                 WHERE {pk_condition}
        #                 IN (SELECT {pk_condition} FROM {staging_table_name});
        #             """
        self.db_connection.execute_query(delete_query)

    def insert_into_target_table(self, table_name: str, **kwargs):
        insert_into_query: str = f"""
                        INSERT INTO {table_name}
                        SELECT DISTINCT * FROM {kwargs['staging_table_name']};
                    """
        self.db_connection.execute_query(insert_into_query)

    def process_delete(self, row: pd.Series, starting_directory: str):
        raw_table = row["extract"].split(".")[1]
        related_file = row["file"]
        file_path: str = f"{starting_directory}/{related_file}"
        table_name = update_table_name_that_starts_with_digit(raw_table.replace('_deletes', ''))
        if raw_table != "metadata_deletes":
            columns = ''
            column_names = ''
            if table_name == 'picklist__sys':
                column_names += 'object || object_field || picklist_value_name'
                columns += 'object TEXT, object_field TEXT, picklist_value_name TEXT'
            elif table_name == 'metadata':
                column_names += 'extract || column_name'
                columns += 'extract TEXT, column_name TEXT'
            else:
                column_names += 'id'
                columns += 'id TEXT'

            # Load the CSV keys into a pandas DataFrame
            df_deletions: DataFrame = pd.read_csv(file_path)

            # Create a temp table
            temp_table_name: str = f'temp_{table_name}_deletes'
            df_deletions.to_sql(
                temp_table_name,
                self.db_connection.con,
                if_exists='replace',
                index=False
            )
            delete_query = (f"DELETE FROM {table_name} "
                            f"WHERE {column_names} IN (SELECT {column_names} FROM {temp_table_name});")

            self.db_connection.execute_query(delete_query)

            # Delete the temp table
            drop_temp_table_query = f"DROP TABLE IF EXISTS {temp_table_name};"
            self.db_connection.execute_query(drop_temp_table_query)


    def load_full_or_log_data(self, table_name: str, object_path: str, headers: list = None):
        dataframe: DataFrame = pd.read_csv(object_path, low_memory=False)
        dataframe.to_sql(name=table_name,
                         con=self.db_connection.con,
                         if_exists='append',
                         index=False)
        log_message(log_level='Info',
                    message=f'Loaded {len(dataframe)} records into {table_name}')

    def load_incremental_data(self, table_name: str, object_path: str, headers: str = None):
        if table_name == 'metadata':
            primary_keys = ["extract", "column_name"]
        elif table_name == 'picklist__sys':
            primary_keys = ["object", "object_field", "picklist_value_name"]
        else:
            primary_keys = ["id"]

        # Load into a temporary staging table
        staging_table_name: str = f"{table_name}_staging"

        self.create_staging_table(staging_table_name=staging_table_name,
                                  table_name=table_name,
                                  object_path=object_path,
                                  csv_headers=headers)

        self.delete_duplicate_rows_from_table(table_name=table_name,
                                              staging_table_name=staging_table_name,
                                              primary_keys=primary_keys)

        self.insert_into_target_table(table_name=table_name,
                                      staging_table_name=staging_table_name)

        self.db_connection.execute_query(f'DROP TABLE IF EXISTS {staging_table_name};')
