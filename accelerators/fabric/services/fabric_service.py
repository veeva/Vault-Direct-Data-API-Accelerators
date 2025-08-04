import pandas as pd
from azure.identity import DefaultAzureCredential
from pandas import DataFrame
from pyarrow import ExtensionArray

from accelerators.fabric.connections.fabric_connection import FabricConnection
from common.services.database_service import DatabaseService
from common.utilities import log_message, update_table_name_that_starts_with_digit


class FabricService(DatabaseService):
    def __init__(self, parameters: dict):
        super().__init__(parameters)
        self.server_name: str = parameters['server_name']
        self.database: str = parameters['database']
        self.schema: str = parameters['schema']
        self.credential = DefaultAzureCredential()
        self.connection_string: str = (parameters['connection_string']
                                       .replace('{server_name}', self.server_name)
                                       .replace('{database}', self.database))
        self.db_connection: FabricConnection = self.get_connection()

    def get_connection(self) -> FabricConnection:
        return FabricConnection(
            server_name=self.server_name,
            database=self.database,
            connection_string=self.connection_string,
            credential=self.credential
        )

    @staticmethod
    def create_sql_str_column_definitions(table_df: DataFrame, is_picklist: bool = False, is_modify: bool = False,
                                          is_add: bool = False) -> str:
        """
        Generates a SQL string for creating table columns in Fabric Warehouse.

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

            if data_type == "id" or (column_name == 'id' and data_type == 'string'):
                column_def = f'[{column_name}] VARCHAR(MAX)'
            elif data_type in ["datetime", "timestamp with time zone"]:
                column_def = f'[{column_name}] DATETIME2(3)'
            elif data_type == "boolean":
                column_def = f'[{column_name}] BIT'
            elif data_type in ["number", "numeric"]:
                column_def = f'[{column_name}] NUMERIC'
            else:
                column_def = f'"{column_name}" VARCHAR(MAX)'

            column_definitions.append(column_def)

        if is_modify and not is_add:
            return "ALTER COLUMN " + ", ALTER COLUMN ".join(column_definitions)
        elif is_add and not is_modify:
            return "ADD " + ", ADD ".join(column_definitions)
        elif is_modify and is_add:
            raise Exception("Cannot modify and add columns at once")
        else:
            if is_picklist:
                column_definitions.append(
                    'CONSTRAINT picklist_primary_key PRIMARY KEY (object, object_field, picklist_value_name)'
                )
        return ", ".join(column_definitions)

    @staticmethod
    def create_sql_str_select_statement(target_columns_info: list) -> str:
        select_expressions_sql_list = []
        for col_info in target_columns_info:
            target_col_name = col_info['name']
            target_system_type_full = col_info['system_type_name']

            source_csv_col_access_in_raw = f"raw.[{target_col_name}]"

            expression = ""

            # General pre-cleaning for many non-string types: trim and convert empty string to NULL
            cleaned_source_for_conversion = f"NULLIF(TRIM({source_csv_col_access_in_raw}), '')"

            if target_system_type_full == "bit":
                expression = (f"TRY_CAST((CASE LOWER(TRIM({source_csv_col_access_in_raw})) "
                              f"WHEN 'true' THEN 1 WHEN 'false' THEN 0 "
                              f"WHEN '1' THEN 1 WHEN '0' THEN 0 ELSE NULL END) AS bit)")

            elif "char" in target_system_type_full.lower() or "text" in target_system_type_full.lower():
                expression = f"TRY_CAST(TRIM({source_csv_col_access_in_raw}) AS {target_system_type_full})"

            elif target_system_type_full.startswith("binary") or target_system_type_full.startswith("varbinary"):
                expression = f"TRY_CAST({cleaned_source_for_conversion} AS {target_system_type_full})"

            else:  # Handles numeric, datetime2, etc.
                expression = f"TRY_CAST({cleaned_source_for_conversion} AS {target_system_type_full})"

            select_expressions_sql_list.append(f"{expression} AS [{target_col_name}]")

        # 1. Join the SELECT expressions into a single string for the SQL query
        select_expressions_sql_str = ",\n                    ".join(select_expressions_sql_list)
        return select_expressions_sql_str

    def check_if_schema_exists(self):

        query: str = f"""
                        SELECT schema_name
                        FROM INFORMATION_SCHEMA.SCHEMATA
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
            SELECT column_name, data_type, character_maximum_length
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE
            table_catalog = '{self.database}' 
            AND table_schema = '{self.schema}'
            AND table_name = '{table_name}';
        """
        sql_result: list = self.db_connection.execute_query(existing_columns_query)
        # Convert pyodbc.Row items to a list of tuples
        sql_result_tuples = [tuple(row) for row in sql_result]

        columns: list = ['COLUMN_NAME', 'DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH']
        existing_columns_df = pd.DataFrame(sql_result_tuples, columns=columns)

        # Convert to dictionary for quick lookup
        return {
            row['COLUMN_NAME'].lower(): {
                "DATA_TYPE": row["DATA_TYPE"].lower(),
                "CHARACTER_MAXIMUM_LENGTH": row["CHARACTER_MAXIMUM_LENGTH"]
            }
            for _, row in existing_columns_df.iterrows()
        }

    def create_all_tables(self, starting_directory: str, metadata_table: pd.DataFrame):

        unique_extract_values: ExtensionArray = metadata_table["extract"].unique()

        for extract in unique_extract_values:
            filtered_metadata: DataFrame = metadata_table[metadata_table["extract"] == extract]
            # Vault allows 255 characters for description__sys, but the extract metadata defines a length of 128.
            if extract == "Object.security_policy__sys":
                filtered_metadata.loc[
                    (filtered_metadata['column_name'] == 'description__sys'),
                    ['type', 'length']
                ] = ['String', 255]

            new_table_name: str = update_table_name_that_starts_with_digit(extract.split(".")[1])
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
        sql_string: str = self.create_sql_str_column_definitions(filtered_metadata, is_picklist=is_picklist,
                                                                 is_modify=False, is_add=False)

        self.db_connection.execute_query(f"""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}' AND schema_id = SCHEMA_ID('{self.schema}'))
                CREATE TABLE {self.schema}.{table_name} ({sql_string});
            """)

    def add_columns_to_table(self, columns_to_add: pd.DataFrame, table_name: str):
        column_definitions: str = self.create_sql_str_column_definitions(
            table_df=columns_to_add, is_picklist=False,
            is_modify=False, is_add=True)
        for column_def in column_definitions.split(", "):
            alter_query: str = f"""
                            ALTER TABLE {self.schema}.{table_name} {column_def}
                        """
            self.db_connection.execute_query(alter_query)

    def drop_tables_in_schema(self, tables: list | tuple):
        for (table_name,) in tables:
            drop_query: str = f'DROP TABLE {self.schema}."{table_name}";'
            log_message(log_level='Info',
                        message=f"Executing: {drop_query}",
                        context=None)
            self.db_connection.execute_query(drop_query)

        log_message(log_level='Info',
                    message=f"Tables dropped successfully")

    def drop_table(self, table_name: str):
        drop_query: str = f'DROP TABLE IF EXISTS {self.schema}.{table_name};'
        self.db_connection.execute_query(drop_query)

    def drop_columns_from_table(self, table_name: str, columns: list):
        for column_name in columns:
            drop_column_query: str = f"""
                ALTER TABLE {self.schema}.{table_name}
                DROP COLUMN [{column_name}];
            """
            self.db_connection.execute_query(drop_column_query)

    def delete_data_from_table(self, starting_directory: str,
                               manifest_table: pd.DataFrame):

        deletes_filter: DataFrame = manifest_table[
            (manifest_table["type"] == "deletes") & (manifest_table["records"] > 0)]

        for index, row in deletes_filter.iterrows():
            self.process_delete(row=row,
                                starting_directory=starting_directory)

    def create_raw_data_table(self, raw_data_table_name: str, headers: list[str] = None):
        column_definitions_list = [f"[{col_name}] VARCHAR(MAX)" for col_name in headers]
        column_definitions_sql_str = ",\n                ".join(column_definitions_list)

        create_raw_data_table_query: str = f"""
                        CREATE TABLE {self.schema}.{raw_data_table_name} ({column_definitions_sql_str});
                        """
        self.db_connection.execute_query(query=create_raw_data_table_query)

    def copy_into_raw_data_table(self, raw_data_table_name: str, object_path: str):
        file_type = 'PARQUET' if self.convert_to_parquet else 'CSV'
        with_statement: str = \
            f"""WITH (
                    FILE_TYPE = '{file_type}'
                );
            """ if self.convert_to_parquet else f"""
            WITH (
                FILE_TYPE = '{file_type}',
                FIRSTROW = 2
            );
        """
        copy_into_raw_data_table_query: str = f"""
            COPY INTO {self.schema}.{raw_data_table_name}
            FROM '{object_path}'
            {with_statement}
            """

        self.db_connection.execute_query(copy_into_raw_data_table_query)

    def create_staging_table(self, staging_table_name: str, **kwargs):
        create_staging_table_query: str = f"""
                        SELECT TOP 0 *
                        INTO {self.schema}.{staging_table_name}
                        FROM {self.schema}.{kwargs['table_name']};
                    """

        self.db_connection.execute_query(create_staging_table_query)

    def get_staging_table_schema_rows(self, staging_table_name: str) -> list:
        select_staging_table_schema_query: str = f"""SELECT
                                                        COLUMN_NAME,
                                                        DATA_TYPE,
                                                        IS_NULLABLE,
                                                        ORDINAL_POSITION
                                                    FROM
                                                        INFORMATION_SCHEMA.COLUMNS
                                                    WHERE
                                                        TABLE_SCHEMA = '{self.schema}'
                                                        AND TABLE_NAME = '{staging_table_name}'
                                                    ORDER BY
                                                        ORDINAL_POSITION;
                                        """
        target_schema_rows: list = self.db_connection.execute_query(select_staging_table_schema_query)
        return target_schema_rows

    def delete_duplicate_rows_from_table(self, table_name: str, staging_table_name: str, pk_condition: str):
        delete_duplicates_query: str = f"""
                        DELETE FROM {self.schema}.{table_name}
                        WHERE EXISTS (
                            SELECT 1
                            FROM {self.schema}.{staging_table_name}
                            WHERE {pk_condition}
                        );
                    """

        self.db_connection.execute_query(delete_duplicates_query)

    def insert_into_staging_table(self, staging_table_name: str, raw_data_table_name: str,
                                  select_expressions_sql_str: str, insert_columns_sql_str: str):
        insert_staging_table_query: str = f"""
                                        INSERT INTO {self.schema}.{staging_table_name} ({insert_columns_sql_str})
                                        SELECT {select_expressions_sql_str}
                                        FROM {self.schema}.{raw_data_table_name} AS raw;
                                    """

        self.db_connection.execute_query(insert_staging_table_query)

    def insert_into_target_table(self, table_name: str, **kwargs):
        insert_query: str = f"""
                        INSERT INTO {self.schema}.{table_name}
                        SELECT * FROM {self.schema}.{kwargs['staging_table_name']};
                    """

        self.db_connection.execute_query(query=insert_query)

    def process_delete(self, row: pd.Series, starting_directory: str):
        raw_table = row["extract"].split(".")[1]
        related_file = row["file"]
        if self.convert_to_parquet:
            related_file = related_file.replace(".csv", ".parquet")
        object_path = f"{self.object_storage_root}/{starting_directory}/{related_file}"
        table_name = update_table_name_that_starts_with_digit(raw_table.replace('_deletes', ''))

        if raw_table != "metadata_deletes":
            columns_def = ''
            column_names_concat = ''
            column_names = ''
            if table_name == 'picklist__sys':
                column_names_concat += 'object + object_field + picklist_value_name'
                columns_def += 'object VARCHAR(255), object_field VARCHAR(255), picklist_value_name VARCHAR(255)'
                column_names += 'object, object_field, picklist_value_name'
            elif table_name == 'metadata':
                column_names_concat += 'extract + column_name'
                columns_def += 'extract VARCHAR(255), column_name VARCHAR(255)'
                column_names += 'extract, column_name'
            else:
                column_names_concat += 'id'
                columns_def += 'id VARCHAR(255)'
                column_names += 'id'

            raw_data_table_name: str = f"{table_name}_deletes_raw"
            staging_table_name: str = f"{table_name}_deletes_staging"

            create_raw_data_table_query: str = f"CREATE TABLE {self.schema}.{raw_data_table_name} ({columns_def}, deleted_date VARCHAR(255))"
            self.db_connection.execute_query(create_raw_data_table_query)

            file_type = 'PARQUET' if self.convert_to_parquet else 'CSV'
            with_statement: str = \
                f"""WITH (
                        FILE_TYPE = '{file_type}'
                    );
                """ if self.convert_to_parquet else f"""
                WITH (
                    FILE_TYPE = '{file_type}',
                    FIRSTROW = 2
                );
            """

            copy_into_raw_data_table_query: str = f"""
            COPY INTO {self.schema}.{raw_data_table_name}
            FROM '{object_path}'
            {with_statement}
            """
            self.db_connection.execute_query(copy_into_raw_data_table_query)

            create_staging_table_query: str = f"CREATE TABLE {self.schema}.{staging_table_name} ({columns_def}, deleted_date DATETIME2(3))"
            self.db_connection.execute_query(create_staging_table_query)

            insert_staging_table_query: str = f"""
            INSERT INTO {self.schema}.{staging_table_name} ({column_names}, deleted_date)
            SELECT {column_names}, TRY_CAST(NULLIF(TRIM(raw.[deleted_date]), '') AS DATETIME2)      AS [deleted_date]
            FROM {self.schema}.{raw_data_table_name} AS raw;
            """
            self.db_connection.execute_query(insert_staging_table_query)

            delete_query: str = f"""DELETE FROM {self.schema}.{table_name}
                            WHERE {column_names_concat} IN (SELECT {column_names_concat} FROM {self.schema}.{staging_table_name});
                            """
            self.db_connection.execute_query(delete_query)

            drop_raw_data_table_query: str = f"DROP TABLE IF EXISTS {self.schema}.{raw_data_table_name};"
            self.db_connection.execute_query(drop_raw_data_table_query)

            # Drop the staging table after processing
            drop_staging_table_query: str = f"DROP TABLE IF EXISTS {self.schema}.{staging_table_name};"
            self.db_connection.execute_query(drop_staging_table_query)

    def load_full_or_log_data(self, table_name: str, object_path: str, headers: list = None):
        # Raw data Table -> Staging Table -> Target Table is used because the 'Z' in dateTime fields from
        # Vault are not recognized by SQL Server. They need to be staged and cleaned first.
        raw_data_table_name: str = f"{table_name}_raw"
        staging_table_name: str = f"{table_name}_staging"
        self.create_raw_data_table(raw_data_table_name=raw_data_table_name,
                                   headers=headers)

        self.copy_into_raw_data_table(raw_data_table_name=raw_data_table_name,
                                      object_path=object_path)

        self.create_staging_table(staging_table_name=staging_table_name,
                                  table_name=table_name)

        target_schema_rows: list = self.get_staging_table_schema_rows(staging_table_name=staging_table_name)
        target_columns_info = []
        for row in target_schema_rows:
            target_columns_info.append({
                "name": row[0],
                "system_type_name": row[1],
                "is_nullable": row[2],
                "column_ordinal": row[3]
            })

        # 1. Create the SELECT expressions for the COPY INTO clause
        select_expressions_sql_str: str = self.create_sql_str_select_statement(target_columns_info=target_columns_info)

        # 2. Create the list of target columns for the INSERT INTO clause
        target_column_names_for_insert_sql = [f"[{info['name']}]" for info in target_columns_info]
        insert_columns_sql_str = ",\n                    ".join(target_column_names_for_insert_sql)

        # 3. Assemble the complete INSERT INTO ... SELECT ... query
        self.insert_into_staging_table(staging_table_name=staging_table_name,
                                       raw_data_table_name=raw_data_table_name,
                                       select_expressions_sql_str=select_expressions_sql_str,
                                       insert_columns_sql_str=insert_columns_sql_str)

        self.insert_into_target_table(table_name=table_name,
                                      staging_table_name=staging_table_name)

        self.drop_table(table_name=staging_table_name)
        self.drop_table(table_name=raw_data_table_name)

    def load_incremental_data(self, table_name: str, object_path: str, headers: str = None):
        if table_name == 'metadata':
            primary_keys = ["extract", "column_name"]
        elif table_name == 'picklist__sys':
            primary_keys = ["object", "object_field", "picklist_value_name"]
        else:
            primary_keys = ["id"]

        raw_data_table_name: str = f"{table_name}_csv"
        staging_table_name: str = f"{table_name}_staging"
        pk_condition: str = " AND ".join(
            [f"{self.schema}.{table_name}.{col} = {staging_table_name}.{col}" for col in primary_keys])

        # Create raw data table and load all data as Strings
        self.create_raw_data_table(raw_data_table_name=raw_data_table_name, headers=headers)
        self.copy_into_raw_data_table(raw_data_table_name=raw_data_table_name, object_path=object_path)

        # Create staging table, then transform and load data from CSV table to staging table
        self.create_staging_table(staging_table_name=staging_table_name, table_name=table_name)

        # Get Schema of the staging table
        target_schema_rows: list = self.get_staging_table_schema_rows(staging_table_name=staging_table_name)

        target_columns_info = []
        for row in target_schema_rows:
            target_columns_info.append({
                "name": row[0],
                "system_type_name": row[1],
                "is_nullable": row[2],
                "column_ordinal": row[3]
            })

        # 1. Create the SELECT expressions for the INSERT INTO clause
        select_expressions_sql_str: str = self.create_sql_str_select_statement(target_columns_info=target_columns_info)

        # 2. Create the list of target columns for the INSERT INTO clause
        target_column_names_for_insert_sql = [f"[{info['name']}]" for info in target_columns_info]
        insert_columns_sql_str = ",\n                    ".join(target_column_names_for_insert_sql)

        # 3. Assemble the complete INSERT INTO ... SELECT ... query
        self.insert_into_staging_table(staging_table_name=staging_table_name,
                                       raw_data_table_name=raw_data_table_name,
                                       select_expressions_sql_str=select_expressions_sql_str,
                                       insert_columns_sql_str=insert_columns_sql_str)

        self.delete_duplicate_rows_from_table(table_name=table_name,
                                              staging_table_name=staging_table_name,
                                              pk_condition=pk_condition)

        self.insert_into_target_table(table_name=table_name,
                                      staging_table_name=staging_table_name)

        self.drop_table(table_name=staging_table_name)
        self.drop_table(table_name=raw_data_table_name)

