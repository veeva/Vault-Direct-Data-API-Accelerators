import pandas as pd
from pandas import DataFrame

from accelerators.snowflake.connections.snowflake_connection import SnowflakeConnection
from common.services.database_service import DatabaseService
from common.utilities import log_message, update_table_name_that_starts_with_digit


class SnowflakeService(DatabaseService):
    def __init__(self, parameters: dict):
        super().__init__(parameters)
        self.account: str = parameters['account']
        self.database: str = parameters['database']
        self.warehouse: str = parameters['warehouse']
        self.username: str = parameters['username']
        self.role: str = parameters['role']
        self.private_key: str = parameters['private_key']
        self.private_key_passphrase: str = parameters['private_key_passphrase']
        self.stage_name: str = parameters['stage_name'].upper()
        self.infer_schema: bool = parameters['infer_schema']
        self.db_connection: SnowflakeConnection = self.get_connection()

    def get_connection(self) -> SnowflakeConnection:
        return SnowflakeConnection(
            database=self.database,
            account=self.account,
            warehouse=self.warehouse,
            schema=self.schema,
            stage_name=self.stage_name,
            username=self.username,
            role=self.role,
            private_key=self.private_key,
            private_key_passphrase=self.private_key_passphrase
        )

    @staticmethod
    def create_sql_str(table_df: DataFrame, **kwargs) -> str:
        """
        Generates a SQL string for creating table columns in Snowflake.

        :param table_df: A DataFrame containing column details (column_name, type, length).
        :return: A partial SQL string defining the table columns.
        """
        is_picklist: bool = kwargs.get('is_picklist', False)
        is_modify: bool = kwargs.get('is_modify', False)
        is_add: bool = kwargs.get('is_add', False)
        column_definitions: list = []

        for _, row in table_df.iterrows():
            column_name: str = row['column_name'].lower()
            data_type: str = row['type'].lower()

            if data_type == "id" or (column_name == 'id' and data_type == 'string'):
                column_def: str = f'"{column_name}" VARCHAR()'
            elif data_type in ["datetime", "timestamp with time zone"]:
                column_def: str = f'"{column_name}" TIMESTAMP_TZ'
            elif data_type == "boolean":
                column_def: str = f'"{column_name}" BOOLEAN'
            elif data_type in ["number", "numeric"]:
                column_def: str = f'"{column_name}" NUMERIC'
            elif data_type == "date":
                column_def: str = f'"{column_name}" DATE'
            else:
                column_def: str = f'"{column_name}" VARCHAR()'

            column_definitions.append(column_def)

        if is_modify and not is_add:
            return "MODIFY COLUMN " + ", MODIFY COLUMN ".join(column_definitions)
        elif is_add and not is_modify:
            return "ADD COLUMN " + ", ".join(column_definitions)
        elif is_modify and is_add:
            raise Exception("Cannot modify and add columns at once")
        else:
            if is_picklist:
                column_definitions.append(
                    'CONSTRAINT picklist_primary_key PRIMARY KEY (object, object_field, picklist_value_name)'
                )
            return ", ".join(column_definitions)

    def check_if_table_exists(self, table: str):
        query = f"""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = '{table.lower()}' 
        AND TABLE_SCHEMA = '{self.schema.lower()}';
        """

        query_result = self.db_connection.execute_query(query)

        return query_result[0][0] > 0

    def retrieve_column_info(self, table_name: str):
        existing_columns_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{self.schema}' 
        AND TABLE_NAME = '{table_name.upper()}'
        """
        sql_result = self.db_connection.execute_query(existing_columns_query)
        columns = ['COLUMN_NAME', 'DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH']
        existing_columns_df = pd.DataFrame(sql_result, columns=columns)

        # Convert to dictionary for quick lookup
        return {
            row['COLUMN_NAME'].lower(): {
                "DATA_TYPE": row["DATA_TYPE"].lower(),
                "CHARACTER_MAXIMUM_LENGTH": row["CHARACTER_MAXIMUM_LENGTH"]
            }
            for _, row in existing_columns_df.iterrows()
        }

    def create_all_tables(self, starting_directory: str, metadata_table: pd.DataFrame):

        unique_extract_values = metadata_table["extract"].unique()

        def create_table(extract: str):
            filtered_metadata = metadata_table[metadata_table["extract"] == extract]

            if extract == "Document.document_version__sys":
                filtered_metadata.loc[
                    (filtered_metadata['column_name'] == 'document_number__v'),
                    ['type', 'length']
                ] = ['String', 255]

            if extract == "Object.security_policy__sys":
                filtered_metadata.loc[
                    (filtered_metadata['column_name'] == 'description__sys'),
                    ['type', 'length']
                ] = ['String', 255]

            new_table_name = update_table_name_that_starts_with_digit(extract.split(".")[1])
            self.create_single_table(table_name=new_table_name, filtered_metadata=filtered_metadata)

        for extract in unique_extract_values:
            create_table(extract=extract)

        # Create metadata table
        column_definitions = {col: ["STRING"] for col in metadata_table.columns}
        new_metadata_df = pd.DataFrame.from_dict(column_definitions).astype(str)
        new_metadata_df.loc["length"] = 1000
        new_metadata_df = new_metadata_df.T.reset_index()
        new_metadata_df.columns = ["column_name", "type", "length"]

        self.create_single_table(table_name="metadata", filtered_metadata=new_metadata_df)

    def create_single_table(self, table_name: str, filtered_metadata: pd.DataFrame):
        is_picklist: bool = False

        if table_name == "picklist":
            is_picklist = True
        sql_string: str = self.create_sql_str(filtered_metadata, is_picklist=is_picklist, is_modify=False,
                                              is_add=False)

        self.db_connection.execute_query(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{table_name} ({sql_string})
            """)

    def drop_tables_in_schema(self, tables: list | tuple):
        for (table_name,) in tables:
            drop_query: str = f'DROP TABLE "{table_name}";'
            log_message(log_level='Info',
                        message=f"Executing: {drop_query}",
                        context=None)
            self.db_connection.execute_query(drop_query)

        log_message(log_level='Info',
                    message=f"Tables dropped successfully",
                    context=None)

    def delete_data_from_table(self, starting_directory: str, file_format: str, file_format_name: str,
                               manifest_table: pd.DataFrame):
        deletes_filter = manifest_table[(manifest_table["type"] == "deletes") & (manifest_table["records"] > 0)]

        for index, row in deletes_filter.iterrows():
            self.process_delete(row=row,
                                starting_directory=starting_directory,
                                file_format=file_format,
                                file_format_name=file_format_name)

    def check_if_stage_exists(self):
        """
        Checks if the Snowflake stage exists, and if not, creates it.
        """

        log_message(log_level='Info',
                    message=f'Checking if stage exists')

        # Get all existing stages in the schema
        staging_query_response = self.db_connection.execute_query(
            f"""SHOW STAGES IN SCHEMA {self.schema};"""
        )

        # Extract stage names from response
        existing_stages = {row[1].upper() for row in staging_query_response}  # Adjust index if needed
        log_message(log_level='Info',
                    message=f'Existing stages: {existing_stages}')
        if self.stage_name not in existing_stages:
            log_message(log_level='Info',
                        message=f'Stage {self.stage_name} does not exist. Creating it...')
            self.db_connection.execute_query(f"""
                CREATE STAGE {self.stage_name}
                STORAGE_INTEGRATION = direct_data_api_integration
                URL = 's3://{self.stage_name}/'
                FILE_FORMAT = (TYPE = 'PARQUET')
            """)
        else:
            log_message(log_level='Info',
                        message=f'Stage {self.stage_name} already exists.')

    def create_table_from_file_format(self, table_name: str, file_name: str, file_format_name: str):
        log_message(log_level='Debug',
                    message=f'Creating table {table_name} from staged file {file_name}...')

        # Use INFER_SCHEMA to create the table
        try:
            self.db_connection.execute_query(f"""
                CREATE OR REPLACE TABLE {table_name}
                USING TEMPLATE (
                    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                    FROM TABLE(INFER_SCHEMA(
                        LOCATION => '@{self.stage_name}/{file_name}',
                        FILE_FORMAT => '{file_format_name.lower()}'
                    ))
                );
                """)
            log_message(log_level='Info',
                        message=f'Table {table_name} created successfully.')
        except Exception as e:
            print(f"Failed to create table {table_name}: {str(e)}")
            raise e

    def load_data_into_tables(self,
                              extract_type: str,
                              table_name: str,
                              file: str,
                              file_format_name: str,
                              s3_stage_uri: str,
                              infer_schema: bool,
                              match_by_column: str):
        if extract_type in ["full", "log"]:
            if infer_schema:
                # Create the table dynamically
                self.create_table_from_file_format(table_name, file, file_format_name)

            # Load data into the table
            self.db_connection.execute_query(f"""
                    COPY INTO {table_name}
                    FROM @{s3_stage_uri}
                    FILE_FORMAT = {file_format_name}
                    {match_by_column}
                """)
        else:
            temp_table_name = f"{table_name}_temp".upper()
            temp_table_create_query = f"""
                    CREATE OR REPLACE TEMPORARY TABLE {temp_table_name} AS
                    SELECT * FROM {table_name} LIMIT 0;
                """
            self.db_connection.execute_query(temp_table_create_query)

            load_data_query = f"""
                    COPY INTO {temp_table_name}
                    FROM @{s3_stage_uri}
                    FILE_FORMAT = {file_format_name}
                    {match_by_column};
                """
            self.db_connection.execute_query(load_data_query)

            table_column_response = self.retrieve_column_info(table_name)
            temp_table_column_response = self.retrieve_column_info(temp_table_name)
            table_columns = list(table_column_response.keys())
            temp_table_columns = [col for col in table_columns if col in temp_table_column_response.keys()]

            set_clause = ', '.join([f'target."{col}" = source."{col}"' for col in table_columns])
            insert_columns = ', '.join([f'"{col}"' for col in table_columns])
            insert_values = ', '.join([f'source."{col}"' for col in temp_table_columns])

            if table_name == 'picklist__sys':
                matching_statement = 'target."object" = source."object" \nAND target."object_field" = source."object_field" \nAND target."picklist_value_name" = source."picklist_value_name"'
            elif table_name == 'metadata':
                matching_statement = 'target."extract" = source."extract" \nAND target."column_name" = source."column_name"'
            else:
                matching_statement = 'target."id" = source."id"'

            merge_data_query = f"""
                    MERGE INTO {table_name} AS target
                    USING {temp_table_name} AS source
                    ON {matching_statement}
                    WHEN MATCHED THEN
                        UPDATE SET 
                            {set_clause}
                    WHEN NOT MATCHED THEN
                        INSERT ({insert_columns}) VALUES ({insert_values});
                """
            self.db_connection.execute_query(merge_data_query)

    def process_delete(self, row: pd.Series, starting_directory: str, file_format: str, file_format_name: str):
        raw_table = row["extract"].split(".")[1].lower()
        related_file = row["file"]
        if file_format == ".parquet":
            related_file = related_file.replace(".csv", ".parquet")
        table_name = update_table_name_that_starts_with_digit(raw_table.replace('_deletes', ''))

        if raw_table != "metadata_deletes":
            columns = ''
            column_names = ''
            if table_name == 'picklist__sys':
                column_names += '("object" || "object_field" || "picklist_value_name")'
                columns += '"object" VARCHAR, "object_field" VARCHAR, "picklist_value_name" VARCHAR'
            elif table_name == "metadata":
                column_names += '("extract" || "column_name")'
                columns += '"extract" VARCHAR, "column_name" VARCHAR'
            else:
                column_names += '"id"'
                columns += '"id" VARCHAR'

            temp_table_name = f"temp_{table_name}_deletes".upper()

            create_query = f"""
            CREATE OR REPLACE TEMPORARY TABLE {temp_table_name} ({columns}, deleted_date TIMESTAMP_NTZ);
            """

            # Load the data from the CSV file into the temporary view
            self.db_connection.execute_query(create_query)

            load_query = f"""
                COPY INTO {temp_table_name}
                FROM @{self.stage_name}/{starting_directory}/{related_file}
                FILE_FORMAT = {file_format_name}
            """
            if file_format == ".parquet":
                load_query += "\nMATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"

            load_query += ";"

            self.db_connection.execute_query(load_query)

            # Delete the matching rows from the target table
            delete_query = f"""
                DELETE FROM {table_name.upper()}
                WHERE {column_names} IN (SELECT {column_names} FROM {temp_table_name});
            """

            self.db_connection.execute_query(delete_query)

    def process_manifest_row(self,
                             row: pd.Series,
                             starting_directory: str,
                             file_extension: str,
                             file_format_name: str,
                             extract_type: str,
                             infer_schema: bool,
                             match_by_column: str):
        raw_table_name: str = row["extract"].split(".")[1]
        table_name: str = update_table_name_that_starts_with_digit(raw_table_name)
        file: str = row['file']

        if file_extension == ".parquet":
            file = file.replace(".csv", ".parquet")

        s3_stage_uri: str = f"{self.stage_name}/{starting_directory}/{file}"

        if not (extract_type == "incremental" and "metadata" in table_name):
            self.load_data_into_tables(extract_type=extract_type,
                                       table_name=table_name,
                                       file=file,
                                       file_format_name=file_format_name,
                                       s3_stage_uri=s3_stage_uri,
                                       infer_schema=infer_schema,
                                       match_by_column=match_by_column)
