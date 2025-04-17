from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from pandas import DataFrame

from accelerators.redshift.connections.redshift_connection import RedshiftConnection
from common.services.database_service import DatabaseService
from common.utilities import log_message, update_table_name_that_starts_with_digit


class RedshiftService(DatabaseService):
    def __init__(self, parameters: dict):
        super().__init__(parameters)
        self.host = parameters['host']
        self.database = parameters['database']
        self.schema = parameters['schema']
        self.user = parameters['user']
        self.password = parameters['password']
        self.port = parameters['port']
        self.iam_role = parameters['iam_redshift_s3_read']
        self.db_connection: RedshiftConnection = self.get_connection()

    def get_connection(self) -> RedshiftConnection:
        return RedshiftConnection(
            database=self.database,
            hostname=self.host,
            port_number=self.port,
            username=self.user,
            user_password=self.password
        )

    @staticmethod
    def create_sql_str(table_df: DataFrame, is_picklist: bool, is_modify: bool, is_add: bool) -> str:
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
            length = 64000

            if data_type == "id" or (column_name == 'id' and data_type == 'string'):
                column_def = f'"{column_name}" VARCHAR({length})'
            elif data_type in ["datetime", "timestamp with time zone"]:
                column_def = f'"{column_name}" TIMESTAMP'
            elif data_type == "boolean":
                column_def = f'"{column_name}" BOOLEAN'
            elif data_type in ["number", "numeric"]:
                column_def = f'"{column_name}" NUMERIC'
            else:
                column_def = f'"{column_name}" VARCHAR({length})'

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
                            message=f'{self.schema} exists.',
                            context=None)
            else:
                log_message(log_level='Info',
                            message=f'{self.schema} does not exist. Creating new schema',
                            context=None)
                create_schema_query = f"""
                            CREATE SCHEMA {self.schema};
                        """
                self.db_connection.execute_query(create_schema_query)

        except Exception as e:
            log_message(log_level='Error',
                        message=f'Error checking if schema {self.schema} exists',
                        exception=e,
                        context=None)
            raise e

    def check_if_table_exists(self, table: str):
        query = f"""
            SELECT COUNT(*)
            FROM pg_tables
            WHERE tablename = '{table.lower()}'
            AND schemaname = '{self.schema.lower()}';
        """

        query_result = self.db_connection.execute_query(query)

        return query_result[0][0] > 0

    def retrieve_column_info(self, table_name: str):
        existing_columns_query = f"""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE
            table_catalog = '{self.database}' 
            AND table_schema = '{self.schema}'
            AND table_name = '{table_name}'
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
        is_picklist = False

        if table_name == "picklist":
            is_picklist = True
        sql_string = self.create_sql_str(filtered_metadata, is_picklist=is_picklist, is_modify=False, is_add=False)

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

    def delete_data_from_table(self, s3_bucket_name: str, starting_directory: str, manifest_table: pd.DataFrame):
        deletes_filter = manifest_table[(manifest_table["type"] == "deletes") & (manifest_table["records"] > 0)]

        for index, row in deletes_filter.iterrows():
            raw_table = row["extract"].split(".")[1]
            related_file = row["file"]
            s3_file_uri = f"s3://{s3_bucket_name}/{starting_directory}/{related_file}"
            table_name = update_table_name_that_starts_with_digit(raw_table.replace('_deletes', ''))
            if raw_table != "metadata_deletes":
                columns = ''
                column_names = ''
                if table_name == 'picklist__sys':
                    column_names += 'object || object_field || picklist_value_name'
                    columns += 'object VARCHAR(255), object_field VARCHAR(255), picklist_value_name VARCHAR(255)'
                elif table_name == 'metadata':
                    column_names += 'extract || column_name'
                    columns += 'extract VARCHAR(255), column_name VARCHAR(255)'
                else:
                    column_names += 'id'
                    columns += 'id VARCHAR(255)'

                create_query = f"CREATE TEMPORARY TABLE temp_{table_name}_deletes ({columns}, deleted_date TIMESTAMPTZ)"


                # Load the data from the CSV file into the temporary view
                self.db_connection.execute_query(create_query)

                copy_query = f"""
                    COPY temp_{table_name}_deletes FROM '{s3_file_uri}'
                    IAM_ROLE '{self.iam_role}'
                    FORMAT AS CSV 
                    QUOTE '\"'
                    IGNOREHEADER 1
                    TIMEFORMAT 'auto';
                    """

                self.db_connection.execute_query(copy_query)

                # Delete the matching rows from the target table
                delete_query = (f"DELETE FROM {self.database}.{self.schema}.{table_name} "
                                f"WHERE {column_names} IN (SELECT {column_names} FROM temp_{table_name}_deletes);")


                self.db_connection.execute_query(delete_query)




