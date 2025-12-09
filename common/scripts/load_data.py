import os
import sys

import pandas as pd
import pyarrow as pa

from common.services.database_service import DatabaseService
from common.services.object_storage_service import ObjectStorageService
from common.utilities import log_message
from common.utilities import update_table_name_that_starts_with_digit
from common.utilities import convert_file_to_table


def handle_metadata_updates(object_storage_service: ObjectStorageService,
                            database_service: DatabaseService,
                            metadata_updates: pd.DataFrame):
    log_message(log_level='Info',
                message=f'Handling metadata updates')

    starting_directory: str = f"{object_storage_service.direct_data_folder}/{object_storage_service.extract_folder}"

    file_extension: str = '.csv'
    if database_service.convert_to_parquet:
        file_extension = '.parquet'
    metadata_updates_file_path: str = f"{starting_directory}/{os.path.splitext(metadata_updates['file'].iloc[0])[0]}{file_extension}"

    # Check if the file exists in Object Storage
    object_storage_service.check_if_object_exists(object_path=metadata_updates_file_path)

    # Download the file from Object Storage to local
    object_storage_service.download_object_to_local(object_path=metadata_updates_file_path,
                                                    output_path=metadata_updates_file_path)

    metadata_updates_table: pd.DataFrame | pa.Table = convert_file_to_table(file_path=metadata_updates_file_path,
                                                                            convert_to_parquet=database_service.convert_to_parquet)

    unique_extract_values = metadata_updates_table["extract"].unique()
    for extract in unique_extract_values:
        table_name: str = update_table_name_that_starts_with_digit(extract.split(".")[1])
        filtered_metadata = metadata_updates_table[metadata_updates_table["extract"] == extract]

        # Get column names for this extract
        columns_df = metadata_updates_table.loc[
            metadata_updates_table["extract"] == extract, ["column_name", "type", "length"]]

        if "id" in columns_df["column_name"].tolist():
            database_service.create_single_table(table_name=table_name,
                                                 filtered_metadata=filtered_metadata)
        else:
            existing_columns = database_service.retrieve_column_info(table_name=table_name)
            columns_to_add: list = []
            columns_to_modify: list = []

            for _, row in columns_df.iterrows():
                column_name = row["column_name"]
                if column_name not in existing_columns.keys():
                    columns_to_add.append(row)
                else:
                    new_column_type = row["type"].lower()
                    if new_column_type != "string":
                        columns_to_modify.append(row[['column_name', 'type', 'length']].to_dict())

            if len(columns_to_add) > 0:
                database_service.add_columns_to_table(columns_to_add=pd.DataFrame(columns_to_add),
                                                      table_name=table_name)

            if len(columns_to_modify) > 0:
                modify_column_statement = database_service.create_sql_str_column_definitions(
                    pd.DataFrame(columns_to_modify), False, is_modify=True, is_add=False)
                database_service.db_connection.execute_query(f"""
                            ALTER TABLE {database_service.schema}.{table_name} {modify_column_statement}
                        """)


def handle_metadata_deletes(object_storage_service: ObjectStorageService,
                            database_service: DatabaseService,
                            metadata_deletes: pd.DataFrame):
    log_message(log_level='Info',
                message=f'Handling metadata deletes')

    starting_directory: str = f"{object_storage_service.direct_data_folder}/{object_storage_service.extract_folder}"

    file_extension: str = '.csv'
    if database_service.convert_to_parquet:
        file_extension = '.parquet'
    metadata_deletes_file_path: str = f"{starting_directory}/{os.path.splitext(metadata_deletes['file'].iloc[0])[0]}{file_extension}"

    # Check if the file exists in Object Storage
    object_storage_service.check_if_object_exists(object_path=metadata_deletes_file_path)

    # Download the file from Object Storage to local
    object_storage_service.download_object_to_local(object_path=metadata_deletes_file_path,
                                                    output_path=metadata_deletes_file_path)

    metadata_deletes_table: pd.DataFrame | pa.Table = convert_file_to_table(file_path=metadata_deletes_file_path,
                                                                            convert_to_parquet=database_service.convert_to_parquet)

    unique_extract_values = metadata_deletes_table["extract"].unique()

    for extract in unique_extract_values:
        table_name: str = extract.split(".")[1]

        # Get column names for this extract
        columns: list = metadata_deletes_table.loc[metadata_deletes_table["extract"] == extract, "column_name"].tolist()

        # Check if 'id' exists in the columns list
        if "id" in columns:
            # Drop the entire table if 'id' is a column
            database_service.drop_table(table_name=table_name)
        elif columns:
            database_service.drop_columns_from_table(table_name=table_name, columns=columns)


def handle_metadata_changes(object_storage_service: ObjectStorageService,
                            database_service: DatabaseService,
                            manifest_table: pd.DataFrame):
    log_message(log_level='Info',
                message=f'Handling metadata changes')

    metadata_filter: pd.DataFrame = manifest_table[manifest_table["extract"] == "Metadata.metadata"]
    metadata_deletes: pd.DataFrame = metadata_filter[metadata_filter["type"] == "deletes"]
    metadata_updates: pd.DataFrame = metadata_filter[metadata_filter["type"] == "updates"]

    # Handle deletes if any
    if not metadata_deletes.empty and int(metadata_deletes['records'].iloc[0]) > 0:
        handle_metadata_deletes(object_storage_service=object_storage_service,
                                database_service=database_service,
                                metadata_deletes=metadata_deletes)

    # Handle updates if any
    if not metadata_updates.empty and int(metadata_updates['records'].iloc[0]) > 0:
        handle_metadata_updates(object_storage_service=object_storage_service,
                                database_service=database_service,
                                metadata_updates=metadata_updates)


def process_manifest_row(database_service: DatabaseService,
                         object_storage_service: ObjectStorageService,
                         row: pd.Series,
                         extract_type: str):
    raw_table_name: str = row["extract"].split(".")[1]
    table_name: str = update_table_name_that_starts_with_digit(raw_table_name)
    filename: str = row['file']
    if database_service.convert_to_parquet:
        filename = filename.replace(".csv", ".parquet")

    if not (extract_type == "incremental" and "metadata" in table_name):
        load_data_into_tables(database_service=database_service,
                              object_storage_service=object_storage_service,
                              extract_type=extract_type,
                              table_name=table_name,
                              filename=filename)


def load_data_into_tables(database_service: DatabaseService,
                          object_storage_service: ObjectStorageService,
                          extract_type: str,
                          table_name: str,
                          filename: str):
    full_object_path: str = object_storage_service.get_full_object_path(filename=filename)
    relative_object_path: str = object_storage_service.get_relative_object_path(filename=filename)
    headers: list[str] | None = None
    if database_service.convert_to_parquet:
        headers = object_storage_service.get_headers_from_parquet_file(object_path=relative_object_path)
    else:
        headers = object_storage_service.get_headers_from_csv_file(object_path=relative_object_path)

    if extract_type in ["full", "log"]:
        database_service.load_full_or_log_data(table_name=table_name,
                                               object_path=full_object_path,
                                               headers=headers)
    if extract_type == "incremental":
        database_service.load_incremental_data(table_name=table_name,
                                               object_path=full_object_path,
                                               headers=headers)


def run(object_storage_service: ObjectStorageService, database_service: DatabaseService, direct_data_params: dict):
    log_message(log_level='Info',
                message=f'---Executing load_data.py---')
    try:
        starting_directory: str = f"{object_storage_service.direct_data_folder}/{object_storage_service.extract_folder}"
        extract_type: str = direct_data_params['extract_type']

        database_service.db_connection.activate_cursor()
        database_service.check_if_schema_exists()

        file_extension: str = '.csv'
        if database_service.convert_to_parquet:
            file_extension = '.parquet'

        # Retrieve the Manifest File from Object Storage
        manifest_filepath: str = f"{starting_directory}/manifest{file_extension}"
        object_storage_service.check_if_object_exists(object_path=manifest_filepath)
        object_storage_service.download_object_to_local(object_path=manifest_filepath, output_path=manifest_filepath)

        # Convert the manifest file to a table
        manifest_table: pd.DataFrame | pa.Table = convert_file_to_table(file_path=manifest_filepath,
                                                                        convert_to_parquet=database_service.convert_to_parquet)

        if extract_type in ["full", "log"]:
            # Retrieve the Metadata File from Object Storage
            metadata_filepath: str = (
                f"{starting_directory}/Metadata/metadata{file_extension}"
                if extract_type == "full"
                else f"{starting_directory}/metadata_full{file_extension}"
            )
            object_storage_service.check_if_object_exists(object_path=metadata_filepath)
            object_storage_service.download_object_to_local(metadata_filepath, metadata_filepath)

            # Convert the metadata file to a table
            metadata_table: pd.DataFrame | pa.Table = convert_file_to_table(file_path=metadata_filepath,
                                                                            convert_to_parquet=database_service.convert_to_parquet)

            # Create all tables in the database
            database_service.create_all_tables(starting_directory=starting_directory, metadata_table=metadata_table)

        elif extract_type == "incremental":

            handle_metadata_changes(object_storage_service=object_storage_service,
                                    database_service=database_service,
                                    manifest_table=manifest_table)
            database_service.delete_data_from_table(starting_directory=starting_directory,
                                                    manifest_table=manifest_table)

        manifest_filtered_table = manifest_table[
            (manifest_table["type"] == "updates") & (manifest_table["records"] > 0)]

        for _, row in manifest_filtered_table.iterrows():
            process_manifest_row(database_service=database_service,
                                 object_storage_service=object_storage_service,
                                 row=row,
                                 extract_type=extract_type)

        database_service.db_connection.close_cursor()
        database_service.db_connection.close()

    except Exception as e:
        log_message(log_level='Error',
                    message=f'Errors encountered when loading data.',
                    exception=e)

