import gzip
import os
import sys
import tarfile
from io import BytesIO
from sqlite3 import Connection
from typing import Dict, Any
import sqlite3
import pandas as pd
import pyarrow as pa

from accelerators.sqlite.services.sqlite_service import SqliteService
from common.api.model.response.direct_data_response import DirectDataResponse
from common.api.model.response.vault_response import VaultResponse
from common.services.vault_service import VaultService

from common.utilities import log_message, convert_file_to_table, update_table_name_that_starts_with_digit

sys.path.append('.')

def handle_metadata_updates(local_params: dict,
                            sqlite_service: SqliteService,
                            metadata_updates: pd.DataFrame):
    log_message(log_level='Info',
                message=f'Handling metadata updates')

    starting_directory: str = f"{local_params['direct_data_folder']}/{local_params['extract_folder']}"

    metadata_updates_file_path: str = f"{starting_directory}/{os.path.splitext(metadata_updates['file'].iloc[0])[0]}.csv"

    metadata_updates_table: pd.DataFrame | pa.Table = convert_file_to_table(file_path=metadata_updates_file_path,
                                                                            convert_to_parquet=sqlite_service.convert_to_parquet)

    unique_extract_values = metadata_updates_table["extract"].unique()
    for extract in unique_extract_values:
        table_name: str = update_table_name_that_starts_with_digit(extract.split(".")[1])
        filtered_metadata = metadata_updates_table[metadata_updates_table["extract"] == extract]

        # Get column names for this extract
        columns_df = metadata_updates_table.loc[
            metadata_updates_table["extract"] == extract, ["column_name", "type", "length"]]

        if "id" in columns_df["column_name"].tolist():
            sqlite_service.create_single_table(table_name=table_name,
                                               filtered_metadata=filtered_metadata)
        else:
            existing_columns = sqlite_service.retrieve_column_info(table_name=table_name)
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
                sqlite_service.add_columns_to_table(columns_to_add=pd.DataFrame(columns_to_add),
                                                    table_name=table_name)

            if len(columns_to_modify) > 0:
                modify_column_statement = sqlite_service.create_sql_str_column_definitions(
                    pd.DataFrame(columns_to_modify), False, is_modify=True, is_add=False)
                sqlite_service.db_connection.execute_query(f"""
                            ALTER TABLE {sqlite_service.schema}.{table_name} {modify_column_statement}
                        """)


def handle_metadata_deletes(local_params: dict,
                            sqlite_service: SqliteService,
                            metadata_deletes: pd.DataFrame):
    log_message(log_level='Info',
                message=f'Handling metadata deletes')

    starting_directory: str = f"{local_params['direct_data_folder']}/{local_params['extract_folder']}"

    metadata_deletes_file_path: str = f"{starting_directory}/{os.path.splitext(metadata_deletes['file'].iloc[0])[0]}.csv"

    metadata_deletes_table: pd.DataFrame | pa.Table = convert_file_to_table(file_path=metadata_deletes_file_path,
                                                                            convert_to_parquet=False)

    unique_extract_values = metadata_deletes_table["extract"].unique()
    for extract in unique_extract_values:
        table_name: str = extract.split(".")[1]

        # Get column names for this extract
        columns: list = metadata_deletes_table.loc[metadata_deletes_table["extract"] == extract, "column_name"].tolist()

        # Check if 'id' exists in the columns list
        if "id" in columns:
            # Drop the entire table if 'id' is a column
            sqlite_service.drop_table(table_name=table_name)
        elif columns:
            sqlite_service.drop_columns_from_table(table_name=table_name, columns=columns)


def handle_metadata_changes(local_params: dict,
                            sqlite_service: SqliteService,
                            manifest_table: pd.DataFrame):
    log_message(log_level='Info',
                message=f'Handling metadata changes')

    metadata_filter: pd.DataFrame = manifest_table[manifest_table["extract"] == "Metadata.metadata"]
    metadata_deletes: pd.DataFrame = metadata_filter[metadata_filter["type"] == "deletes"]
    metadata_updates: pd.DataFrame = metadata_filter[metadata_filter["type"] == "updates"]

    # Handle deletes if any
    if not metadata_deletes.empty and int(metadata_deletes['records'].iloc[0]) > 0:
        handle_metadata_deletes(local_params=local_params,
                                sqlite_service=sqlite_service,
                                metadata_deletes=metadata_deletes)

    # Handle updates if any
    if not metadata_updates.empty and int(metadata_updates['records'].iloc[0]) > 0:
        handle_metadata_updates(local_params=local_params,
                                sqlite_service=sqlite_service,
                                metadata_updates=metadata_updates)

def process_manifest_row(sqlite_service: SqliteService,
                         local_params: dict,
                         row: pd.Series,
                         extract_type: str):
    raw_table_name: str = row["extract"].split(".")[1]
    table_name: str = update_table_name_that_starts_with_digit(raw_table_name)
    filename: str = row['file']

    if not (extract_type == "incremental" and "metadata" in table_name):
        load_data_into_tables(sqlite_service=sqlite_service,
                              local_params=local_params,
                              extract_type=extract_type,
                              table_name=table_name,
                              filename=filename)


def load_data_into_tables(sqlite_service: SqliteService,
                          local_params: dict,
                          extract_type: str,
                          table_name: str,
                          filename: str):
    full_object_path: str = f"{local_params['direct_data_folder']}/{local_params['extract_folder']}/{filename}"
    headers: list[str] | None = None

    if extract_type in ["full", "log"]:
        sqlite_service.load_full_or_log_data(table_name=table_name,
                                             object_path=full_object_path,
                                             headers=headers)
    if extract_type == "incremental":
        sqlite_service.load_incremental_data(table_name=table_name,
                                             object_path=full_object_path,
                                             headers=headers)


def run(direct_data_params: Dict[str, Any],
        local_params: Dict[str, Any],
        sqlite_service: SqliteService) -> None:
    """
    This method downloads a .tar.gz file from Direct Data API, unzips it, and loads the data into a SQLite database.
    """
    try:
        log_message(log_level='Info',
                    message=f'---Executing load_data.py---')
        sqlite_service.db_connection.open()
        sqlite_service.db_connection.activate_cursor()
        starting_directory: str = f"{local_params['direct_data_folder']}/{local_params['extract_folder']}"
        extract_type: str = direct_data_params['extract_type']

        sqlite_service.db_connection.activate_cursor()

        manifest_filepath: str = f"{starting_directory}/manifest.csv"
        manifest_table: pd.DataFrame | pa.Table = convert_file_to_table(file_path=manifest_filepath,
                                                                        convert_to_parquet=False)

        if extract_type in ["full", "log"]:
            # Retrieve the Metadata File from Object Storage
            metadata_filepath: str = (
                f"{starting_directory}/Metadata/metadata.csv"
                if extract_type == "full"
                else f"{starting_directory}/metadata_full.csv"
            )

            metadata_table: pd.DataFrame | pa.Table = convert_file_to_table(file_path=metadata_filepath,
                                                                            convert_to_parquet=False)
            sqlite_service.create_all_tables(starting_directory=starting_directory, metadata_table=metadata_table)

        elif extract_type == "incremental":
            handle_metadata_changes(local_params=local_params,
                                    sqlite_service=sqlite_service,
                                    manifest_table=manifest_table)
            sqlite_service.delete_data_from_table(starting_directory=starting_directory,
                                                    manifest_table=manifest_table)

        manifest_filtered_table = manifest_table[
            (manifest_table["type"] == "updates") & (manifest_table["records"] > 0)]

        for _, row in manifest_filtered_table.iterrows():
            process_manifest_row(sqlite_service=sqlite_service,
                                 local_params=local_params,
                                 row=row,
                                 extract_type=extract_type)

    except Exception as e:
        log_message(log_level='Error',
                    message=f'Errors encountered when loading into SQLite',
                    exception=e)
