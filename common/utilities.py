import importlib
import json
import os
import sys
from io import BytesIO
import datetime
import traceback

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def log_message(log_level, message, exception=None, context=None):
    """
    Logs a message with the specified log level.

    :param log_level: The severity level of the log message.
    :param message: The log message to be logged.
    :param exception: An exception object to log the exception details and traceback. Defaults to None.
    :param context: Additional contextual information. Defaults to None.
    """

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{log_level}] {timestamp} - {message}"
    if exception:
        log_entry += f"\nException: {exception}\n{traceback.format_exc()}\n{traceback.print_exc()}"
    if context:
        log_entry += f"\nContext: {context}"
    print(log_entry)

def read_json_file(file_path: str) -> dict:
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError as e:
        log_message(log_level='Error',
                    message=f'Error: File not found at {file_path}.',
                    exception=e)
        return {}
    except json.JSONDecodeError as e:
        log_message(log_level='Error',
                    message=f'Error: Failed to decode JSON',
                    exception=e)
        return {}


def update_table_name_that_starts_with_digit(table_name: str) -> str:
    """
    This method handles reconciling Vault objects that begin with a number and appending a 'n_' so that Redshift will
    accept the naming convention
    :param table_name: The name of the table that needs to be updated
    :return: The updated table name
    """
    if table_name.isdigit():
        return f'n_{table_name}'
    else:
        return table_name

def convert_file_to_table(file_path: str, convert_to_parquet: bool) -> pd.DataFrame | pa.Table:
    """
    Converts a file to a table data structure.

    :param file_path: Path to the file.
    :param convert_to_parquet: If True, the file will be read as a Parquet file; otherwise, it will be read as a CSV.

    :return: A Pandas DataFrame or Pyarrow Table containing the data from the manifest file.
    """

    log_message(log_level='Info',
                message=f'Converting file to table structure: {file_path}')
    try:
        if convert_to_parquet:
            return pq.read_table(source=file_path).to_pandas()
        else:
            return pd.read_csv(filepath_or_buffer=file_path)
    except Exception as e:
        log_message(log_level='Error',
                    message=f'Error converting file to table: {file_path}',
                    exception=e)
        return None