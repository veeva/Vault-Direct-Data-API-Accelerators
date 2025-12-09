import gzip
import os
import sys
import tarfile
from io import BytesIO
from typing import Dict, Any

import pandas as pd
import pyarrow as pa

from accelerators.sqlite.services.sqlite_service import SqliteService
from common.api.model.response.direct_data_response import DirectDataResponse
from common.api.model.response.vault_response import VaultResponse
from common.services.vault_service import VaultService
from common.utilities import log_message, convert_file_to_table, update_table_name_that_starts_with_digit

sys.path.append('.')


def run(local_params: Dict[str, Any]) -> None:
    """
    This script unzips a local direct data file.
    """

    try:
        log_message(log_level='Info',
                    message=f'---Executing unzip_direct_data_file.py---')
        # Create output directory
        archive_filepath: str = local_params['archive_filepath']
        output_directory: str = f"{archive_filepath.split('.')[0]}/"
        if output_directory is None or output_directory == "":
            output_directory = os.path.basename(archive_filepath)[:-7]
        os.makedirs(output_directory, exist_ok=True)

        try:
            # Open the file in binary read mode ('rb')
            with open(archive_filepath, 'rb') as binary_file:
                tarfile_content = binary_file.read()

        except FileNotFoundError:
            print(f"Error: The file was not found at '{archive_filepath}'")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        try:
            with tarfile.open(fileobj=gzip.GzipFile(fileobj=BytesIO(tarfile_content), mode='rb'), mode='r') as tar:

                for member in tar.getmembers():
                    # Full local file path
                    extract_file_path: str = os.path.join(output_directory, member.name)

                    # Ensure the directory structure exists locally
                    os.makedirs(os.path.dirname(extract_file_path), exist_ok=True)

                    # Process the file content
                    file_content = tar.extractfile(member).read()

                    with open(extract_file_path, 'wb') as file:
                        file.write(file_content)

            log_message(log_level='Info',
                        message=f'Unzipped Direct Data File to {output_directory}')

        except tarfile.TarError or gzip.BadGzipFile as e:
            if isinstance(tarfile.TarError, e):
                log_message(log_level='Error', message=f'Tar file error', exception=e)
            else:
                log_message(log_level='Error', message=f'Gzip file error', exception=e)

    except Exception as e:
        log_message(log_level='Error',
                    message=f'Errors encountered when unzipping direct data files',
                    exception=e)
