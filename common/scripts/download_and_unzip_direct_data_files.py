import gzip
import os
import sys
import tarfile
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from common.services.object_storage_service import ObjectStorageService
from common.utilities import log_message

sys.path.append('.')


def convert_csv_to_parquet(file_content: bytes, extract_file_path: str):
    log_message(log_level='Debug',
                message=f'Converting CSV to Parquet: {extract_file_path}')

    csv_df = pd.read_csv(BytesIO(file_content), low_memory=False)
    parquet_file_path = os.path.splitext(extract_file_path)[0] + '.parquet'

    # Ensure the parent directory exists before writing the Parquet file
    os.makedirs(os.path.dirname(parquet_file_path), exist_ok=True)

    # Convert DataFrame to Parquet
    table = pa.Table.from_pandas(csv_df)
    with open(parquet_file_path, 'wb') as parquet_file:
        pq.write_table(table, parquet_file)

    log_message(log_level='Info',
                message=f"Parquet file created: {parquet_file_path}")


def run(object_storage_service: ObjectStorageService):
    """
    TODO: Is this supposed to delete csv files?
    This method downloads a .tar.gz file from Object Storage, unzips it, converts CSV files to Parquet if `convert_to_parquet` is True,
    deletes the CSV files, and uploads the converted files (either Parquet or CSV) back to Object Storage.

    :param object_storage_service: An instance of ObjectStorageService class
    """
    log_message(log_level='Info',
                message=f'---Executing download_and_unzip_direct_data_files.py---')
    try:
        # Create output directory
        output_directory: str = f"{object_storage_service.archive_filepath.split('.')[0]}/"
        if output_directory is None or output_directory == "":
            output_directory = os.path.basename(object_storage_service.archive_filepath)[:-7]
        os.makedirs(output_directory, exist_ok=True)

        # Get the zipped file from Object Storage
        tarfile_content: bytes = object_storage_service.download_object_bytes(object_path=object_storage_service.archive_filepath)

        # Write the zipped file to disk
        try:
            with tarfile.open(fileobj=gzip.GzipFile(fileobj=BytesIO(tarfile_content), mode='rb'), mode='r') as tar:
                for member in tar.getmembers():
                    # Full local file path
                    extract_file_path: str = os.path.join(output_directory, member.name)

                    # Ensure the directory structure exists locally
                    os.makedirs(os.path.dirname(extract_file_path), exist_ok=True)

                    # Process the file content
                    file_content = tar.extractfile(member).read()

                    # If it's a CSV file and convert_to_parquet is True, convert it
                    if member.name.endswith('.csv') and object_storage_service.convert_to_parquet:
                        try:
                            parquet_file_path: str = os.path.splitext(extract_file_path)[0] + '.parquet'
                            convert_csv_to_parquet(file_content=file_content, extract_file_path=parquet_file_path)

                            # Upload the Parquet file to Object Storage, maintaining the directory structure
                            upload_object_path: str = f'{output_directory}{member.name}'.replace('.csv', '.parquet')
                            with open(parquet_file_path, 'rb') as parquet_file:
                                object_storage_service.upload_object(object_path=upload_object_path, data=parquet_file)
                        except Exception as e:
                            log_message(log_level='Error',
                                        message=f"Failed to convert {member.name} to Parquet",
                                        exception=e)
                        # Skip to the next file if conversion fails
                        continue
                    else:
                        # Upload CSV or any other files directly to Object Storage, maintaining directory structure
                        with open(extract_file_path, 'wb') as file:
                            file.write(file_content)

                        # Upload file to Object Storage with the same directory structure
                        upload_object_path = f'{output_directory}{member.name}'
                        object_storage_service.upload_object(object_path=upload_object_path, data=file_content)

        except tarfile.TarError or gzip.BadGzipFile as e:
            if isinstance(tarfile.TarError, e):
                log_message(log_level='Error', message=f'Tar file error', exception=e)
            else:
                log_message(log_level='Error', message=f'Gzip file error', exception=e)

    except Exception as e:
        log_message(log_level='Error',
                    message=f'Errors encountered when unzipping direct data files',
                    exception=e)
