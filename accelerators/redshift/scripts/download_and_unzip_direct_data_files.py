import gzip
import os
import sys
import tarfile
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from accelerators.redshift.services.aws_s3_service import AwsS3Service
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


def run(s3_service: AwsS3Service, convert_to_parquet: bool):
    """
    TODO: Is this supposed to delete csv files?
    This method downloads a .tar.gz file from S3, unzips it, converts CSV files to Parquet if `convert_to_parquet` is True,
    deletes the CSV files, and uploads the converted files (either Parquet or CSV) back to S3.

    :param s3_service: An instance of the AwsS3Service class
    :param convert_to_parquet: A boolean value to determine if CSV files should be converted to Parquet
    """
    log_message(log_level='Info',
                message=f'---Executing download_and_unzip_direct_data_files.py---')
    try:
        # Create output directory
        output_directory: str = f"{s3_service.archive_filepath.split('.')[0]}/"
        if output_directory is None or output_directory == "":
            output_directory = os.path.basename(s3_service.archive_filepath)[:-7]
        os.makedirs(output_directory, exist_ok=True)

        # Get the zipped file from S3
        get_object_response: dict = s3_service.get_object(key=s3_service.archive_filepath)
        tarfile_content: bytes = get_object_response['Body'].read()

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
                    if member.name.endswith('.csv') and convert_to_parquet:
                        try:
                            parquet_file_path: str = os.path.splitext(extract_file_path)[0] + '.parquet'
                            convert_csv_to_parquet(file_content=file_content, extract_file_path=parquet_file_path)

                            # Upload the Parquet file to S3, maintaining the directory structure
                            s3_destination_key = f'{output_directory}{member.name}'.replace('.csv', '.parquet')
                            with open(parquet_file_path, 'rb') as parquet_file:
                                s3_service.put_object(key=s3_destination_key, body=parquet_file)
                        except Exception as e:
                            log_message(log_level='Error',
                                        message=f"Failed to convert {member.name} to Parquet",
                                        exception=e)
                        # Skip to the next file if conversion fails
                        continue
                    else:
                        # Upload CSV or any other files directly to S3, maintaining directory structure
                        with open(extract_file_path, 'wb') as file:
                            file.write(file_content)

                        # Upload file to S3 with the same directory structure
                        s3_destination_key = f'{output_directory}{member.name}'
                        s3_service.put_object(key=s3_destination_key, body=file_content)

        except tarfile.TarError or gzip.BadGzipFile as e:
            if isinstance(tarfile.TarError, e):
                log_message(log_level='Error', message=f'Tar file error', exception=e)
            else:
                log_message(log_level='Error', message=f'Gzip file error', exception=e)

    except Exception as e:
        log_message(log_level='Error',
                    message=f'Errors encountered when unzipping direct data files',
                    exception=e)
