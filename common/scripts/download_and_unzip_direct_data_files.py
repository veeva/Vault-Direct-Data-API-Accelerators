import gzip
import os
import sys
import tarfile
import io
from io import BytesIO
from typing import Dict, Any, IO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from common.services.object_storage_service import ObjectStorageService
from common.utilities import log_message

sys.path.append('.')


def process_tar_gz_member(tar: tarfile.TarFile,
                          member: tarfile.TarInfo,
                          object_storage_service: ObjectStorageService,
                          metadata_df: pd.DataFrame) -> None:
    try:
        log_message(log_level='Debug',
                    message=f'Processing TAR Member: {member.name}')
        output_directory: str = f"{object_storage_service.archive_filepath.split('.')[0]}/"
        if output_directory is None or output_directory == "":
            output_directory = os.path.basename(object_storage_service.archive_filepath)[:-7]
        os.makedirs(output_directory, exist_ok=True)

        # Full local file path
        extract_file_path: str = os.path.join(output_directory, member.name)

        # Ensure the directory structure exists locally
        os.makedirs(os.path.dirname(extract_file_path), exist_ok=True)

        # Process the file content
        file_content: IO[bytes] = tar.extractfile(member)
        is_first_chunk = True

        with pd.read_csv(file_content, chunksize=100000) as reader:
            if member.name.endswith('.csv') and object_storage_service.convert_to_parquet:
                extract_file_path = extract_file_path.replace('.csv', '.parquet')
                os.makedirs(os.path.dirname(extract_file_path), exist_ok=True)
                first_chunk = next(reader)
                schema = get_pyarrow_schema(metadata_df=metadata_df,
                                            csv_df=first_chunk,
                                            extract_name=member.name)
                cleaned_first_chunk = clean_column_data_types(csv_df=first_chunk, schema=schema)
                with pq.ParquetWriter(where=extract_file_path, schema=schema) as writer:
                    table: pa.Table = pa.Table.from_pandas(df=cleaned_first_chunk, schema=schema)
                    writer.write_table(table=table)
                    is_first_chunk = False
                    for chunk in reader:
                        cleaned_chunk = clean_column_data_types(csv_df=chunk, schema=schema)
                        table: pa.Table = pa.Table.from_pandas(df=cleaned_chunk, schema=schema)
                        writer.write_table(table=table)


            else:
                for chunk in reader:
                    if is_first_chunk:
                        # For the first chunk, write to a new file with the header
                        chunk.to_csv(extract_file_path, mode='w', header=True, index=False)
                        is_first_chunk = False
                    else:
                        # For all other chunks, append to the existing file without the header
                        chunk.to_csv(extract_file_path, mode='a', header=False, index=False)

            # Upload file to Object Storage with the same directory structure
            with open(extract_file_path, 'rb') as file:
                object_storage_service.upload_object(object_path=extract_file_path, data=file)


    except Exception as e:
        log_message(log_level='Error',
                    message=f"Failed to process tar member {member.name}",
                    exception=e)


def get_manifest_schema() -> pa.Schema:
    return pa.schema([
        pa.field('extract', pa.string()),
        pa.field('extract_label', pa.string()),
        pa.field('type', pa.string()),
        pa.field('records', pa.int64()),
        pa.field('file', pa.string())
    ])


def get_metadata_schema() -> pa.Schema:
    return pa.schema([
        pa.field('modified_date__v', pa.timestamp('ns', tz='UTC')),
        pa.field('extract', pa.string()),
        pa.field('extract_label', pa.string()),
        pa.field('column_name', pa.string()),
        pa.field('column_label', pa.string()),
        pa.field('type', pa.string()),
        pa.field('length', pa.int64()),
        pa.field('related_extract', pa.string())
    ])


def get_pyarrow_schema(metadata_df: pd.DataFrame, csv_df: pd.DataFrame, extract_name: str) -> pa.Schema:
    log_message('Debug', f"Generating schema for file: {extract_name}")

    # Filter metadata for the relevant table
    if extract_name == 'manifest.csv':
        return get_manifest_schema()

    if extract_name == 'Metadata/metadata.csv' or extract_name == 'metadata_full.csv':
        return get_metadata_schema()

    normalized_extract_name = os.path.splitext(extract_name)[0].replace('/', '.')
    extract_metadata = metadata_df[metadata_df['extract'] == normalized_extract_name]

    # Mapping from your metadata types to PyArrow types
    type_mapping: Dict[str, Any] = {
        'String': pa.string(),
        'Number': pa.int64(),
        'LongText': pa.large_string(),
        'Date': pa.date32(),
        'DateTime': pa.timestamp('ms', tz='UTC'),
        'Relationship': pa.string(),
        'MultiRelationship': pa.string(),
        'Picklist': pa.string(),
        'MultiPicklist': pa.string(),
        'Boolean': pa.bool_()
    }

    fields = []
    for column_name in csv_df.columns:
        # Find the metadata for the current column
        column_meta: pd.DataFrame = extract_metadata[extract_metadata['column_name'] == column_name]
        if not column_meta.empty:
            meta_type = column_meta.iloc[0]['type']

            if meta_type == 'Number':
                # Convert the column to numeric first
                numeric_col = pd.to_numeric(csv_df[column_name], errors='coerce')

                # Check if any non-null value has a decimal part
                if numeric_col.dropna().apply(lambda x: x != int(x)).any():
                    # If decimals exist, use float
                    log_message('Info', f"Column '{column_name}' contains decimals. Setting type to float64.")
                    pa_type = pa.float64()
                else:
                    pa_type = pa.int64()
            else:
                # Use the standard mapping for all other types
                pa_type = type_mapping.get(meta_type, pa.string())

            fields.append(pa.field(column_name, pa_type))
        else:
            # Default for columns not in metadata
            log_message('Warning', f"Column '{column_name}' not found in metadata. Defaulting to string.")
            fields.append(pa.field(column_name, pa.string()))

    return pa.schema(fields)


def clean_column_data_types(csv_df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
    csv_df_cleaned = csv_df.copy()
    for field in schema:
        col_name = field.name
        if col_name not in csv_df_cleaned.columns:
            continue  # Skip if the column doesn't exist in the CSV

        # --- Handles Timestamp Columns ---
        if pa.types.is_timestamp(field.type):
            csv_df_cleaned[col_name] = pd.to_datetime(csv_df_cleaned[col_name], errors='coerce')

        # --- Handles Date Columns ---
        elif pa.types.is_date(field.type):
            csv_df_cleaned[col_name] = pd.to_datetime(csv_df_cleaned[col_name], errors='coerce').dt.date

        # --- Handles Integer Columns ---
        elif pa.types.is_integer(field.type):
            csv_df_cleaned[col_name] = pd.to_numeric(csv_df_cleaned[col_name], errors='coerce').astype('Int64')

        # --- Handles Float Columns ---
        elif pa.types.is_floating(field.type):
            csv_df_cleaned[col_name] = pd.to_numeric(csv_df_cleaned[col_name], errors='coerce').astype('float64')

        elif pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
            csv_df_cleaned[col_name] = csv_df_cleaned[col_name].astype(str)

    return csv_df_cleaned


def run(object_storage_service: ObjectStorageService):
    """
    This method downloads a .tar.gz file from Object Storage, unzips it, converts CSV files to Parquet if `convert_to_parquet` is True,
    deletes the CSV files, and uploads the converted files (either Parquet or CSV) back to Object Storage.

    :param object_storage_service: An instance of ObjectStorageService class
    """
    log_message(log_level='Info',
                message=f'---Executing download_and_unzip_direct_data_files.py---')
    try:
        # Get the zipped file from Object Storage
        tarfile_content: bytes = object_storage_service.download_object_bytes(
            object_path=object_storage_service.archive_filepath)

        # Write the zipped file to disk
        try:
            with tarfile.open(fileobj=gzip.GzipFile(fileobj=BytesIO(tarfile_content), mode='rb'), mode='r') as tar:

                # --- First Pass: Find and process metadata.csv ---
                for member in tar.getmembers():
                    filename = os.path.basename(member.name).lower()

                    if 'metadata_full.csv' in filename or 'metadata.csv' in filename:
                        file_content = tar.extractfile(member).read()
                        metadata_df = pd.read_csv(BytesIO(file_content))
                        break

                for member in tar.getmembers():
                    process_tar_gz_member(metadata_df=metadata_df,
                                          member=member,
                                          tar=tar,
                                          object_storage_service=object_storage_service)


        except tarfile.TarError or gzip.BadGzipFile as e:
            if isinstance(tarfile.TarError, e):
                log_message(log_level='Error', message=f'Tar file error', exception=e)
            else:
                log_message(log_level='Error', message=f'Gzip file error', exception=e)

    except Exception as e:
        log_message(log_level='Error',
                    message=f'Errors encountered when unzipping direct data files',
                    exception=e)
