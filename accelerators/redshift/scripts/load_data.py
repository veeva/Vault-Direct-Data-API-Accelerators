import csv
import io
import os
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pyarrow.parquet as pq

from accelerators.redshift.services.redshift_service import RedshiftService
from accelerators.redshift.services.aws_s3_service import AwsS3Service
from common.utilities import log_message
from common.utilities import update_table_name_that_starts_with_digit


def handle_metadata_updates(s3_service: AwsS3Service,
                            redshift_service: RedshiftService,
                            metadata_updates: pd.DataFrame,
                            starting_directory: str):
    log_message(log_level='Info',
                message=f'Handling metadata updates')

    file_path = f"{starting_directory}/{os.path.splitext(metadata_updates['file'].iloc[0])[0]}.csv"

    # Download the file from S3
    s3_service.download_file(file_path, file_path)

    # if file_extension == ".parquet":
    #     metadata_updates_table = pq.read_table(file_path).to_pandas()
    # else:
    metadata_updates_table = pd.read_csv(file_path)

    unique_extract_values = metadata_updates_table["extract"].unique()
    for extract in unique_extract_values:
        table_name = update_table_name_that_starts_with_digit(extract.split(".")[1])
        filtered_metadata = metadata_updates_table[metadata_updates_table["extract"] == extract]

        # Get column names for this extract
        columns_df = metadata_updates_table.loc[
            metadata_updates_table["extract"] == extract, ["column_name", "type", "length"]]

        if "id" in columns_df["column_name"].tolist():
            redshift_service.create_single_table(table_name=table_name,
                                                 filtered_metadata=filtered_metadata)
        else:
            existing_columns = redshift_service.retrieve_column_info(table_name=table_name)
            columns_to_add = []
            columns_to_modify = []

            for _, row in columns_df.iterrows():
                column_name = row["column_name"]
                if column_name not in existing_columns.keys():
                    columns_to_add.append(row)
                else:
                    new_column_type = row["type"].lower()
                    if new_column_type != "string":
                        columns_to_modify.append(row[['column_name', 'type', 'length']].to_dict())


            if len(columns_to_add) > 0:
                add_column_statement = redshift_service.create_sql_str(pd.DataFrame(columns_to_add), False, is_modify=False, is_add=True)
                for column_def in add_column_statement.split(", "):
                    alter_query = f"""
                            ALTER TABLE {redshift_service.schema}.{table_name} {column_def}
                        """
                    redshift_service.db_connection.execute_query(alter_query)

            if len(columns_to_modify) > 0:
                modify_column_statement = redshift_service.create_sql_str(pd.DataFrame(columns_to_modify), False, is_modify=True, is_add=False)
                redshift_service.db_connection.execute_query(f"""
                            ALTER TABLE {redshift_service.schema}.{table_name} {modify_column_statement}
                        """)

def handle_metadata_deletes(s3_service: AwsS3Service,
                            redshift_service: RedshiftService,
                            metadata_deletes: pd.DataFrame,
                            starting_directory: str):
    log_message(log_level='Info',
                message=f'Handling metadata deletes')

    file_path = f"{starting_directory}/{os.path.splitext(metadata_deletes['file'].iloc[0])[0]}.csv"

    # Download the file from S3
    s3_service.download_file(file_path, file_path)

    # if file_extension == ".parquet":
    #     metadata_deletes_table = pq.read_table(file_path).to_pandas()
    # else:
    metadata_deletes_table = pd.read_csv(file_path)

    unique_extract_values = metadata_deletes_table["extract"].unique()

    for extract in unique_extract_values:
        table_name = extract.split(".")[1]

        # Get column names for this extract
        columns = metadata_deletes_table.loc[metadata_deletes_table["extract"] == extract, "column_name"].tolist()

        # Check if 'id' exists in the columns list
        if "id" in columns:
            # Drop the entire table if 'id' is a column
            drop_table_command = f"DROP TABLE IF EXISTS {redshift_service.schema}.{table_name}"
            redshift_service.db_connection.execute_query(drop_table_command)
        else:
            if columns:
                alter_command = f"""ALTER TABLE {redshift_service.schema}.{table_name} 
                                        {", ".join(f'DROP COLUMN "{col}"' for col in columns)}"""
                redshift_service.db_connection.execute_query(alter_command)


def handle_metadata_changes(s3_service: AwsS3Service,
                            redshift_service: RedshiftService,
                            starting_directory: str,
                            manifest_table: pd.DataFrame, ):
    log_message(log_level='Info',
                message=f'Handling metadata changes')

    metadata_filter = manifest_table[manifest_table["extract"] == "Metadata.metadata"]

    metadata_deletes = metadata_filter[metadata_filter["type"] == "deletes"]
    metadata_updates = metadata_filter[metadata_filter["type"] == "updates"]

    # Handle deletes if any
    if not metadata_deletes.empty and int(metadata_deletes['records'].iloc[0]) > 0:
        handle_metadata_deletes(s3_service=s3_service, redshift_service=redshift_service, metadata_deletes=metadata_deletes,
                                starting_directory=starting_directory)

    # Handle updates if any
    if not metadata_updates.empty and int(metadata_updates['records'].iloc[0]) > 0:
        handle_metadata_updates(s3_service=s3_service, redshift_service=redshift_service, metadata_updates=metadata_updates,
                                starting_directory=starting_directory)


def process_manifest_row(redshift_service: RedshiftService,
                         s3_service: AwsS3Service,
                         row: pd.Series,
                         starting_directory: str,
                         extract_type: str):
    raw_table_name: str = row["extract"].split(".")[1]
    table_name: str = update_table_name_that_starts_with_digit(raw_table_name)
    file: str = row['file']

    s3_uri: str = f"s3://{s3_service.bucket_name}/{starting_directory}/{file}"

    if not (extract_type == "incremental" and "metadata" in table_name):
        load_data_into_tables(redshift_service=redshift_service,
                              s3_service= s3_service,
                              extract_type=extract_type,
                              table_name=table_name,
                              file=file,
                              s3_uri=s3_uri)


def load_data_into_tables(redshift_service: RedshiftService,
                          s3_service: AwsS3Service,
                          extract_type: str,
                          table_name: str,
                          file: str,
                          s3_uri: str):

    headers = get_csv_headers(s3_service=s3_service, csv_location=file)

    if extract_type in ["full", "log"]:
        # Load data into the table
        redshift_service.db_connection.execute_query(f"""
                COPY {redshift_service.database}.{redshift_service.schema}.{table_name} ({headers}) 
                FROM '{s3_uri}' 
                IAM_ROLE '{redshift_service.iam_role}' 
                FORMAT AS CSV 
                QUOTE '\"' 
                IGNOREHEADER 1 
                TIMEFORMAT 'auto' 
                ACCEPTINVCHARS 
                FILLRECORD 
                TRUNCATECOLUMNS;
            """)
    else:
        if table_name == 'metadata':
            primary_keys = ["extract", "column_name"]
        elif table_name == 'picklist__sys':
            primary_keys = ["object", "object_field", "picklist_value_name"]
        else:
            primary_keys = ["id"]

            # Load into a temporary staging table
        staging_table_name = f"{table_name}_staging"
        pk_condition = " AND ".join(
            [f"{redshift_service.schema}.{table_name}.{col} = {staging_table_name}.{col}" for col in primary_keys])

        copy_query = f"""
                        CREATE TEMP TABLE {staging_table_name} (LIKE {redshift_service.schema}.{table_name});
                        COPY {staging_table_name} ({headers}) 
                        FROM '{s3_uri}'
                        IAM_ROLE '{redshift_service.iam_role}'
                        FORMAT AS CSV
                        QUOTE '\"'
                        IGNOREHEADER 1
                        TIMEFORMAT 'auto'
                        ACCEPTINVCHARS
                        FILLRECORD
                        TRUNCATECOLUMNS;
                    """

        delete_duplicates_query = f"""
                        DELETE FROM {redshift_service.schema}.{table_name}
                        USING {staging_table_name}
                        WHERE {pk_condition};
                    """

        insert_query = f"""
                        INSERT INTO {redshift_service.schema}.{table_name}
                        SELECT DISTINCT * FROM {staging_table_name};
                    """
        redshift_service.db_connection.execute_query(copy_query)
        redshift_service.db_connection.execute_query(delete_duplicates_query)
        redshift_service.db_connection.execute_query(insert_query)


def run(s3_service: AwsS3Service, redshift_service: RedshiftService, direct_data_params: dict):
    log_message(log_level='Info',
                message=f'---Executing load_data_into_snowflake.py---')
    try:
        starting_directory = f"{s3_service.direct_data_folder}/{s3_service.extract_folder}"
        extract_type = direct_data_params['extract_type']

        redshift_service.db_connection.activate_cursor()

        redshift_service.check_if_schema_exists()

        manifest_filepath: str = f"{starting_directory}/manifest.csv"
        output_filepath: str = f"{starting_directory}/manifest.csv"
        log_message(log_level='Info',
                    message=f'Retrieving manifest file from s3://{manifest_filepath}')

        os.makedirs(os.path.dirname(output_filepath), exist_ok=True)

        # Check if the file exists in S3
        s3_service.head_object(key=manifest_filepath)

        # Download the file from S3
        s3_service.download_file(manifest_filepath, output_filepath)

        log_message(log_level='Info',
                    message=f'Converting manifest to pandas dataframe')

        manifest_table = pd.read_csv(manifest_filepath)

        if extract_type in ["full", "log"]:
            metadata_filepath = (
                f"{starting_directory}/Metadata/metadata.csv"
                if extract_type == "full"
                else f"{starting_directory}/metadata_full.csv"
            )

            log_message(log_level='Info',
                        message=f'Retrieving manifest file from s3://{manifest_filepath}')
            # Check if the file exists in S3
            s3_service.head_object(key=metadata_filepath)

            # Download the file from S3
            s3_service.download_file(metadata_filepath, metadata_filepath)

            metadata_table = pd.read_csv(metadata_filepath)

            log_message(log_level='Info',
                        message='Creating tables from metadata file')
            redshift_service.create_all_tables(starting_directory, metadata_table)

        elif extract_type == "incremental":

            handle_metadata_changes(s3_service=s3_service,
                                    redshift_service=redshift_service,
                                    starting_directory=starting_directory,
                                    manifest_table=manifest_table)
            redshift_service.delete_data_from_table(s3_bucket_name=s3_service.bucket_name, starting_directory=starting_directory,
                                                    manifest_table=manifest_table)

        manifest_filtered_table = manifest_table[
            (manifest_table["type"] == "updates") & (manifest_table["records"] > 0)]


        for _, row in manifest_filtered_table.iterrows():
            process_manifest_row(redshift_service=redshift_service,
                                 s3_service=s3_service,
                                 row=row,
                                 starting_directory=starting_directory,
                                 extract_type=extract_type)

        redshift_service.db_connection.close_cursor()
        redshift_service.db_connection.close()

    except Exception as e:
        log_message(log_level='Error',
                    message=f'Errors encountered when loading data into Snowflake.',
                    exception=e)

def get_csv_headers(s3_service: AwsS3Service, csv_location):
    """
    This method retrieves the headers of the specified CSV in the order they appear in the file

    :param s3_service:
    :param csv_location: The location of the CSV file within the starting directory
    :return: A string of ordered column headers of the provided CSV files
    """
    log_message(log_level='Info',
                message=f'Retrieving CSV headers for {s3_service.direct_data_folder}/{s3_service.extract_folder}/{csv_location}',
                context=None)

    response = s3_service.get_object(key=f'{s3_service.direct_data_folder}/{s3_service.extract_folder}/{csv_location}')

    try:
        with io.TextIOWrapper(response['Body'], encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            headers = next(csv_reader)  # Read the first line containing headers

        updated_headers = [update_table_name_that_starts_with_digit(header) for header in headers]
        headers_str = ', '.join(updated_headers)

        return headers_str
    except csv.Error as e:
        log_message(log_level='Error',
                    message=f'Error reading CSV file: {e}',
                    exception=e,
                    context=None)
        return None
    except StopIteration:
        log_message(log_level='Error',
                    message='CSV file appears to be empty or corrupted',
                    context=None)
        return None