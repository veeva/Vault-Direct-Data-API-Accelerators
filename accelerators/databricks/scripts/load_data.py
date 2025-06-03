import os

import pandas as pd
import pyarrow.parquet as pq
from pandas import DataFrame

from common.services.aws_s3_service import AwsS3Service
from accelerators.databricks.services.databricks_service import DatabricksService
from common.utilities import log_message, update_table_name_that_starts_with_digit


def handle_metadata_updates(s3_service: AwsS3Service,
                            databricks_service: DatabricksService,
                            metadata_updates: pd.DataFrame,
                            starting_directory: str,
                            file_extension: str):
    log_message(log_level='Info',
                message=f'Handling metadata updates')

    file_path = f"{starting_directory}/{os.path.splitext(metadata_updates['file'].iloc[0])[0] + file_extension}"

    # Download the file from S3
    s3_service.download_file(file_path, file_path)

    if file_extension == ".parquet":
        metadata_updates = pq.read_table(file_path).to_pandas()
    else:
        metadata_updates = pd.read_csv(file_path)

    unique_extract_values = metadata_updates["extract"].unique()
    for extract in unique_extract_values:
        table_name = update_table_name_that_starts_with_digit(extract.split(".")[1])
        filtered_metadata = metadata_updates[metadata_updates["extract"] == extract]

        # Get column names for this extract
        columns_df = metadata_updates.loc[metadata_updates["extract"] == extract, ["column_name", "type", "length"]]

        if "id" in columns_df["column_name"].tolist():
            databricks_service.create_single_table(table_name=table_name, filtered_metadata=filtered_metadata)
        else:

            existing_columns = databricks_service.retrieve_column_info(table_name=table_name)
            columns_to_add = []
            columns_to_modify = []

            for _, row in columns_df.iterrows():
                column_name = row["column_name"]
                if column_name not in existing_columns.keys():
                    columns_to_add.append(row)


            if len(columns_to_add) > 0:
                add_column_statement = databricks_service.create_sql_str(pd.DataFrame(columns_to_add))
                databricks_service.db_connection.execute_query(f"""
                    ALTER TABLE {databricks_service.schema}.{table_name}
                    ADD COLUMNS ({add_column_statement})
                """)

            # if len(columns_to_modify) > 0:
            #     modify_column_statement = databricks_service.create_sql_str(pd.DataFrame(columns_to_add))
            #     databricks_service.db_connection.execute_query(f"""
            #         ALTER TABLE {databricks_service.schema}.{table_name} {modify_column_statement}
            #     """)


def handle_metadata_deletes(s3_service: AwsS3Service,
                            databricks_service: DatabricksService,
                            metadata_deletes: pd.DataFrame,
                            starting_directory: str,
                            file_extension: str):
    log_message(log_level='Info',
                message=f'Handling metadata deletes')

    file_path = f"{starting_directory}/{os.path.splitext(metadata_deletes['file'].iloc[0])[0] + file_extension}"
    # Check if the file exists in S3
    s3_service.head_object(key=file_path)

    # Download the file from S3
    s3_service.download_file(file_path, file_path)

    if file_extension == ".parquet":
        metadata_deletes_table = pq.read_table(file_path).to_pandas()
    else:
        metadata_deletes_table = pd.read_csv(file_path)

    unique_extract_values = metadata_deletes_table["extract"].unique()

    for extract in unique_extract_values:
        table_name = extract.split(".")[1]

        # Get column names for this extract
        columns = metadata_deletes_table.loc[metadata_deletes_table["extract"] == extract, "column_name"].tolist()

        # Check if 'id' exists in the columns list
        # Check if 'id' exists in the columns list
        if "id" in columns:
            # Drop the entire table if 'id' is a column
            drop_table_command = f"DROP TABLE IF EXISTS {databricks_service.schema}.{table_name}"
            databricks_service.db_connection.execute_query(drop_table_command)
        else:
            if columns:
                enable_column_mapping_mode_query = f"""ALTER TABLE {databricks_service.schema}.{table_name} 
                        SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
                    """
                databricks_service.db_connection.execute_query(enable_column_mapping_mode_query)

                alter_command = f"""ALTER TABLE {databricks_service.schema}.{table_name} 
                                        DROP COLUMN {", ".join(f'{col}' for col in columns)}"""

                databricks_service.db_connection.execute_query(alter_command)


def handle_metadata_changes(s3_service: AwsS3Service,
                            databricks_service: DatabricksService,
                            starting_directory: str,
                            file_extension: str,
                            manifest_table: pd.DataFrame, ):
    log_message(log_level='Info',
                message=f'Handling metadata changes')

    metadata_filter = manifest_table[manifest_table["extract"] == "Metadata.metadata"]

    metadata_deletes = metadata_filter[metadata_filter["type"] == "deletes"]
    metadata_updates = metadata_filter[metadata_filter["type"] == "updates"]

    if not metadata_deletes.empty and int(metadata_deletes['records'].iloc[0]) > 0:
        handle_metadata_deletes(s3_service=s3_service,
                                databricks_service=databricks_service,
                                metadata_deletes=metadata_deletes,
                                starting_directory=starting_directory,
                                file_extension=file_extension)

    if not metadata_updates.empty and int(metadata_updates['records'].iloc[0]) > 0:
        handle_metadata_updates(s3_service=s3_service,
                                databricks_service=databricks_service,
                                metadata_updates=metadata_updates,
                                starting_directory=starting_directory,
                                file_extension=file_extension)


def run(s3_service: AwsS3Service, databricks_service: DatabricksService, direct_data_params: dict,
        convert_to_parquet: bool):
    log_message(log_level='Info',
                message=f'---Executing load_data.py---')
    try:
        databricks_service.db_connection.open()
        starting_directory = f"{s3_service.direct_data_folder}/{s3_service.extract_folder}"
        extract_type = direct_data_params['extract_type']
        infer_schema = databricks_service.infer_schema
        file_extension = '.csv'
        if convert_to_parquet or convert_to_parquet == "true":
            file_extension = '.parquet'

        # Retrieve Manifest file
        manifest_filepath: str = f"{starting_directory}/manifest{file_extension}"
        output_filepath: str = f"{starting_directory}/manifest{file_extension}"
        log_message(log_level='Info',
                    message=f'Retrieving manifest file from s3://{manifest_filepath}')

        os.makedirs(os.path.dirname(output_filepath), exist_ok=True)

        # Check if the file exists in S3
        s3_service.head_object(key=manifest_filepath)

        # Download the file from S3
        s3_service.download_file(manifest_filepath, output_filepath)

        log_message(log_level='Info',
                    message=f'Converting manifest to pandas dataframe')
        if file_extension == ".parquet":
            manifest_table = pq.read_table(manifest_filepath).to_pandas()
            file_format_name = "PARQUET"
        else:
            manifest_table = pd.read_csv(manifest_filepath)
            file_format_name = "CSV"

        if extract_type in ["full", "log"]:
            metadata_filepath = (
                f"{starting_directory}/Metadata/metadata{file_extension}"
                if extract_type == "full"
                else f"{starting_directory}/metadata_full{file_extension}"
            )
            log_message(log_level='Info',
                        message=f'Retrieving metadata file from s3://{metadata_filepath}')
            # Check if the file exists in S3
            s3_service.head_object(key=metadata_filepath)

            # Download the file from S3
            s3_service.download_file(metadata_filepath, metadata_filepath)

            if file_extension == ".parquet":
                metadata_table = pq.read_table(metadata_filepath).to_pandas()
            else:
                metadata_table = pd.read_csv(metadata_filepath)

            if file_extension != ".parquet":
                log_message(log_level='Info',
                            message='Creating tables from metadata file')
                databricks_service.create_all_tables(starting_directory, metadata_table)

        elif extract_type == "incremental":
            handle_metadata_changes(s3_service=s3_service,
                                    databricks_service=databricks_service,
                                    starting_directory=starting_directory,
                                    file_extension=file_extension,
                                    manifest_table=manifest_table)
            databricks_service.delete_data_from_table(starting_directory=starting_directory,
                                                      file_extension=file_extension,
                                                      file_format_name=file_format_name,
                                                      manifest_table=manifest_table)

        manifest_filtered_table: DataFrame = manifest_table[
            (manifest_table["type"] == "updates") & (manifest_table["records"] > 0)]

        for _, row in manifest_filtered_table.iterrows():
            databricks_service.process_manifest_row(row=row,
                                                    starting_directory=starting_directory,
                                                    file_extension=file_extension,
                                                    file_format_name=file_format_name,
                                                    extract_type=extract_type)

        databricks_service.db_connection.close()

    except Exception as e:
        log_message(log_level='Error',
                    message=f'Errors encountered when loading data into Databricks.',
                    exception=e)
