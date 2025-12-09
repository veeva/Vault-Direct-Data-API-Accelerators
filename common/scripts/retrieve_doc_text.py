from typing import Generator, List, Any
import io

import pandas as pd
import pyarrow.parquet as pq
from pandas import DataFrame

from common.api.model.response.vault_response import VaultResponse
from common.services.object_storage_service import ObjectStorageService
from common.services.vault_service import VaultService
from common.utilities import log_message


def batch_document_list(data_list: List[Any], batch_size: int) -> Generator[List[Any], None, None]:
    if not isinstance(data_list, list):
        raise TypeError("Input 'data_list' must be a list.")
    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("'batch_size' must be a positive integer.")

    for i in range(0, len(data_list), batch_size):
        yield data_list[i:i + batch_size]


def run(object_storage_service: ObjectStorageService, vault_service: VaultService):
    log_message(log_level='Info',
                message=f'---Executing retrieve_doc_text.py---')

    starting_directory: str = f"{object_storage_service.direct_data_folder}/{object_storage_service.extract_folder}"
    file_extension: str = '.csv'
    if object_storage_service.convert_to_parquet:
        file_extension = '.parquet'

    document_filepath: str = f"{starting_directory}/Document/document_version__sys{file_extension}"
    log_message(log_level='Info',
                message=f'Retrieving document version file from Object Storage')

    # Check if the file exists in Object Storage
    object_storage_service.check_if_object_exists(object_path=document_filepath)

    # Download the file from Object Storage to local
    object_storage_service.download_object_to_local(object_path=document_filepath, output_path=document_filepath)

    if file_extension == '.parquet':
        document_table = pq.read_table(document_filepath).to_pandas()
    else:
        document_table = pd.read_csv(document_filepath)

    # 1. Filter the DataFrame and select the desired columns.
    filtered_df: DataFrame = document_table.loc[
        document_table['size__v'].notna(),  # Row filter: keep rows where size__v is not null
        ['version_id', 'name__v']           # Column selection: keep these two columns
    ]

    # 2. Convert the resulting DataFrame into a list of dictionaries.
    document_list_for_batching: list[dict] = filtered_df.to_dict('records')
    direct_data_folder: str = f"{object_storage_service.direct_data_folder}/{object_storage_service.extract_folder}/{object_storage_service.document_text_folder}"

    document_batches: Generator[List] = batch_document_list(document_list_for_batching, 10000)

    for i, batch_of_doc_versions in enumerate(document_batches):
        try:
            for doc_version in batch_of_doc_versions:
                split_version_id: list[int] = doc_version['version_id'].split('_')
                doc_id: int = int(split_version_id[0])
                major_version: int = int(split_version_id[1])
                minor_version: int = int(split_version_id[2])

                download_text_response: VaultResponse = vault_service.retrieve_document_version_text(doc_id=doc_id,
                                                                                                     major_version=major_version,
                                                                                                     minor_version=minor_version)
                if download_text_response.has_errors():
                    log_message(log_level='Warning',
                                message=f'Error downloading document text for Document ID: {doc_id} - {download_text_response.errors[0].message}'
                                        f' - Skipping to next document',)
                    continue
                object_path: str = f'{direct_data_folder}/{doc_id}/{major_version}_{minor_version}/{doc_version["name__v"]}.txt'
                object_storage_service.upload_object(object_path=object_path,
                                                     data=io.BytesIO(download_text_response.binary_content))

        except Exception as e:
            log_message(log_level='Error',
                        message=f'Error when attempting to download document text',
                        exception=e)
