import time
from typing import Generator, List, Any

from pandas import Series

import pyrfc6266

from common.services.aws_s3_service import AwsS3Service
from common.services.vault_service import VaultService
from common.api.model.response.document_response import DocumentExportResponse
from common.api.model.response.jobs_response import JobCreateResponse
from common.api.model.response.vault_response import VaultResponse
from common.api.request.document_request import DocumentRequest
from common.utilities import log_message
import pandas as pd
import pyarrow.parquet as pq


def batch_document_list(data_list: List[Any], batch_size: int) -> Generator[List[Any], None, None]:
    if not isinstance(data_list, list):
        raise TypeError("Input 'data_list' must be a list.")
    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("'batch_size' must be a positive integer.")

    for i in range(0, len(data_list), batch_size):
        yield data_list[i:i + batch_size]

def run(s3_service: AwsS3Service, vault_service: VaultService, convert_to_parquet: bool):
    log_message(log_level='Info',
                message=f'---Executing extract_doc_content.py---')

    starting_directory = f"{s3_service.direct_data_folder}/{s3_service.extract_folder}"
    file_extension = '.csv'
    if convert_to_parquet or convert_to_parquet == "true":
        file_extension = '.parquet'

    document_filepath: str = f"{starting_directory}/Document/document_version__sys{file_extension}"
    log_message(log_level='Info',
                message=f'Retrieving document version file from s3://{document_filepath}')

    # Check if the file exists in S3
    s3_service.head_object(key=f"{document_filepath}")

    # Download the file from S3
    s3_service.download_file(document_filepath, document_filepath)

    if file_extension == '.parquet':
        document_table = pq.read_table(document_filepath).to_pandas()
    else:
        document_table = pd.read_csv(document_filepath)

    total_doc_versions: List[str] = document_table['version_id']
    document_version_list: Series = total_doc_versions[document_table['size__v'].notna()]
    direct_data_folder = f"{s3_service.direct_data_folder}/{s3_service.extract_folder}/{s3_service.document_content_folder}"

    document_batches = batch_document_list(document_version_list.tolist(), 10000)

    for i, batch_of_versions in enumerate(document_batches):
        try:
            request_string = []
            for doc_version_id in batch_of_versions:
                split_version_id: list[str] = doc_version_id.split('_')
                doc_id = split_version_id[0]
                major_version = split_version_id[1]
                minor_version = split_version_id[2]

                doc_version_dict = {
                    "id": doc_id,
                    "major_version_number__v": major_version,
                    "minor_version_number__v": minor_version
                }
                request_string.append(doc_version_dict)

            export_doc_response: JobCreateResponse = vault_service.export_document_versions(request_string)

            job_id: int = export_doc_response.job_id
            if job_id is None:
                log_message(log_level='Error',
                        message=export_doc_response.errors)
                raise Exception("An error has occured exporting document versions")
            is_vault_job_finished = False

            log_message(log_level='Info',
                        message=f'Polling status of job {job_id} in Vault')
            while not is_vault_job_finished:
                response: DocumentExportResponse = vault_service.retrieve_document_export_results(job_id=job_id)

                if response.responseStatus == 'SUCCESS':
                    for exported_document in response.data:

                        # If the individual document export failed, log it and skip to the next one.
                        if exported_document.responseStatus == 'FAILURE':
                            log_message(log_level='Warning',
                                        message=f'ResponseStatus: {exported_document.responseStatus} - Skipping document ID {exported_document.id}')
                            continue  # Go to the next exported_document

                        # If the individual document export was successful, download it and put on S3.
                        if exported_document.responseStatus == "SUCCESS":
                            file_staging_response: VaultResponse = vault_service.download_item_from_file_staging(exported_document=exported_document)
                            filename: str = pyrfc6266.parse_filename(file_staging_response.headers.get("Content-Disposition"))
                            log_message(log_level='Debug',
                                        message=f'File Staging results: {file_staging_response.responseMessage}')
                            s3_service.put_object(key=f'{direct_data_folder}/{exported_document.id}/{exported_document.major_version_number__v}_{exported_document.minor_version_number__v}/{filename}',
                                                  body=file_staging_response.binary_content)
                    is_vault_job_finished = True
                else:
                    log_message(log_level='Debug',
                                message=f'Waiting 10s to check the job status')
                    time.sleep(10)

        except Exception as e:
            log_message(log_level='Error',
                        message=f'Error when attempting to download document',
                        exception=e)


