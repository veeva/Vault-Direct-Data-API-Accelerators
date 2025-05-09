import json
import urllib.parse
from pathlib import Path

from common.api.client.vault_client import VaultClient
from common.api.model.response.direct_data_response import DirectDataResponse
from common.api.model.response.document_response import DocumentExportResponse
from common.api.model.response.jobs_response import JobCreateResponse
from common.api.model.response.vault_response import VaultResponse
from common.api.request.direct_data_request import DirectDataRequest, ExtractType
from common.api.request.document_request import DocumentRequest
from common.api.request.file_staging_request import FileStagingRequest
from common.utilities import log_message


class VaultService:
    _vault_client: VaultClient = None

    def __init__(self, vapil_settings_filepath: str):
        if VaultService._vault_client is None:
            VaultService._vault_client = VaultClient.authenticate_from_settings_file(
                file_path=vapil_settings_filepath)

    def retrieve_available_direct_data_files(self, extract_type: str, start_time: str, stop_time: str) -> DirectDataResponse:
        log_message(log_level='Debug',
                    message=f'Listing Direct Data files with start time: {start_time} and stop time: {stop_time}',
                    context=None)
        try:
            request: DirectDataRequest = self._vault_client.new_request(DirectDataRequest)
            response: DirectDataResponse = request.retrieve_available_direct_data_files(
                extract_type=ExtractType(extract_type.lower()),
                start_time=start_time, stop_time=stop_time)

            if response.has_errors():
                raise Exception(response.errors[0].message)
            if response.is_failure() or not bool(response.data):
                raise Exception('Error retrieving Direct Data files')
            log_message(log_level='Info',
                        message='Direct Data files listed successfully',
                        context=None)
            return response

        except Exception as e:
            log_message(log_level='Error',
                        message=f'Exception when listing Direct Data files',
                        exception=e,
                        context=None)
            raise e

    def download_direct_data_file(self, name: str, filepart: int) -> VaultResponse:
        log_message(log_level='Debug',
                    message=f'Downloading Direct Data file: {name}')
        try:
            request: DirectDataRequest = self._vault_client.new_request(DirectDataRequest)
            response: VaultResponse = request.download_direct_data_file(
                name=name, filepart=filepart)

            if response.has_errors():
                raise Exception(response.errors[0].message)

            log_message(log_level='Info',
                        message=f'Direct Data file: {name} downloaded successfully')

            return response

        except Exception as e:
            log_message(log_level='Error',
                        message=f'Exception when downloading Direct Data file',
                        exception=e)
            raise e

    def download_document_version(self, document_version_id: str) -> VaultResponse:

        split_document_version_id = document_version_id.split("_")
        request: DocumentRequest = self._vault_client.new_request(DocumentRequest)
        response: VaultResponse = request.download_document_version_file(int(split_document_version_id[0]),
                                                                         int(split_document_version_id[1]),
                                                                         int(split_document_version_id[2]))

        if response.has_errors():
            raise Exception(response.errors[0].message)

        return response

    def export_document_versions(self, document_version_dict: list[dict[str, str]]) -> JobCreateResponse:
        log_message(log_level='Debug',
                    message=f'Exporting Document Versions')
        doc_request: DocumentRequest = self._vault_client.new_request(DocumentRequest)
        doc_response: JobCreateResponse = doc_request.export_document_versions(
            request_string=json.dumps(document_version_dict),
            include_source=True,
            include_renditions=False)
        log_message(log_level='Info',
                    message=f'Export Document Versions Job Initiated')
        return doc_response

    def retrieve_document_export_results(self, job_id: int) -> DocumentExportResponse:
        log_message(log_level='Debug',
                    message=f'Retrieve document export results for Job ID: {job_id}')
        try:

            document_request: DocumentRequest = self._vault_client.new_request(DocumentRequest)
            response: DocumentExportResponse = document_request.retrieve_document_export_results(job_id=job_id)

            return response

        except Exception as e:
            log_message(log_level='Error',
                        message=f'Error retrieving document export results for Job ID: {job_id}',
                        exception=e)
            raise e

    def download_item_from_file_staging(self, exported_document: DocumentExportResponse.ExportedDocument) -> VaultResponse:
        log_message(log_level='Debug',
                    message=f'File Path on Staging Server: {exported_document.file}')
        file_staging_request: FileStagingRequest = self._vault_client.new_request(FileStagingRequest)
        log_message(log_level='Debug',
                    message=f'File Staging Request: {file_staging_request}')

        path_object = Path(f'u{exported_document.user_id__v}{exported_document.file}')
        file_path_posix = path_object.as_posix()
        encoded_file_path = urllib.parse.quote(file_path_posix)

        file_staging_response: VaultResponse = file_staging_request.download_item_content(
            item=encoded_file_path)

        return file_staging_response