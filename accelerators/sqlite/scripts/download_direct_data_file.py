import sys
from typing import Dict, Any

from common.api.model.response.direct_data_response import DirectDataResponse
from common.api.model.response.vault_response import VaultResponse
from common.services.vault_service import VaultService
from common.utilities import log_message

sys.path.append('.')

def _handle_multipart_download(local_params: dict, vault_service: VaultService,
                               direct_data_item: DirectDataResponse.DirectDataItem):
    log_message(log_level='Info',
                message=f'Handling multipart upload')

    try:
        archive_filepath: str = local_params['archive_filepath']
        for file_part in direct_data_item.filepart_details:
            download_response: VaultResponse = vault_service.download_direct_data_file(
                name=file_part.name)

            write_mode: str = 'ab' if file_part.filepart > 1 else 'wb'
            with open(archive_filepath, write_mode) as f:
                f.write(download_response.binary_content)

        log_message(log_level='Info',
                    message=f'Multipart download completed')

    except Exception as e:
        log_message(log_level='Error',
                    message=f'Failed to download multipart Direct Data file',
                    exception=e)
        raise e

def run(vault_service: VaultService,
        direct_data_params: Dict[str, Any],
        local_params: Dict[str, Any]) -> None:
    """
    This script downloads a .tar.gz file from Direct Data API to local storage.
    """
    log_message(log_level='Info',
                message=f'---Executing download_direct_data_file.py---')

    try:
        # List the Direct Data files of the specified extract type and time window
        extract_type: str = f"{direct_data_params['extract_type']}_directdata"
        start_time: str = direct_data_params['start_time']
        stop_time: str = direct_data_params['stop_time']
        list_direct_data_files_response: DirectDataResponse = vault_service.retrieve_available_direct_data_files(
            extract_type=extract_type,
            start_time=start_time,
            stop_time=stop_time
        )

        # Download the latest Direct Data file in the response, and upload to file storage.
        direct_data_item: DirectDataResponse.DirectDataItem = list_direct_data_files_response.data[-1]

        # Exit if there are no records in the Direct Data extract
        archive_filepath: str = local_params['archive_filepath']
        if direct_data_item.record_count == 0:
            log_message(log_level='Info',
                        message=f'No records in the Direct Data extract.')
            return

        tarfile_content: bytes
        if direct_data_item.fileparts == 1:
            download_response: VaultResponse = vault_service.download_direct_data_file(
                name=direct_data_item.filepart_details[0].name)

            tarfile_content = download_response.binary_content
            try:
                with open(archive_filepath, "wb") as f:
                    f.write(tarfile_content)

            except IOError as exception:
                log_message(log_level='Error',
                            message=f'Error writing Direct Data file to disk',
                            exception=exception)

        else:
            _handle_multipart_download(local_params=local_params,
                                       vault_service=vault_service,
                                       direct_data_item=direct_data_item)

        log_message(log_level='Info',
                    message=f'Downloaded Direct Data file to {archive_filepath}')

    except Exception as exception:
        log_message(log_level='Error',
                    message=f'Error retrieving Direct Data files from Vault',
                    exception=exception)

