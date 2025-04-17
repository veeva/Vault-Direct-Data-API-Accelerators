from common.api.client.vault_client import VaultClient
from common.api.model.response.direct_data_response import DirectDataResponse
from common.api.model.response.vault_response import VaultResponse
from common.api.request.direct_data_request import DirectDataRequest, ExtractType
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
                    message=f'Downloading Direct Data file: {name}',
                    context=None)
        try:
            request: DirectDataRequest = self._vault_client.new_request(DirectDataRequest)
            response: VaultResponse = request.download_direct_data_file(
                name=name, filepart=filepart)

            if response.has_errors():
                raise Exception(response.errors[0].message)

            log_message(log_level='Info',
                        message=f'Direct Data file: {name} downloaded successfully',
                        context=None)

            return response

        except Exception as e:
            log_message(log_level='Error',
                        message=f'Exception when downloading Direct Data file',
                        exception=e,
                        context=None)
            raise e
