"""
Module that defines classes used to send Direct Data requests to the Vault API.
"""
from enum import Enum

from ..connector.http_request_connector import HttpMethod
from ..model.response.direct_data_response import DirectDataResponse
from ..model.response.vault_response import VaultResponse
from ..request.vault_request import VaultRequest


class ExtractType(Enum):
    """
    Enumeration class representing different direct data api extract types.

    Attributes:
        FULL (str): Full extract
        INCREMENTAL (str): Incremental extract
        LOG (str): Log extract
    """

    FULL: str = 'full_directdata'
    INCREMENTAL: str = 'incremental_directdata'
    LOG: str = 'log_directdata'


class DirectDataRequest(VaultRequest):
    """
    Class that defines methods used to call Direct Data endpoints.

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.1/#retrieve-available-direct-data-files](https://developer.veevavault.com/api/24.1/#retrieve-available-direct-data-files)
        [https://developer.veevavault.com/api/24.1/#download-direct-data-file](https://developer.veevavault.com/api/24.1/#download-direct-data-file)
    """

    _URL_LIST_ITEMS: str = '/services/directdata/files'
    _URL_DOWNLOAD_ITEM: str = "/services/directdata/files/{name}"

    _EXTRACT_TYPE_PARAMETER: str = 'extract_type'
    _START_TIME_PARAMETER: str = 'start_time'
    _STOP_TIME_PARAMETER: str = 'stop_time'

    def retrieve_available_direct_data_files(self, extract_type: ExtractType = None,
                                             start_time: str = None,
                                             stop_time: str = None) -> DirectDataResponse:
        """
        **Retrieve Available Direct Data Files**

        Retrieve a list of all Direct Data files available for download.

        Args:
            extract_type: Enum value representing the extract type. Can be either FULL or INCREMENTAL
            start_time: Start time
            stop_time: Stop time

        Returns:
          DirectDataResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/services/directdata/files

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#retrieve-available-direct-data-files](https://developer.veevavault.com/api/24.1/#retrieve-available-direct-data-files)

        Example:
            ```python
            # Example Request
            request: DirectDataRequest = vault_client.new_request(DirectDataRequest)
            response: DirectDataResponse = request.list_items(extract_type=ExtractType.INCREMENTAL,
                                                              start_time=start_time,
                                                              stop_time=stop_time)

            for item in response.data:
                print('-----Item-----')
                print(f'Name: {item.name}')
                print(f'Filename: {item.filename}')
                print(f'Extract Type: {item.extract_type}')
                print(f'Fileparts: {item.fileparts}')

                for filepart in item.filepart_details:
                    print('-----File part-----')
                    print(f'Name: {filepart.name}')
                    print(f'Filename: {filepart.filename}')
                    print(f'Filepart: {filepart.filepart}')
                    print(f'Size: {filepart.size}')
                    print(f'URL: {filepart.url}')
            ```
        """

        endpoint = self.get_api_endpoint(endpoint=self._URL_LIST_ITEMS)

        if extract_type is not None:
            self._add_query_param(self._EXTRACT_TYPE_PARAMETER, extract_type.value)

        if start_time is not None:
            self._add_query_param(self._START_TIME_PARAMETER, start_time)

        if stop_time is not None:
            self._add_query_param(self._STOP_TIME_PARAMETER, stop_time)

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=DirectDataResponse)

    def download_direct_data_file(self, name: str,
                                  filepart: int = None) -> VaultResponse:
        """
        **Download Direct Data File**

        Download a Direct Data file.

        Args:
            name: Name of the file to download
            filepart: Filepart number to download

        Returns:
          VaultResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/services/directdata/files/{name}

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#download-direct-data-file](https://developer.veevavault.com/api/24.1/#download-direct-data-file)

        Example:
            ```python
            # Example Request
            request: DirectDataRequest = vault_client.new_request(DirectDataRequest)
            response: VaultResponse = request.download_item(name=name)

            # Example Response
            print(f'Size: {len(response.binary_content)}')
            ```
        """

        endpoint = self.get_api_endpoint(endpoint=self._URL_DOWNLOAD_ITEM)
        endpoint = endpoint.replace('{name}', name)

        self._add_query_param('filepart', filepart)

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=VaultResponse)
