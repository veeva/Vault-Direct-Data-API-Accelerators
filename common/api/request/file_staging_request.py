"""
Module that defines classes used to send File Staging requests to the Vault API.
"""

from enum import Enum

from ..connector import http_request_connector
from ..connector.http_request_connector import HttpMethod
from ..model.response.file_staging_response import FileStagingItemBulkResponse
from ..model.response.file_staging_response import FileStagingItemResponse
from ..model.response.file_staging_response import FileStagingJobResponse
from ..model.response.file_staging_response import FileStagingSessionBulkResponse
from ..model.response.file_staging_response import FileStagingSessionPartBulkResponse
from ..model.response.file_staging_response import FileStagingSessionPartResponse
from ..model.response.file_staging_response import FileStagingSessionResponse
from ..model.response.vault_response import VaultResponse
from ..request.vault_request import VaultRequest, _RequestOption, _ResponseOption


class Kind(Enum):
    """
    Enumeration class representing different file staging items.

    Attributes:
        FILE (str): File
        FOLDER (str): Folder
    """

    FILE: str = 'file'
    FOLDER: str = 'folder'


class FileStagingRequest(VaultRequest):
    """
    Class that defines methods used to call File Staging endpoints.

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.2/#file-staging](https://developer.veevavault.com/api/24.2/#file-staging)
    """

    _HTTP_HEADER_FILE_PART_NUMBER: str = "X-VaultAPI-FilePartNumber"

    _URL_LIST_ITEMS_AT_A_PATH: str = "/services/file_staging/items/{item}"
    _URL_DOWNLOAD_ITEM_CONTENT: str = "/services/file_staging/items/content/{item}"
    _URL_FILE_STAGING_CREATE_FILE_OR_FOLDER: str = "/services/file_staging/items"
    _URL_FILE_STAGING_UPDATE_OR_DELETE_FILE_OR_FOLDER: str = "/services/file_staging/items/{item}";
    _URL_FILE_STAGING_CREATE_RESUMABLE_UPLOAD_SESSION: str = "/services/file_staging/upload"
    _URL_FILE_STAGING_RESUMABLE_SESSION: str = "/services/file_staging/upload/{upload_session_id}"
    _URL_FILE_STAGING_RESUMABLE_SESSION_PARTS: str = "/services/file_staging/upload/{upload_session_id}/parts"

    _RECURSIVE_PARAMETER: str = "recursive"
    _LIMIT_PARAMETER: str = "limit"
    _FORMAT_RESULT_PARAMETER: str = "format_result"
    _KIND_PARAMETER: str = "kind"
    _PATH_PARAMETER: str = "path"
    _OVERWRITE_PARAMETER: str = "overwrite"
    _FILE_PARAMETER: str = "file"
    _PARENT_PARAMETER: str = "parent"
    _NAME_PARAMETER: str = "name"
    _SIZE_PARAMETER: str = "size"
    _CHUNK_PARAMETER: str = "@/chunk-ab."

    def list_items_at_a_path(self, item: str = '',
                             recursive: bool = None,
                             limit: int = None,
                             format_result: str = None) -> FileStagingItemBulkResponse:
        """
        **List Items at a Path**

        Return a list of files and folders for the specified path.
        Paths are different for Admin users (Vault Owners and System Admins) and non-Admin users.

        Args:
            item (str): Path to the file or folder
            recursive (bool): If true, the response will contain the contents of all subfolders. If not specified, the default value is false
            limit (int): The maximum number of items per page in the response. This can be any value between 1 and 1000. If omitted, the default value is 1000
            format_result (str): If set to csv, the response includes a job_id. Use the Job ID value to retrieve the status and results of the request

        Returns:
            FileStagingItemBulkResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/services/file_staging/items/{item}

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.2/#list-items-at-a-path](https://developer.veevavault.com/api/24.2/#list-items-at-a-path)

        Example:
            ```python
            # Example Request
            user_folder: str = 'u1234567'
            request: FileStagingRequest = vault_client.new_request(request_class=FileStagingRequest)
            response: FileStagingItemBulkResponse = request.list_items_at_a_path(recursive=True, item=user_folder)

            # Example Response
            for item in response.data:
                print('-----Item-----')
                print(f'Name: {item.name}')
                print(f'Kind: {item.kind}')
                print(f'Path: {item.path}')
            ```
        """

        endpoint = self.get_api_endpoint(endpoint=self._URL_LIST_ITEMS_AT_A_PATH)
        endpoint = endpoint.replace('{item}', item)

        if recursive is not None:
            self._add_query_param(self._RECURSIVE_PARAMETER, recursive)

        if limit is not None:
            self._add_query_param(self._LIMIT_PARAMETER, limit)

        if format_result is not None:
            self._add_query_param(self._FORMAT_RESULT_PARAMETER, format_result)

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=FileStagingItemBulkResponse)

    def list_items_at_a_path_by_page(self, page_url: str) -> FileStagingItemBulkResponse:
        """
        **List Items at a Path by Page**

        Return a list of files and folders for the specified page url.

        Args:
            page_url (str): full path to the page (including https://{vaultDNS}/api/{version}/)

        Returns:
            FileStagingItemBulkResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/services/file_staging/items/{item}

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.2/#list-items-at-a-path](https://developer.veevavault.com/api/24.2/#list-items-at-a-path)

        Example:
            ```python
            # Example Request
            next_page_url: str = list_response.responseDetails.next_page
            request: FileStagingRequest = vault_client.new_request(request_class=FileStagingRequest)
            response: FileStagingItemBulkResponse = request.list_items_at_a_path_by_page(next_page_url)

            # Example Response
            for item in response.data:
                print('-----Item-----')
                print(f'Name: {item.name}')
                print(f'Kind: {item.kind}')
                print(f'Path: {item.path}')
            ```
        """

        endpoint = self.get_pagination_endpoint(page_url)

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=FileStagingItemBulkResponse)

    def download_item_content(self, item: str = '',
                              byte_range: str = None) -> VaultResponse:
        """
        **Download Item Content**

        Retrieve the content of a specified file from the file staging server. Use the Range header to create resumable downloads for large files,
        or to continue downloading a file if your session is interrupted.

        Args:
            item (str): Path to the file
            byte_range (str): Specifies a partial range of bytes to include in the download

        Returns:
            VaultResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/services/file_staging/items/content/{item}

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.2/#download-item-content](https://developer.veevavault.com/api/24.2/#download-item-content)

        Example:
            ```python
            # Example Request
            file_path: str = "u1234567/test_document.docx"
            request: FileStagingRequest = vault_client.new_request(request_class=FileStagingRequest)
            response = request.download_item_content(item=file_path)

            # Example Response
            print(f'Size: {len(response.binary_content)}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOWNLOAD_ITEM_CONTENT)
        endpoint = endpoint.replace('{item}', item)

        if byte_range is not None:
            self._add_header_param(http_request_connector.HTTP_HEADER_RANGE, byte_range)

        self._response_option = _ResponseOption.BYTES

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=VaultResponse)

    def create_folder_or_file(self, kind: Kind,
                              path: str,
                              overwrite: bool = None,
                              input_path: str = None,
                              content_md5: str = None) -> FileStagingItemResponse:
        """
        **Create Folder or File**

        Upload files or folders up to 50MB to the File Staging Server.

        Args:
            kind (Kind): a Kind enum value representing the type of the item. Can be either FILE or FOLDER type
            path (str): The absolute path, including file or folder name, to place the item in the file staging server
            overwrite (bool): If set to true, Vault will overwrite any existing files with the same name at the specified destination. For folders, this is always false
            input_path (str): Path to the file or folder to upload
            content_md5 (str): The MD5 checksum of the file being uploaded

        Returns:
            FileStagingItemResponse: Modeled response from Vault

        Vault API Endpoint:
            POST /api/{version}/services/file_staging/items

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.2/#create-folder-or-file](https://developer.veevavault.com/api/24.2/#create-folder-or-file)

        Example:
            ```python
            # Example Request
            test_folder: str = 'u1234567/test_create_folder'
            request: FileStagingRequest = vault_client.new_request(request_class=FileStagingRequest)
            response: FileStagingItemResponse = request.create_folder_or_file(kind=Kind.FOLDER,
                                                                              path=test_folder)

            # Example Response
            item: FileStagingItem = response.data
            print(f'Name: {item.name}')
            print(f'Kind: {item.kind}')
            print(f'Path: {item.path}')
            ```
        """

        endpoint = self.get_api_endpoint(endpoint=self._URL_FILE_STAGING_CREATE_FILE_OR_FOLDER)

        self._add_body_param(self._KIND_PARAMETER, kind.value)
        self._add_body_param(self._PATH_PARAMETER, path)

        if overwrite is not None:
            self._add_body_param(self._OVERWRITE_PARAMETER, overwrite)

        if content_md5 is not None:
            self._add_header_param(http_request_connector.HTTP_HEADER_CONTENT_MD5, content_md5)

        if input_path is not None:
            self._add_file_multipart(self._FILE_PARAMETER, input_path)

        return self._send(http_method=HttpMethod.POST,
                          url=endpoint,
                          response_class=FileStagingItemResponse)

    def update_folder_or_file(self, item: str,
                              parent: str = None,
                              name: str = None) -> FileStagingJobResponse:
        """
        **Update Folder or File**

        Move or rename a folder or file on the file staging server. You can move and rename an item in the same request.

        Args:
            item (str): The absolute path to a file or folder. This path is specific to the authenticated user. Admin users can access the root directory. All other users can only access their own user directory
            parent (str): When moving a file or folder, specifies the absolute path to the parent directory in which to place the file
            name (str): When renaming a file or folder, specifies the new name

        Returns:
            FileStagingJobResponse: Modeled response from Vault

        Vault API Endpoint:
            PUT /api/{version}/services/file_staging/items/{item}

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.2/#update-folder-or-file](https://developer.veevavault.com/api/24.2/#update-folder-or-file)

        Example:
            ```python
            # Example Request
            current_folder_path: str = 'u1234567/test_create_folder'
            new_folder_name: str = 'test_update_folder'
            request: FileStagingRequest = vault_client.new_request(request_class=FileStagingRequest)
            response: FileStagingJobResponse = request.update_folder_or_file(item=current_folder_path,
                                                                             name=new_folder_name)

            # Example Response
            print(f'Job ID: {response.data.job_id}')
            print(f'URL: {response.data.url}')
            ```
        """

        endpoint = self.get_api_endpoint(endpoint=self._URL_FILE_STAGING_UPDATE_OR_DELETE_FILE_OR_FOLDER)
        endpoint = endpoint.replace('{item}', item)

        if parent is not None:
            self._add_body_param(self._PARENT_PARAMETER, parent)

        if name is not None:
            self._add_body_param(self._NAME_PARAMETER, name)

        return self._send(http_method=HttpMethod.PUT,
                          url=endpoint,
                          response_class=FileStagingJobResponse)

    def delete_folder_or_file(self, item: str,
                              recursive: bool = None) -> FileStagingJobResponse:
        """
        **Delete Folder or File**

        Delete an individual file or folder from the file staging server.

        Args:
            item (str): The absolute path to a file or folder. This path is specific to the authenticated user. Admin users can access the root directory. All other users can only access their own user directory
            recursive (bool): Applicable to deleting folders only. If true, the request will delete the contents of a folder and all subfolders. The default is false

        Returns:
            FileStagingJobResponse: Modeled response from Vault

        Vault API Endpoint:
            DELETE /api/{version}/services/file_staging/items/{item}

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.2/#delete-file-or-folder](https://developer.veevavault.com/api/24.2/#delete-file-or-folder)

        Example:
            ```python
            # Example Request
            folder_path: str = 'u1234567/test_create_folder'
            request: FileStagingRequest = vault_client.new_request(request_class=FileStagingRequest)
            response: FileStagingJobResponse = request.delete_folder_or_file(folder_path)

            # Example Response
            print(f'Job ID: {response.data.job_id}')
            print(f'URL: {response.data.url}')
            ```
        """

        endpoint = self.get_api_endpoint(endpoint=self._URL_FILE_STAGING_UPDATE_OR_DELETE_FILE_OR_FOLDER)
        endpoint = endpoint.replace('{item}', item)

        if recursive is not None:
            self._add_query_param(self._RECURSIVE_PARAMETER, recursive)

        return self._send(http_method=HttpMethod.DELETE,
                          url=endpoint,
                          response_class=FileStagingJobResponse)

    def create_resumable_upload_session(self, path: str,
                                        size: int,
                                        overwrite: bool = None) -> FileStagingSessionResponse:
        """
        **Create Resumable Upload Session**

        Initiate a multipart upload session and return an upload session ID.

        Args:
            path (str): The absolute path, including file name, to place the file in the file staging server
            size (int): The size of the file in bytes
            overwrite (bool): If set to true, Vault will overwrite any existing files with the same name at the specified destination

        Returns:
            FileStagingSessionResponse: Modeled response from Vault

        Vault API Endpoint:
            POST /api/{version}/services/file_staging/upload

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.2/#create-resumable-upload-session](https://developer.veevavault.com/api/24.2/#create-resumable-upload-session)

        Example:
            ```python
            # Example Request
            local_file_path: str = 'path/to/file.txt'
            file_size: int = os.path.getsize(local_file_path)
            file_staging_path: str = 'u1234567/vapil_test_document.docx'
            request: FileStagingRequest = vault_client.new_request(request_class=FileStagingRequest)
            response: FileStagingSessionResponse = request.create_resumable_upload_session(path=file_staging_path,
                                                                                           size=file_size,
                                                                                           overwrite=True)

            # Example Response
            print(f'ID: {response.data.id}')
            print(f'Path: {response.data.path}')
            ```
        """

        endpoint = self.get_api_endpoint(endpoint=self._URL_FILE_STAGING_CREATE_RESUMABLE_UPLOAD_SESSION)

        self._add_body_param(self._PATH_PARAMETER, path)
        self._add_body_param(self._SIZE_PARAMETER, size)

        if overwrite is not None:
            self._add_body_param(self._OVERWRITE_PARAMETER, overwrite)

        return self._send(http_method=HttpMethod.POST,
                          url=endpoint,
                          response_class=FileStagingSessionResponse)

    def upload_to_a_session(self, upload_session_id: str,
                            part_number: str,
                            content_md5: str = None,
                            file_path: str = None) -> FileStagingSessionPartResponse:
        """
        **Upload to a Session**

        The session owner can upload parts of a file to an active upload session.
        By default, you can upload up to 2000 parts per upload session, and each part can be up to 50MB.
        Use the Range header to specify the range of bytes for each upload, or split files into parts and add each part as a separate file.
        Each part must be the same size, except for the last part in the upload session.

        Args:
            upload_session_id (str): The upload session ID
            part_number (str): The part number of the file being uploaded
            content_md5 (str): The MD5 checksum of the file being uploaded
            file_path (str): Path to the file to upload

        Returns:
            FileStagingSessionPartResponse: Modeled response from Vault

        Vault API Endpoint:
            PUT /api/{version}/services/file_staging/upload/{upload_session_id}

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.2/#upload-to-a-session](https://developer.veevavault.com/api/24.2/#upload-to-a-session)

        Example:
            ```python
            # Example Request
            request: FileStagingRequest = vault_client.new_request(request_class=FileStagingRequest)
            response: FileStagingSessionPartResponse = request.upload_to_a_session(upload_session_id=upload_session_id,
                                                                                   part_number='1',
                                                                                   file_path=file_path)

            # Example Response
            print(f'Size: {response.data.size}')
            print(f'MD5: {response.data.part_content_md5}')
            ```
        """

        endpoint = self.get_api_endpoint(endpoint=self._URL_FILE_STAGING_RESUMABLE_SESSION)
        endpoint = endpoint.replace('{upload_session_id}', upload_session_id)

        self._add_header_param(http_request_connector.HTTP_HEADER_CONTENT_TYPE,
                               http_request_connector.HTTP_CONTENT_TYPE_OCTET)
        self._add_header_param(self._HTTP_HEADER_FILE_PART_NUMBER, part_number)

        if content_md5 is not None:
            self._add_header_param(http_request_connector.HTTP_HEADER_CONTENT_MD5, content_md5)

        if file_path is not None:
            binary_content = None
            with open(file_path, 'rb') as file:
                binary_content = file.read()
            self._binary_content = binary_content
            self._request_option = _RequestOption.BYTES
            self._add_header_param(http_request_connector.HTTP_HEADER_CONTENT_LENGTH, str(len(binary_content)))

        return self._send(http_method=HttpMethod.PUT,
                          url=endpoint,
                          response_class=FileStagingSessionPartResponse)

    def commit_upload_session(self, upload_session_id: str) -> FileStagingJobResponse:
        """
        **Commit Upload Session**

        Mark an upload session as complete and assemble all previously uploaded parts to create a file.

        Args:
            upload_session_id (str): The upload session ID

        Returns:
            FileStagingJobResponse: Modeled response from Vault

        Vault API Endpoint:
            POST /api/{version}/services/file_staging/upload/{upload_session_id}

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.2/#commit-upload-session](https://developer.veevavault.com/api/24.2/#commit-upload-session)

        Example:
            ```python
            # Example Request
            request: FileStagingRequest = vault_client.new_request(request_class=FileStagingRequest)
            response: FileStagingJobResponse = request.commit_upload_session(upload_session_id=upload_session_id)

            # Example Response
            print(f'Job ID: {response.data.job_id}')
            ```
        """

        endpoint = self.get_api_endpoint(endpoint=self._URL_FILE_STAGING_RESUMABLE_SESSION)
        endpoint = endpoint.replace('{upload_session_id}', upload_session_id)

        return self._send(http_method=HttpMethod.POST,
                          url=endpoint,
                          response_class=FileStagingJobResponse)

    def abort_upload_session(self, upload_session_id: str) -> VaultResponse:
        """
        **Abort Upload Session**

        Abort an active upload session and purge all uploaded file parts. Admin users can see and abort all upload sessions,
        while non-Admin users can only see and abort sessions where they are the owner.

        Args:
            upload_session_id (str): The upload session ID

        Returns:
            VaultResponse: Modeled response from Vault

        Vault API Endpoint:
            DELETE /api/{version}/services/file_staging/upload/{upload_session_id}

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.2/#abort-upload-session](https://developer.veevavault.com/api/24.2/#abort-upload-session)

        Example:
            ```python
            # Example Request
            request: FileStagingRequest = vault_client.new_request(request_class=FileStagingRequest)
            response: VaultResponse = request.abort_upload_session(upload_session_id=upload_session_id)

            # Example Response
            print(f'Response Status: {response.responseStatus}')
            ```
        """

        endpoint = self.get_api_endpoint(endpoint=self._URL_FILE_STAGING_RESUMABLE_SESSION)
        endpoint = endpoint.replace('{upload_session_id}', upload_session_id)

        return self._send(http_method=HttpMethod.DELETE,
                          url=endpoint,
                          response_class=VaultResponse)

    def list_upload_sessions(self) -> FileStagingSessionBulkResponse:
        """
        **List Upload Sessions**

        Return a list of active upload sessions.

        Returns:
            FileStagingSessionBulkResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/services/file_staging/upload

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.2/#list-upload-sessions](https://developer.veevavault.com/api/24.2/#list-upload-sessions)

        Example:
            ```python
            # Example Request
            request: FileStagingRequest = vault_client.new_request(request_class=FileStagingRequest)
            response: FileStagingSessionBulkResponse = request.list_upload_sessions()

            # Example Response
            for session in response.data:
                print('-----Session-----')
                print(f'ID: {session.id}')
                print(f'Path: {session.path}')
            ```
        """

        endpoint = self.get_api_endpoint(endpoint=self._URL_FILE_STAGING_CREATE_RESUMABLE_UPLOAD_SESSION)

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=FileStagingSessionBulkResponse)

    def list_upload_sessions_by_page(self, page_url: str) -> FileStagingSessionBulkResponse:
        """
        **List Upload Sessions by Page**

        Return a list of active upload sessions using the previous_page or next_page parameter of a previous request.

        Args:
            page_url (str): full path to the page (including https://{vaultDNS}/api/{version}/)

        Returns:
            FileStagingSessionBulkResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/services/file_staging/upload

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.2/#list-upload-sessions](https://developer.veevavault.com/api/24.2/#list-upload-sessions)

        Example:
            ```python
            # Example Request

            # Example Response

            ```
        """

        endpoint = self.get_pagination_endpoint(page_url)

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=FileStagingSessionBulkResponse)

    def get_upload_session_details(self, upload_session_id: str) -> FileStagingSessionResponse:
        """
        **Get Upload Session Details**

        Retrieve the details of an active upload session. Admin users can get details for all sessions,
        while non-Admin users can only get details for sessions if they are the owner.

        Args:
            upload_session_id (str): The upload session ID

        Returns:
            FileStagingSessionResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/services/file_staging/upload/{upload_session_id}

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.2/#get-upload-session-details](https://developer.veevavault.com/api/24.2/#get-upload-session-details)

        Example:
            ```python
            # Example Request
            request: FileStagingRequest = vault_client.new_request(request_class=FileStagingRequest)
            response: FileStagingSessionResponse = request.get_upload_session_details(upload_session_id=upload_session_id)

            # Example Response
            print(f'ID: {response.data.id}')
            print(f'Path: {response.data.path}')
            print(f'Uploaded Parts: {response.data.uploaded_parts}')
            ```
        """

        endpoint = self.get_api_endpoint(endpoint=self._URL_FILE_STAGING_RESUMABLE_SESSION)
        endpoint = endpoint.replace('{upload_session_id}', upload_session_id)

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=FileStagingSessionResponse)

    def list_file_parts_uploaded_to_a_session(self, upload_session_id: str,
                                              limit: int = None) -> FileStagingSessionPartBulkResponse:
        """
        **List File Parts Uploaded to a Session**

        Return a list of parts uploaded in a session. You must be an Admin user or the session owner.

        Args:
            upload_session_id (str): The upload session ID
            limit (int): The maximum number of items per page in the response. This can be any value between 1 and 1000. If omitted, the default value is 1000

        Returns:
            FileStagingSessionPartBulkResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/services/file_staging/upload/{upload_session_id}/parts

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.2/#list-file-parts-uploaded-to-session](https://developer.veevavault.com/api/24.2/#list-file-parts-uploaded-to-session)

        Example:
            ```python
            # Example Request
            request: FileStagingRequest = vault_client.new_request(request_class=FileStagingRequest)
            response: FileStagingSessionPartBulkResponse = request.list_file_parts_uploaded_to_a_session(upload_session_id)

            # Example Response
            for part in response.data:
                print('-----Part-----')
                print(f'Part Number: {part.part_number}')
                print(f'Size: {part.size}')
                print(f'MD5: {part.part_content_md5}')
            ```
        """

        endpoint = self.get_api_endpoint(endpoint=self._URL_FILE_STAGING_RESUMABLE_SESSION_PARTS)
        endpoint = endpoint.replace('{upload_session_id}', upload_session_id)

        if limit is not None:
            self._add_query_param(self._LIMIT_PARAMETER, limit)

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=FileStagingSessionPartBulkResponse)
