"""
Module that defines classes used to send Vault API requests.
"""

import json
from abc import ABC
from enum import Enum
from typing import Any, Dict

from pydantic.dataclasses import dataclass
from pydantic.fields import Field

from ..connector import http_request_connector


class _ParamType(Enum):
    # Enumeration class representing different types of parameters in a Vault request.
    #
    # Attributes:
    #    QUERY (ParamType): Represents a query parameter
    #    HEADER (ParamType): Represents a header parameter
    #    BODY (ParamType): Represents a body parameter

    QUERY = 'QUERY'
    HEADER = 'HEADER'
    BODY = 'BODY'


class _RequestOption(Enum):
    # Enumeration class representing different options for handling request data in a Vault request.
    #
    # Attributes:
    #     EMPTY (RequestOption): Represents an empty request option
    #     BYTES (RequestOption): Represents a request option for handling raw bytes

    EMPTY = 'EMPTY'
    BYTES = 'BYTES'
    STRING = 'STRING'


class _ResponseOption(Enum):
    # Enumeration class representing different options for handling request data in a Vault request.
    #
    # Attributes:
    #     EMPTY (RequestOption): Represents an empty request option
    #     BYTES (RequestOption): Represents a request option for handling raw bytes

    STRING = 'STRING'
    TO_FILE = 'TO_FILE'
    BYTES = 'BYTES'


@dataclass
class VaultRequest(ABC):
    """
    Abstract Class that is extended by request classes that map to a Vault
    API endpoint. Used for performing HTTP requests to the Vault API.

    Attributes:
        VAULT_API_VERSION (str): This variable drives the version used in all API calls
        HTTP_HEADER_AUTHORIZATION (str): HTTP header key for Authorization
        HTTP_HEADER_VAULT_CLIENT_ID (str): HTTP header key for Vault Client ID
        HTTP_HEADER_REFERENCE_ID (str): HTTP header key for Reference ID
        reference_id (str): The Reference ID Header to be used in the request. When set in the request,
            the Reference ID is returned in the response headers of the returned Response class.
    """

    VAULT_API_VERSION: str = 'v24.2'
    HTTP_HEADER_AUTHORIZATION: str = 'Authorization'
    HTTP_HEADER_VAULT_CLIENT_ID: str = 'X-VaultAPI-ClientID'
    HTTP_HEADER_REFERENCE_ID: str = "X-VaultAPI-ReferenceId"
    reference_id: str = None

    _header_params: Dict[str, Any] = Field(default_factory=dict, alias="header_params")
    _body_params: Dict[Any, Any] = Field(default_factory=dict, alias="body_params")
    _query_params: Dict[str, Any] = Field(default_factory=dict, alias="query_params")
    _file_params: Dict[str, Any] = Field(default_factory=dict, alias="file_params")
    _request_raw_string: str = Field(default=None, alias="request_raw_string")
    _binary_content: bytes = Field(default=None, alias="binary_content")
    _vault_dns: str = Field(default=None, alias="vault_dns")
    _vault_username: str = Field(default=None, alias="vault_username")
    _vault_password: str = Field(default=None, alias="vault_password")
    _vault_client_id: str = Field(default=None, alias="vault_client_id")
    _http_timeout: int = Field(default=60, alias="http_timeout")
    _set_log_api_errors: bool = Field(default=True, alias="set_log_api_errors")
    _idp_oauth_access_token: str = Field(default=None, alias="idp_oauth_access_token")
    _idp_oauth_scope: str = Field(default='openid', alias="idp_oauth_scope")
    _idp_password: str = Field(default=None, alias="idp_password")
    _idp_username: str = Field(default=None, alias="idp_username")
    _idp_client_id: str = Field(default=None, alias="idp_client_id")
    _set_validate_session: bool = Field(default=True, alias="set_validate_session")
    _vault_oauth_client_id: str = Field(default=None, alias="vault_oauth_client_id")
    _vault_oauth_profile_id: str = Field(default=None, alias="vault_oauth_profile_id")
    _vault_session_id: str = Field(default=None, alias="vault_session_id")
    _request_option: _RequestOption = Field(default=_RequestOption.EMPTY, alias="request_option")

    def _send(self, http_method: http_request_connector.HttpMethod,
              url: str,
              response_class: Any) -> Any:
        # Send an HTTP request with standard Vault information set, such as the session id.
        # This method acts as a wrapper to http_request_connector, providing a central
        # location for setting information and processing the response.
        #
        # Args:
        #     http_method (http_request_connector.HttpMethod): The HTTP method for the request
        #     url (str): The URL for the HTTP request
        #     response_class (Any): The class to use for deserializing the response
        #
        # Returns:
        #     Any: The processed response as a Response Class from the HTTP request

        self._set_vault_header_params()
        body = self._get_request_body()
        response_dict = http_request_connector.send(http_method=http_method,
                                                    url=url,
                                                    query_params=self._query_params,
                                                    body=body,
                                                    headers=self._header_params,
                                                    files=self._file_params)
        return self._process_response(response_dict=response_dict, response_class=response_class)

    def _set_vault_header_params(self):
        # Set the HTTP header with standard Vault header parameters

        # Add the vault session id if it exists
        if self._vault_session_id is not None and self._vault_session_id != '':
            self._header_params[self.HTTP_HEADER_AUTHORIZATION] = self._vault_session_id

        if self._vault_client_id is not None and self._vault_client_id != '':
            self._header_params[self.HTTP_HEADER_VAULT_CLIENT_ID] = self._vault_client_id

        if self.reference_id is not None and self.reference_id != '':
            self._header_params[self.HTTP_HEADER_REFERENCE_ID] = self.reference_id

    @staticmethod
    def _process_response(response_dict: Dict[str, str], response_class: Any) -> Any:
        # Deserialize the JSON response from the HTTP request to Python Response object.
        #
        # Args:
        #     response_dict (Dict[str, str]): The response dictionary from the HTTP request
        #     response_class (Any): The class to use for deserializing the response
        #
        # Returns:
        #     Any: The processed response as a Response Class

        if http_request_connector.HTTP_CONTENT_TYPE_OCTET in response_dict['content_type']:
            response_object = response_class(binary_content=response_dict['binary_content'])
            response_object.binary_content = response_dict['binary_content']
        else:
            data = json.loads(response_dict['response'])
            response_object = response_class(**data)
            response_object.response = response_dict['response']
        response_object.headers = dict(response_dict['headers'])
        return response_object

    def _add_header_param(self, key: str, value: Any):
        # Add a header param name/value pair to the request.
        #
        # Args:
        #     key (str): The name of the parameter
        #     value (Any): The value of the parameter
        self._add_param(param_type=_ParamType.HEADER, key=key, value=value)

    def _add_body_param(self, key: str, value: Any):
        # Add a body param name/value pair to the request.
        #
        # Args:
        #     key (str): The name of the parameter
        #     value (Any): The value of the parameter
        self._add_param(param_type=_ParamType.BODY, key=key, value=value)

    def _add_query_param(self, key: str, value: Any):
        # Add a query param name/value pair to the request.
        #
        # Args:
        #     key (str): The name of the parameter
        #     value (Any): The value of the parameter
        self._add_param(param_type=_ParamType.QUERY, key=key, value=value)

    def _add_raw_string(self, raw_string: str):
        # Add a string to the request, such as POST of raw data
        #
        # Args:
        #     raw_string (str): The raw string to be included in the request

        self._request_option = _RequestOption.STRING
        self._request_raw_string = raw_string

    def _add_file_multipart(self, param_name: str, file_path: str):
        # Add a file parameter for multipart/form-data in the request.
        #
        # Args:
        #     param_name (str): The name of the file parameter
        #     file_path (str): The path to the file to be included

        self._file_params[param_name] = file_path

    def _add_param(self, param_type: _ParamType, key: str, value: Any):
        # Add a parameter to the request.
        #
        # Args:
        #     param_type (_ParamType): The type of parameter to be added
        #     key (str): The name of the parameter
        #     value (Any): The value of the parameter

        if param_type == _ParamType.HEADER:
            self._header_params[key] = value
        elif param_type == _ParamType.BODY:
            self._body_params[key] = value
        elif param_type == _ParamType.QUERY:
            self._query_params[key] = value

    def get_api_endpoint(self, endpoint: str, include_version: bool = True) -> str:
        """
        Get a fully formed API URL consisting of the Vault DNS,
        API version, and the API endpoint.

        Args:
            endpoint (str): API endpoint in the form "/objects/documents"
            include_version (bool): Whether to include the API version in the URL

        Returns:
            str: URL for the API endpoint in the form:\n
                 - Include version: "https://myvault.com/api/{version}/objects/documents"\n
                 - Exclude version: "https://myvault.com/api/mdl/components"
        """

        if include_version:
            return f"{self.get_vault_url()}/api/{self.VAULT_API_VERSION}{endpoint}"
        else:
            return f"{self.get_vault_url()}/api{endpoint}"

    def get_vault_url(self) -> str:
        """
        Returns the Vault URL in format "https://myvault.veevavault.com".

        Returns:
            str: The Vault URL
        """

        return 'https://' + self._vault_dns

    def get_pagination_endpoint(self, page_url: str) -> str:
        """
        Get a fully formed API URL consisting of the Vault DNS, API version, and the API endpoint

        Args:
          page_url (str): The URL from the previous_page or next_page parameter

        Returns:
          str: URL for the API endpoint in form https://myvault.com/api/{version}/objects/documents
        """

        if page_url.startswith(f"https://{self._vault_dns}"):
            return page_url

        if page_url.startswith(f"/api/{self.VAULT_API_VERSION}"):
            return f"https://{self._vault_dns}{page_url}"

        if page_url.startswith("/api/"):
            return self.get_api_endpoint(page_url[5:], False)

        return self.get_api_endpoint(endpoint=page_url)

    def _get_request_body(self) -> Any:
        # Form the request body based on the set request option
        # and set properties of the class.
        #
        # Returns:
        #     Any: The request body

        if self._request_option == _RequestOption.EMPTY:
            return self._body_params
        elif self._request_option == _RequestOption.BYTES:
            return self._binary_content
        elif self._request_option == _RequestOption.STRING:
            return self._request_raw_string
        else:
            return None
