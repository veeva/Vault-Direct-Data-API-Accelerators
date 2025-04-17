"""
Module that defines the VaultClient class, which is used for all API requests.
"""

import json
import logging
from enum import Enum
from typing import Any, Optional

from pydantic.dataclasses import dataclass

from ..model.response import vault_response
from ..model.response.authentication_response import AuthenticationResponse
from ..request.vault_request import VaultRequest
from ..request.authentication_request import AuthenticationRequest

_LOGGER: logging.Logger = logging.getLogger(__name__)
_URL_LOGIN: str = 'login.veevavault.com'


class AuthenticationType(Enum):
    """
    Type of Authentication with the Vault API

    Attributes:
        BASIC (AuthenticationType): Vault username and password
        OAUTH_ACCESS_TOKEN (AuthenticationType): OAuth OpenID Connect with IDP Access Token
        OAUTH_DISCOVERY (AuthenticationType): OAuth OpenID Connect with Vault Client Discovery
        SESSION_ID (AuthenticationType): Existing Vault session ID
    """

    BASIC = 'BASIC'
    OAUTH_ACCESS_TOKEN = 'OAUTH_ACCESS_TOKEN'
    OAUTH_DISCOVERY = 'OAUTH_DISCOVERY'
    SESSION_ID = 'SESSION_ID'


@dataclass
class VaultClient:
    """
    Base class for all Vault integration calls where a Vault session is established via:

    1. Basic authentication using Vault username and password
    2. Existing Vault session, such as a session passed from Vault Web Action/Tab or Vault Job Scheduler
    3. OAuth

    New API requests are created via the "new_request" method, passing in the request class to instantiate.

    Attributes:
        vault_dns (str): The Vault's DNS address
        vault_username (str): Vault username for authentication
        vault_password (str): Vault password for authentication
        vault_client_id (str): Vault Client ID for Vault authentication
        http_timeout (int): Timeout duration for the HTTP request in seconds. Default=60
        authentication_type (AuthenticationType): Type of authentication to use
        set_log_api_errors (bool): Flag to indicate whether to log API errors. Default=True
        idp_oauth_access_token (str): Access token for Identity Provider (IDP) OAuth
        idp_oauth_scope (str): Scope for IDP OAuth. Default=openid
        idp_password (str): Password for IDP authentication
        idp_username (str): Username for IDP authentication
        set_validate_session (bool): Flag to indicate whether to validate the session
        vault_oauth_client_id (str): Client ID for OAuth authentication
        vault_oauth_profile_id (str): Profile ID for OAuth authentication
        vault_session_id (str): Session ID associated with the Vault request
        authentication_response (AuthenticationResponse): Authentication response from Vault
    """

    vault_dns: str = None
    vault_username: str = None
    vault_password: str = None
    vault_client_id: str = None
    http_timeout: Optional[int] = 60
    authentication_type: AuthenticationType = None
    set_log_api_errors: bool = True
    idp_oauth_access_token: str = None
    idp_oauth_scope: str = 'openid'
    idp_password: str = None
    idp_username: str = None
    set_validate_session: bool = True
    vault_oauth_client_id: str = None
    vault_oauth_profile_id: str = None
    vault_session_id: str = None
    authentication_response: AuthenticationResponse = None

    def __post_init__(self):
        # Reassign attributes to trigger __setattr__
        for key, value in self.__dict__.items():
            setattr(self, key, value)

    def __setattr__(self, key: str, value: Any):
        # This method specifically handles the 'authentication_type' attribute,
        # validating that its value is either an instance of the AuthenticationType enum or a
        # valid string representing one of its members. Needed when authenticating from a settings file.

        if key == 'authentication_type':
            if isinstance(value, AuthenticationType):
                return super().__setattr__(key, value)
            if value in AuthenticationType.__members__.keys():
                authentication_type_enum = getattr(AuthenticationType, value)
                return super().__setattr__(key, authentication_type_enum)
            else:
                raise ValueError(f"'{value}' is not a valid AuthenticationType. Valid values are:"
                                 f" {AuthenticationType.__members__.keys()}")
        else:
            return super().__setattr__(key, value)

    def new_request(self, request_class: Any) -> Any:
        """
        Instantiate a new request to the Vault API endpoint.

        The Vault Client Id is required for all new requests.
        An error is thrown if no client id is set.

        Args:
          request_class (Any): The request class to instantiate.

        Returns:
          An instance of the request class.
        """

        request = None
        if self.vault_client_id is None or self.vault_client_id == '':
            _LOGGER.error('Vault Client ID is required')
        else:
            request = request_class()
            setattr(request, '_vault_client_id', self.vault_client_id)
            setattr(request, '_vault_dns', self.vault_dns)
            if self.authentication_response is not None:
                setattr(request, '_vault_session_id', self.authentication_response.sessionId)
        return request

    @staticmethod
    def get_login_endpoint(endpoint: str) -> str:
        """
        Get a fully formed API URL consisting of the Vault login URL and the API endpoint.

        Args:
            endpoint (str): API endpoint in the form "/objects/documents"

        Returns:
            str: URL for the API endpoint in the form https://login.veevavault.com/auth/discovery
        """
        return _URL_LOGIN + endpoint

    def is_log_api_errors_enabled(self) -> bool:
        """
        Indicates whether to log api errors. Default=true

        Returns:
            bool: true/false
        """
        return self.set_log_api_errors

    def validate_session(self, auth_request: AuthenticationRequest) -> bool:
        """
        Validates the current session ID. The session must be active and the vault DNS
        from the request and the response must be equal. If the session or vault DNS
        is not valid, the current session ID will be cleared and an error logged.

        Returns:
          bool: True if the session is valid.
        """

        is_valid = False
        _LOGGER.info('Validating session')
        request = self.new_request(AuthenticationRequest)
        version_response = request.retrieve_api_versions()
        if version_response is not None and version_response.is_successful():
            self.authentication_response.headers = version_response.headers
            self.authentication_response.responseStatus = version_response.responseStatus
            self.authentication_response.responseMessage = version_response.responseMessage
            self.authentication_response.errors = version_response.errors
            self.authentication_response.userId = version_response.get_header_vault_user_id()

            if version_response.values is not None:
                response_url = version_response.get_version_url(VaultRequest.VAULT_API_VERSION)
                is_valid = response_url == auth_request.get_api_endpoint(endpoint='')
                if is_valid:
                    _LOGGER.info('Session validation successful')
                else:
                    _LOGGER.error('vault_dns verification failed')
                    _LOGGER.error(f'Response endpoint = {response_url}')

        if not is_valid:
            self.authentication_response.responseStatus = vault_response.HTTP_RESPONSE_FAILURE
            self.authentication_response.sessionId = None
            if version_response is not None and version_response.has_errors():
                self.authentication_response.errors = version_response.errors

        return is_valid

    @staticmethod
    def authenticate_from_settings_file(file_path: str) -> 'VaultClient':
        """
        Authenticates a client using parameters from a .json file.

        Args:
          file_path (str): The path to a .json file containing the Vault Client parameters

        Returns:
            VaultClient: An instance of Vault Client

        Example:
            ```python
            # Example Request
            settings_file_path: str = 'settings_vapil_basic.json'
            vault_client: VaultClient = VaultClient.authenticate_from_settings_file(file_path=settings_file_path)

            # Example Response
            response: AuthenticationResponse = vault_client.authentication_response
            print(f'Response Status: {response.responseStatus}')
            print(f'Session ID: {response.sessionId}')
            print(f'Vault ID: {response.vaultId}')
            print(f'User ID: {response.userId}')
            ```
        """

        json_to_python_mapping = {
            "authenticationType": "authentication_type",
            "idpOauthAccessToken": "idp_oauth_access_token",
            "idpOauthScope": "idp_oauth_scope",
            "idpUsername": "idp_username",
            "idpPassword": "idp_password",
            "vaultUsername": "vault_username",
            "vaultPassword": "vault_password",
            "vaultDNS": "vault_dns",
            "vaultSessionId": "vault_session_id",
            "vaultClientId": "vault_client_id",
            "vaultOauthClientId": "vault_oauth_client_id",
            "vaultOauthProfileId": "vault_oauth_profile_id",
            "logApiErrors": "set_log_api_errors",
            "httpTimeout": "http_timeout",
            "validateSession": "set_validate_session",
        }

        with open(file_path, 'r') as settings_file:
            settings = json.load(settings_file)
            mapped_settings = {
                json_to_python_mapping[key]: value if value is not None else None
                for key, value in settings.items()
                if key in json_to_python_mapping
            }
            vault_client = VaultClient(**mapped_settings)
            return vault_client.authenticate()

    def authenticate(self) -> 'VaultClient':
        """
        Authenticates a Vault API Client with the configured credentials.
        Throws ValueError when required parameters are missing.

        Returns:
            VaultClient: Returns the current instance

        Raises:
            ValueError: When required parameters are missing

        Example:
            ```python
            # Example Request
            vault_client: VaultClient = VaultClient(
                vault_client_id='Veeva-Vault-DevSupport-VAPIL',
                vault_username='username@veeva.com',
                vault_password='Password123',
                vault_dns='cholecap.veevavault.com',
                authentication_type=AuthenticationType.BASIC)

            vault_client.authenticate()

            # Example Response
            response: AuthenticationResponse = vault_client.authentication_response
            print(f'Response Status = {response.responseStatus}')
            print(f'Session ID = {response.sessionId}')
            print(f'Vault ID = {response.vaultId}')
            print(f'User ID = {response.userId}')
            ```
        """

        if self.authentication_type is None:
            _LOGGER.error('Authentication type is required')
            raise ValueError('Vault Authentication Type is required')

        if self.vault_dns is None or self.vault_dns == '':
            _LOGGER.error('Vault DNS is required')
            raise ValueError('Vault DNS is required')

        if self.vault_client_id is None or self.vault_client_id == '':
            _LOGGER.error('Vault Client ID is required')
            raise ValueError('Vault Client ID is required')

        auth_request = self.new_request(AuthenticationRequest)

        self._switch_authentication_type(self.authentication_type, auth_request)
        return self

    def _switch_authentication_type(self, auth_type: AuthenticationType,
                                    auth_request: AuthenticationRequest = None) -> 'VaultClient':
        handlers = {
            AuthenticationType.BASIC: self._handle_basic_authentication,
            AuthenticationType.OAUTH_ACCESS_TOKEN: self._handle_oauth_access_token,
            AuthenticationType.OAUTH_DISCOVERY: self._handle_oauth_discovery,
            AuthenticationType.SESSION_ID: self._handle_session_id,
        }

        handler = handlers.get(auth_type, lambda: 'Invalid authentication type')
        return handler(auth_request)

    def _handle_basic_authentication(self, auth_request: AuthenticationRequest) -> 'VaultClient':
        """
        Handle BASIC [AuthenticationType](vault_client.md#src.vapil.client.vault_client.AuthenticationType).
        Authenticates using a username and password.

        Args:
            auth_request (AuthenticationRequest): The authentication request object

        Raises:
            ValueError: If the Vault username or password is missing

        Returns:
            VaultClient: Returns the current instance
        """

        if self.vault_username is None or self.vault_username == '':
            _LOGGER.error('Vault Username is required')
            raise ValueError('Vault Username is required')
        if self.vault_password is None or self.vault_password == '':
            _LOGGER.error('Vault Password is required')
            raise ValueError('Vault Password is required')

        auth_request._vault_dns = self.vault_dns
        auth_request._username = self.vault_username
        auth_request._password = self.vault_password
        auth_response = auth_request._login()
        self.authentication_response = auth_response
        return self

    def _handle_oauth_access_token(self, auth_request: AuthenticationRequest) -> 'VaultClient':
        """
        Handle OAuth_ACCESS_TOKEN [AuthenticationType](vault_client.md#src.vapil.client.vault_client.AuthenticationType).
        Authenticates using the provided OAuth profile ID and IDP OAuth Access Token.

        Args:
            auth_request (AuthenticationRequest): The authentication request object

        Raises:
            ValueError: If the OAuth profile ID or IDP OAuth Access Token is missing

        Returns:
            VaultClient: Returns the current instance
        """

        if self.vault_oauth_profile_id is None or self.vault_oauth_profile_id == '':
            _LOGGER.error('OAuth Profile ID is required')
            raise ValueError('OAuth Profile ID is required')
        if self.idp_oauth_access_token is None or self.idp_oauth_access_token == '':
            _LOGGER.error('IDP OAuth Access Token is required')
            raise ValueError('IDP OAuth Access Token is required')

        auth_request._vault_dns = self.vault_dns
        auth_request._vault_oauth_profile_id = self.vault_oauth_profile_id
        auth_request._idp_oauth_access_token = self.idp_oauth_access_token
        auth_response = auth_request._login_oauth()
        self.authentication_response = auth_response
        return self

    def _handle_oauth_discovery(self, auth_request: AuthenticationRequest = None):
        if self.vault_username is None or self.vault_username == '':
            _LOGGER.error('Vault Username is required')
            raise ValueError('Vault Username is required')

        if self.idp_password is None or self.idp_password == '':
            _LOGGER.error('IDP Password is required')
            raise ValueError('IDP Password is required')

        if self.idp_username is not None and self.idp_username != '':
            auth_request._idp_username = self.idp_username

        if self.vault_oauth_client_id is not None and self.vault_oauth_client_id != '':
            auth_request._vault_oauth_client_id = self.vault_oauth_client_id

        if self.vault_oauth_profile_id is not None and self.vault_oauth_profile_id != '':
            auth_request._vault_oauth_profile_id = self.vault_oauth_profile_id

        response: AuthenticationResponse = auth_request._login_with_discovery(self.vault_username, self.idp_password)
        self.authentication_response = response
        return self

    def _handle_session_id(self, auth_request: AuthenticationRequest) -> 'VaultClient':
        """
        Handle SESSION_ID [AuthenticationType](vault_client.md#src.vapil.client.vault_client.AuthenticationType).
        Authenticates using a provided Vault session ID.

        Args:
            auth_request (AuthenticationRequest): The authentication request object

        Raises:
            ValueError: If the Vault Session ID is missing

        Returns:
            VaultClient: Returns the current instance
        """

        if self.vault_session_id is None or self.vault_session_id == '':
            _LOGGER.error('Vault Session ID is required')
            raise ValueError('Vault Session ID is required')

        auth_request._vault_dns = self.vault_dns
        auth_response = AuthenticationResponse()
        auth_response.sessionId = self.vault_session_id
        self.authentication_response = auth_response
        if self.set_validate_session:
            self.validate_session(auth_request)
        return self
