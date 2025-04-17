"""
Module that defines classes used to send Authentication requests to the Vault API.
"""
import json
import logging

from pydantic.dataclasses import dataclass

from ..connector import http_request_connector
from ..connector.http_request_connector import HttpMethod
from ..model.response import vault_response
from ..model.response.authentication_response import ApiVersionResponse
from ..model.response.authentication_response import AuthenticationResponse
from ..model.response.authentication_response import DiscoveryResponse
from ..model.response.shared.user_retrieve_response import UserRetrieveResponse
from ..model.response.vault_response import VaultResponse
from ..request.vault_request import VaultRequest

_LOGGER: logging.Logger = logging.getLogger(__name__)


@dataclass
class AuthenticationRequest(VaultRequest):
    """
    Authenticate to Vault using standard username/password, OAuth,
    or Salesforce delegated authentication. Successful connections
    return an AuthenticationResponse, which stores the Vault session ID.

    Note:
        The VaultClient automatically performs Authentication requests
        to establish the Vault session.

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.1/#authentication](https://developer.veevavault.com/api/24.1/#authentication)
    """

    _URL_API: str = ''
    _URL_AUTH: str = '/auth'
    _URL_KEEP_ALIVE: str = '/keep-alive'
    _URL_VALIDATE_SESSION_USER: str = '/objects/users/me'
    _URL_RETRIEVE_DELEGATIONS: str = '/delegation/vaults'
    _URL_INITIATE_DELEGATED_SESSION: str = '/delegation/login'
    _URL_DISCOVERY: str = 'https://login.veevavault.com/auth/discovery'
    _URL_OAUTH: str = 'https://login.veevavault.com/auth/oauth/session/{oauth_oidc_profile_id}'
    _URL_END_SESSION: str = '/session'
    _USERNAME: str = 'username'
    _PASSWORD: str = 'password'
    _CLIENT_ID: str = 'client_id'
    _VAULT_DNS: str = 'vaultDNS'
    _VAULT_ID: str = 'vault_id'
    _DELEGATOR_USER_ID: str = 'delegator_userid'
    _GRANT_TYPE: str = 'grant_type'
    _SCOPE: str = 'scope'

    def _login(self) -> AuthenticationResponse:
        # Authenticate via standard Vault username and password.
        #
        # Returns:
        #   AuthenticationResponse: Modeled response from Vault.
        #
        # Vault API Endpoint:
        #     GET /api/{version}/auth
        #
        # Vault API Documentation:
        #     [https://developer.veevavault.com/api/24.1/#user-name-and-password](https://developer.veevavault.com/api/24.1/#user-name-and-password)

        endpoint = self.get_api_endpoint(endpoint=self._URL_AUTH)
        self._add_header_param(http_request_connector.HTTP_HEADER_CONTENT_TYPE,
                               http_request_connector.HTTP_CONTENT_TYPE_XFORM)
        self._add_header_param(http_request_connector.HTTP_HEADER_ACCEPT,
                               http_request_connector.HTTP_CONTENT_TYPE_JSON)
        self._add_body_param(self._USERNAME, self._username)
        self._add_body_param(self._PASSWORD, self._password)
        self._add_body_param(self._VAULT_DNS, self._vault_dns)

        auth_response = self._send(http_method=HttpMethod.POST,
                                   url=endpoint,
                                   response_class=AuthenticationResponse)

        return self._validate_login_response(auth_response)

    def _login_oauth(self) -> AuthenticationResponse:
        # Authenticate your account using OAuth 2.0 / Open ID Connect token to obtain a Vault session ID.
        # Learn more about OAuth 2.0 / Open ID Connect in Vault Help.
        #
        # When requesting a sessionId, Vault allows the ability for Oauth2/OIDC client applications
        # to pass the client_id with the request. Vault uses this client_id when talking
        # with the introspection endpoint at the authorization server to validate
        # that the access_token presented by the application is valid.
        # Learn more about Client ID in the REST API Documentation.
        #
        # Returns:
        #     AuthenticationResponse: Modeled response from Vault
        #
        # Vault API Endpoint:
        #     POST login.veevavault.com/auth/oauth/session/{oauth_oidc_profile_id}
        #
        # Vault API Documentation:
        #     [https://developer.veevavault.com/api/24.1/#oauth-2-0-openid-connect](https://developer.veevavault.com/api/24.1/#oauth-2-0-openid-connect)

        endpoint = self._URL_OAUTH.replace("{oauth_oidc_profile_id}", self._vault_oauth_profile_id)

        self._add_header_param(http_request_connector.HTTP_HEADER_CONTENT_TYPE,
                               http_request_connector.HTTP_CONTENT_TYPE_XFORM)
        self._add_header_param('Authorization', 'Bearer ' + self._idp_oauth_access_token)

        if self._vault_oauth_client_id is not None and self._vault_oauth_client_id != '':
            self._add_body_param(self._CLIENT_ID, self._vault_oauth_client_id)

        if self._vault_dns is not None and self._vault_dns != '':
            self._add_body_param(self._VAULT_DNS, self._vault_dns)

        auth_response = self._send(http_method=HttpMethod.POST,
                                   url=endpoint,
                                   response_class=AuthenticationResponse)
        return self._validate_login_response(auth_response)

    def _login_with_discovery(self, vault_username: str,
                              password: str) -> AuthenticationResponse:
        # Authenticate using Vault Discovery endpoints. First Vault is queried for the user's
        # authentication method, and if SSO, this method attempts to acquire an OAuth token.
        # If the user is basic username/password, the simple login method is used.
        #
        # Args:
        #     vault_username: The user’s Vault username
        #     password: The user’s  password
        #
        # Returns:
        #     AuthenticationResponse: Modeled response from Vault

        discovery_response: DiscoveryResponse = self.authentication_type_discovery(username=vault_username)
        authentication_response: AuthenticationResponse = AuthenticationResponse()

        if discovery_response.is_successful():
            auth_type: str = discovery_response.data.auth_type
            if auth_type == 'sso':
                auth_profile: DiscoveryResponse.DiscoveryData.AuthProfile = \
                    discovery_response.data.auth_profiles[0]

                token_endpoint: str = auth_profile.as_metadata.token_endpoint

                token_username: str = self._idp_username
                if token_username is None or token_username == '':
                    token_username = self._username

                self._get_oauth_access_token(token_endpoint=token_endpoint,
                                             username=token_username,
                                             password=password,
                                             as_client_id=auth_profile.as_client_id)

            authentication_response = self._login_oauth()

        else:
            authentication_response = self._login()

        return self._validate_login_response(authentication_response)

    def authentication_type_discovery(self, username: str) -> DiscoveryResponse:
        """
        **Authentication Type Discovery**

        Discover the authentication type of a user. With this API,
        applications can dynamically adjust the login requirements per user,
        and support either username/password or OAuth2.0 / OpenID Connect authentication schemes.

        Args:
            username: The user’s Vault user name

        Returns:
            DiscoveryResponse: Modeled response from Vault

        Vault API Endpoint:
            POST https://login.veevavault.com/auth/discovery

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#authentication-type-discovery](https://developer.veevavault.com/api/24.1/#authentication-type-discovery)

        Note:
            Authenticating beforehand is not required to use this endpoint

        Example:
            ```python
            # Example Request
            vault_client: VaultClient = VaultClient(
                vault_client_id='Veeva-Vault-DevSupport-VAPIL')
            request: AuthenticationRequest = vault_client.new_request(request_class=AuthenticationRequest)
            response: DiscoveryResponse = request.authentication_type_discovery(username='username@veeva.com')

            # Example Response
            print(f'Response Status: {response.responseStatus}')
            print(f'Auth Type: {response.data.auth_type}')
            ```
        """

        if username is None or username == '':
            _LOGGER.error('Vault Username is required')
            raise ValueError('Vault Username is required')

        self._add_header_param(http_request_connector.HTTP_HEADER_ACCEPT,
                               http_request_connector.HTTP_CONTENT_TYPE_JSON)
        self._add_header_param(http_request_connector.HTTP_HEADER_CONTENT_TYPE,
                               http_request_connector.HTTP_CONTENT_TYPE_XFORM)

        self._add_body_param(self._USERNAME, username)
        if self._vault_oauth_client_id is not None and self._vault_oauth_client_id != '':
            self._add_body_param(self._CLIENT_ID, self._vault_oauth_client_id)

        return self._send(http_method=HttpMethod.POST,
                          url=self._URL_DISCOVERY,
                          response_class=DiscoveryResponse)

    def session_keep_alive(self) -> VaultResponse:
        """
        **Session Keep Alive**

        Retrieves API versions supported by the current Vault.

        Returns:
            VaultResponse: Modeled response from Vault

        Vault API Endpoint:
            POST /api/{version}/keep-alive

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#session-keep-alive](https://developer.veevavault.com/api/24.1/#session-keep-alive)

        Example:
            ```python
            # Example Request
            request: AuthenticationRequest = vault_client.new_request(request_class=AuthenticationRequest)
            response: VaultResponse = request.session_keep_alive()

            # Example Response
            print(f'Response Status: {response.responseStatus}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_KEEP_ALIVE)
        self._add_header_param(http_request_connector.HTTP_HEADER_ACCEPT,
                               http_request_connector.HTTP_CONTENT_TYPE_JSON)

        return self._send(http_method=HttpMethod.POST,
                          url=endpoint,
                          response_class=VaultResponse)

    def validate_session_user(self) -> UserRetrieveResponse:
        """
        **Validate Session User**

        Given a valid session ID, this request returns information for the currently authenticated user.
        If the session ID is not valid, this request returns an INVALID_SESSION_ID error type.
        This is similar to a whoami request.

        Returns:
            UserRetrieveResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/objects/users/me

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#validate-session-user](https://developer.veevavault.com/api/24.1/#validate-session-user)

        Example:
            ```python
            # Example Request
            request: AuthenticationRequest = vault_client.new_request(request_class=AuthenticationRequest)
            response: UserRetrieveResponse = request.validate_session_user()

            # Example Response
            print(f'Response Status: {response.responseStatus}')
            print(f'User ID: {response.users[0].user.id}')
            print(f'First Name: {response.users[0].user.user_first_name__v}')
            print(f'Last Name: {response.users[0].user.user_last_name__v}')
            ```
        """

        endpoint = self.get_api_endpoint(endpoint=self._URL_VALIDATE_SESSION_USER)

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=UserRetrieveResponse)

    def retrieve_api_versions(self) -> ApiVersionResponse:
        """
        **Retrieve API Versions**

        Retrieves API versions supported by the current Vault.

        Returns:
          ApiVersionResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#retrieve-api-versions](https://developer.veevavault.com/api/24.1/#retrieve-api-versions)

        Example:
            ```python
            # Example Request
            request: AuthenticationRequest = vault_client.new_request(request_class=AuthenticationRequest)
            response: ApiVersionResponse = request.retrieve_api_versions()

            # Example Response
            print(f'Response Status: {response.responseStatus}')
            print(f'Values: {response.values}')
            print(f'API Versions: {response.get_versions()}')
            print(f'API v24.1 URL: {response.get_version_url("v24.1")}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_API)
        self._add_header_param(http_request_connector.HTTP_HEADER_ACCEPT,
                               http_request_connector.HTTP_CONTENT_TYPE_JSON)

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=ApiVersionResponse)

    def end_session(self) -> VaultResponse:
        """
        **End Session**

        Given an active sessionId, inactivate an API session. If a user has multiple active sessions,
        inactivating one session does not inactivate all sessions for that user.
        Each session has its own unique sessionId.

        Returns:
            VaultResponse: Modeled response from Vault

        Vault API Endpoint:
            DELETE /api/{version}/session

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#end-session](https://developer.veevavault.com/api/24.1/#end-session)

        Example:
            ```python
            # Example Request
            request: AuthenticationRequest = vault_client.new_request(request_class=AuthenticationRequest)
            response: VaultResponse = request.end_session()

            # Example Response
            print(f'Response Status: {response.responseStatus}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_END_SESSION)
        self._add_header_param(http_request_connector.HTTP_HEADER_ACCEPT,
                               http_request_connector.HTTP_CONTENT_TYPE_JSON)

        return self._send(http_method=HttpMethod.DELETE,
                          url=endpoint,
                          response_class=VaultResponse)

    def _get_oauth_access_token(self, token_endpoint,
                                username: str,
                                password: str,
                                as_client_id: str):

        body_params = {self._GRANT_TYPE: 'password',
                       self._USERNAME: username,
                       self._PASSWORD: password,
                       self._CLIENT_ID: as_client_id}
        token_url = token_endpoint

        if self._idp_oauth_scope is not None:
            body_params[self._SCOPE] = self._idp_oauth_scope

        headers = {'Content-Type': http_request_connector.HTTP_CONTENT_TYPE_XFORM,
                   'Accept': http_request_connector.HTTP_CONTENT_TYPE_JSON}

        response_dict = http_request_connector.send(http_method=HttpMethod.POST,
                                                    url=token_url,
                                                    body=body_params,
                                                    headers=headers)

        response = json.loads(response_dict['response'])
        self._idp_oauth_access_token = response['access_token']

    def _validate_login_response(self, auth_response: AuthenticationResponse):
        # Verifies that the currently authenticated Vault shown by the “vaultId” field matches the vault DNS provided.
        #
        # Args:
        #     auth_response: AuthenticationResponse object
        #
        # Returns:
        #     AuthenticationResponse: Modeled response from Vault

        if auth_response is not None and auth_response.is_successful():
            if self._set_validate_session:
                user_supplied_endpoint = self.get_api_endpoint('')
                authenticated_vault_id = auth_response.vaultId
                response_url = None

                for vault in auth_response.vaultIds:
                    if vault.id == authenticated_vault_id:
                        response_url = f"{vault.url}/{VaultRequest.VAULT_API_VERSION}"
                        if user_supplied_endpoint.startswith(response_url):
                            _LOGGER.info('Authentication Succeeded')
                            return auth_response
                failed_response = AuthenticationResponse()
                failed_response.responseStatus = vault_response.HTTP_RESPONSE_FAILURE
                failed_response.response = auth_response.response
                failed_response.responseMessage = 'vaultDNS verification failed'
                _LOGGER.error(failed_response.responseMessage)
                _LOGGER.error(f'Response endpoint = {response_url}')
                return failed_response
            else:
                _LOGGER.info('Authentication Succeeded')
                return auth_response
        else:
            _LOGGER.error('Authentication Failed')
            return auth_response
