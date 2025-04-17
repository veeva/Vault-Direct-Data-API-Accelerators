"""
Module that defines classes used to represent responses from the Authentication endpoints.
"""
from __future__ import annotations

from typing import List, Dict, Set

from pydantic.dataclasses import dataclass
from pydantic.fields import Field

from .vault_response import VaultResponse
from ..vault_model import VaultModel


@dataclass
class AuthenticationResponse(VaultResponse):
    """
    Model for the following API calls responses:

    User Name and Password<br>
    OAuth 2.0 / OpenID Connect

    Attributes:
        sessionId (str): Session ID
        userId (int): User ID
        vaultId (int): Vault ID
        vaultIds (List[Vault]): The vault IDs the user has access to

    Vault API Endpoint:
        POST https://{vault_subdomain}/api/{version}/auth<br>
        POST https://login.veevavault.com/auth/oauth/session/{oath_oidc_profile_id}

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.1/#user-name-and-password](https://developer.veevavault.com/api/24.1/#user-name-and-password)<br>
        [https://developer.veevavault.com/api/24.1/#oauth-2-0-openid-connect](https://developer.veevavault.com/api/24.1/#oauth-2-0-openid-connect)
    """

    sessionId: str = None
    userId: int = None
    vaultId: int = None
    vaultIds: List[Vault] = Field(default_factory=list)

    @dataclass
    class Vault(VaultModel):
        """
        Model for the VaultId objects in the response.

        Attributes:
            id (int): The Vault ID
            name (str): The Vault name
            url (str): The Vault URL
        """

        id: int = None
        name: str = None
        url: str = None


@dataclass
class ApiVersionResponse(VaultResponse):
    """
    Model for the following API calls responses:

    Retrieve API Versions

    Attributes:
        values (Dict[str, str]): Dictionary of API versions and their URLs

    Vault API Endpoint:
        GET /api

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.1/#retrieve-api-versions](https://developer.veevavault.com/api/24.1/#retrieve-api-versions)
    """

    values: Dict[str, str] = Field(default_factory=dict)

    def get_versions(self) -> Set[str]:
        """
        Returns a set of API versions

        Returns:
            set: set of API versions
        """

        return set(self.values.keys())

    def get_version_url(self, version: str) -> str:
        """
        Returns the URL for a specific API version

        Args:
            version (str): API version

        Returns:
            str: URL for the API version
        """

        return self.values[version]


@dataclass
class DiscoveryResponse(VaultResponse):
    """
    Model for the following API calls responses:

        Authentication Type Discovery

    Attributes:
        data (DiscoveryData): Discovery data returned from Vault

    Vault API endpoint:
        GET https://login.veevavault.com/auth/discovery

    Vault API documentation:
        [https://developer.veevavault.com/api/24.1/#authentication-type-discovery](https://developer.veevavault.com/api/24.1/#authentication-type-discovery)
    """

    data: DiscoveryData = None

    @dataclass
    class DiscoveryData(VaultModel):
        """
        Model for the data objects in the response

        Attributes:
            auth_profiles (List[AuthProfile]): List of authentication profiles
            auth_type (str): Authentication type
        """

        auth_profiles: List[AuthProfile] = None
        auth_type: str = None

        @dataclass
        class AuthProfile(VaultModel):
            """
            Model for the authentication profile objects in the response

            Attributes:
                as_client_id (str): Client ID
                as_metadata (AsMetadata): Metadata
                description (str): Description
                id (str): ID
                label (str): Label
                use_adal (bool): Use ADAL
                vault_session_endpoint (str): Vault session endpoint
            """

            as_client_id: str = None
            as_metadata: AsMetadata = None
            description: str = None
            id: str = None
            label: str = None
            use_adal: bool = None
            vault_session_endpoint: str = None

            @dataclass
            class AsMetadata(VaultModel):
                """
                Model for the AS Metadata object in the response

                Attributes:
                    token_endpoint (str): Token endpoint
                """

                token_endpoint: str = None
