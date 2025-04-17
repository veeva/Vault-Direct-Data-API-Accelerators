"""
Module that defines classes used to represent responses from the Validate Session User endpoint.
"""
from __future__ import annotations

from typing import List

from pydantic.dataclasses import dataclass
from pydantic.fields import Field

from ..vault_response import VaultResponse
from ...component.user import User


@dataclass
class UserRetrieveResponse(VaultResponse):
    """
    Model for the following API calls responses:

    Validate Session User

    Attributes:
        users (List[UserNode]): List of User Nodes

    Vault API Endpoint:
        GET /api/{version}/objects/users/me

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.1/#validate-session-user](https://developer.veevavault.com/api/24.1/#validate-session-user)
    """

    users: List[UserNode] = Field(default_factory=list)

    @dataclass
    class UserNode:
        """
        Model for the user node in the response

        Attributes:
            user (User): User
        """

        user: User = None
