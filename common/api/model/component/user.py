from typing import List

from pydantic import Field
from pydantic.dataclasses import dataclass

from ..vault_model import VaultModel


@dataclass
class User(VaultModel):
    active__v: bool = None
    company__v: str = None
    created_by__v: int = None
    created_date__v: str = None
    domain_active__v: bool = None
    domain_id__v: int = None
    domain_name__v: str = None
    fax__v: str = None
    federated_id__v: str = None
    group_id__v: List[int] = Field(default_factory=list)
    id: int = None
    is_domain_admin__v: bool = None
    mobile_phone__v: str = None
    last_login__v: str = None
    license_type__v: str = None
    modified_by__v: int = None
    modified_date__v: str = None
    office_phone__v: str = None
    salesforce_user_name__v: str = None
    security_profile__v: str = None
    site__v: str = None
    user_email__v: str = None
    user_first_name__v: str = None
    user_language__v: str = None
    user_last_name__v: str = None
    user_locale__v: str = None
    user_name__v: str = None
    user_needs_to_change_password__v: bool = None
    user_timezone__v: str = None
    user_title__v: str = None
    vault_id__v: List[int] = Field(default_factory=list)
