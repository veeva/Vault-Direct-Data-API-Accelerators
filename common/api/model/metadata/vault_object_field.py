from __future__ import annotations

from typing import List

from pydantic.dataclasses import dataclass

from ..vault_model import VaultModel


@dataclass
class VaultObjectField(VaultModel):
    """
    Model for the Vault Object Field in the response
    """
    checkbox: bool = None
    component: str = None
    created_by: int = None
    created_date: str = None
    create_object_inline: bool = None
    editable: bool = None
    encrypted: bool = None
    facetable: bool = None
    help_content: str = None
    label: str = None
    list_column: bool = None
    lookup_relationship_name: str = None
    lookup_source_field: str = None
    max_length: int = None
    max_value: int = None
    min_value: int = None
    modified_by: int = None
    modified_date: str = None
    multi_part_field: str = None
    multi_part_readonly: str = None
    multi_value: bool = None
    name: str = None
    no_copy: bool = None
    object: ObjectReference = None
    order: int = None
    picklist: str = None
    relationship_criteria: str = None
    relationship_deletion: str = None
    relationship_inbound_label: str = None
    relationship_inbound_name: str = None
    relationship_outbound_name: str = None
    relationship_type: str = None
    required: bool = None
    scale: int = None
    secure_relationship: bool = None
    sequential_naming: bool = None
    source: str = None
    start_number: int = None
    status: List[str] = None
    system_managed_name: bool = None
    type: str = None
    value_format: str = None
    unique: bool = None

    @dataclass
    class ObjectReference(VaultModel):
        label: str = None
        label_plural: str = None
        name: str = None
        prefix: str = None
        url: str = None
