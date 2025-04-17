from __future__ import annotations
from dataclasses import field
from typing import List

from pydantic.dataclasses import dataclass

from ..vault_model import VaultModel


@dataclass(config=dict(extra="allow"))
class DocumentField(VaultModel):
    """
    Model for the Document Field object in the response.

    Attributes:
        required (bool): When true, the field value must be set when creating new documents.
        editable (bool): When true, the field value can be defined by the currently authenticated user. When false, the field value is read-only or system-managed,
            or the current user does not have adequate permissions to edit this field.
        setOnCreateOnly (bool): When true, the field value can only be set once (when creating new documents).
        hidden: (bool): Boolean indicating field availability to the UI. When true, the field is never available to nor visible in the UI. When false, the field is always available to the UI
            but visibility to users is subject to field-level security overrides.
        queryable (bool): When true, field values can be retrieved using VQL.
        noCopy (bool): When true, field values are not copied when using the Make a Copy action.
        facetable (bool): When true, the field is available for use as a faceted filter in the Vault UI.
    """

    definedIn: str = None
    definedInType: str = None
    disabled: bool = None
    editable: bool = None
    facetable: bool = None
    helpContent: str = None
    hidden: bool = None
    label: str = None
    maxLength: int = None
    maxValue: int = None
    minValue: int = None
    name: str = None
    noCopy: bool = None
    queryable: bool = None
    repeating: bool = None
    required: bool = None
    scope: str = None
    section: str = None
    sectionPosition: int = None
    setOnCreateOnly: bool = None
    shared: bool = None
    systemAttribute: bool = None
    type: str = None
    usedIn: List[UsedIn] = field(default_factory=list)

    @dataclass
    class UsedIn:
        key: str = None
        type: str = None
