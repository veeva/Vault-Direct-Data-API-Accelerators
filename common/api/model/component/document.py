from __future__ import annotations
from dataclasses import field
from typing import List

from pydantic.dataclasses import dataclass

from ..vault_model import VaultModel


@dataclass(config=dict(extra="allow"))
class Document(VaultModel):
    """
    Model for the Document object in the response.

    """

    id: int = None
    version_id: str = None
    major_version_number__v: int = None
    minor_version_number__v: int = None
    annotations_all__v: int = None
    annotations_anchors__v: int = None
    annotations_lines__v: int = None
    annotations_links__v: int = None
    annotations_notes__v: int = None
    annotations_resolved__v: int = None
    annotations_unresolved__v: int = None
    archive__v: bool = None
    binder__v: bool = None
    bound_source_major_version__v: int = None
    bound_source_minor_version__v: int = None
    classification__v: str = None
    created_by__v: int = None
    crosslink__v: bool = None
    description__v: str = None
    document_creation_date__v: str = None
    document_number__v: str = None
    external_id__v: str = None
    filename__v: str = None
    format__v: str = None
    latest_source_major_version__v: int = None
    latest_source_minor_version__v: int = None
    last_modified_by__v: int = None
    lifecycle__v: str = None
    link_status__v: List[str] = field(default_factory=list)
    locked__v: bool = None
    md5checksum__v: str = None
    name__v: str = None
    pages__v: int = None
    size__v: int = None
    source_binding_rule__v: List[str] = field(default_factory=list)
    source_document_id__v: int = None
    source_document_name__v: str = None
    source_document_number__v: str = None
    source_owner__v: int = None
    source_vault_id__v: int = None
    source_vault_name__v: str = None
    status__v: str = None
    subtype__v: str = None
    suppress_rendition__v: str = None
    title__v: str = None
    type__v: str = None
    version_created_by__v: int = None
    version_creation_date__v: str = None
    version_modified_date__v: str = None