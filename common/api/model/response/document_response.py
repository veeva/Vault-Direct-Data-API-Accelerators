"""
Module that defines classes used to represent responses from the MDL endpoints.
"""
from __future__ import annotations

from typing import List

from pydantic import Field
from pydantic.dataclasses import dataclass

from .vault_response import VaultResponse
from ..component.document import Document
from ..component.document_field import DocumentField
from ..vault_model import VaultModel


@dataclass
class DocumentFieldResponse(VaultResponse):
    """
    Model for the following API calls responses:

    Retrieve All Document Fields <br/>
    Retrieve Common Document Fields

    Attributes:
        properties (List[DocumentField]): The list of document fields.

    Vault API Endpoint:
        GET /api/{version}/metadata/objects/documents/properties<br/>
        POST /api/{version}/metadata/objects/documents/properties/find_common

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.1/#retrieve-all-document-fields](https://developer.veevavault.com/api/24.1/#retrieve-all-document-fields)
        [https://developer.veevavault.com/api/24.1/#retrieve-common-document-fields](https://developer.veevavault.com/api/24.1/#retrieve-common-document-fields)
    """

    properties: List[DocumentField] = Field(default_factory=list)


@dataclass
class DocumentTypesResponse(VaultResponse):
    """
    Model for the following API calls responses:

    Retrieve All Document Types

    Attributes:
        types (List[DocumentType]): List of all standard and custom document types in your Vault.
        lock (str): URL to retrieve the document lock metadata (document check-out).

    Vault API Endpoint:
        GET /api/{version}/metadata/objects/documents/types

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.1/#retrieve-all-document-types](https://developer.veevavault.com/api/24.1/#retrieve-all-document-types)
    """

    types: List[DocumentType] = Field(default_factory=list)
    lock: str = None

    @dataclass
    class DocumentType(VaultModel):
        """
        Model for the Document Type object in the response.

        Attributes:
            label (str): Label of each document type as seen in the API and UI.
            value (str): URL to retrieve the metadata associated with each document type.
        """

        label: str = None
        value: str = None


@dataclass
class DocumentTypeHeirarchyResponse(VaultResponse):
    """
    Model for the following API calls responses:

    Retrieve Document Type<br/>
    Retrieve Document Subtype<br/>
    Retrieve Document Classification

    Attributes:
        name (str): Name of the document type. Used primarily in the API.
        label (str): Label of the document type as seen in the API and UI.
        properties (List[DocumentField]): List of all the document fields associated to the document type.
        renditions (List[str]): List of all rendition types available.
        relationshipTypes (List[RelationshipType]): List of all relationship types available.
        templates (List[Template]): List of all templates available (when configured).
        availableLifecycles (List[Lifecycle]): List of all lifecycles available.
        subtypes (List[Subtype]): List of all document subtypes available for the document type.
        classifications (List[Classification]): List of all document classifications available for the document subtype.

    Vault API Endpoint:
        GET /api/{version}/metadata/objects/documents/types/{type}<br/>
        GET /api/{version}/metadata/objects/documents/types/{type}/subtypes/{subtype}<br/>
        GET /api/{version}/metadata/objects/documents/types/{type}/classifications/{classification}

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.1/#retrieve-document-type](https://developer.veevavault.com/api/24.1/#retrieve-document-type)
        [https://developer.veevavault.com/api/24.1/#retrieve-document-subtype](https://developer.veevavault.com/api/24.1/#retrieve-document-subtype)
        [https://developer.veevavault.com/api/24.1/#retrieve-document-classification](https://developer.veevavault.com/api/24.1/#retrieve-document-classification)
    """
    name: str = None
    label: str = None
    properties: List[DocumentField] = Field(default_factory=list)
    renditions: List[str] = Field(default_factory=list)
    relationshipTypes: List[RelationshipType] = Field(default_factory=list)
    templates: List[Template] = Field(default_factory=list)
    availableLifecycles: List[Lifecycle] = Field(default_factory=list)
    subtypes: List[Subtype] = Field(default_factory=list)
    classifications: List[Classification] = Field(default_factory=list)

    @dataclass
    class RelationshipType(VaultModel):
        """
        Model for the Relationship Type object in the response.

        Attributes:
            label (str): Label of relationship type.
            value (str): URL to retrieve the metadata associated with each relationship type.
        """
        label: str = None
        value: str = None

    @dataclass
    class Template(VaultModel):
        """
        Model for the Template object in the response.

        Attributes:
            label (str): Label of template.
            name (str): Name of template.
            kind (str): Kind of template.
            definedIn (str): Defined in.
            definedInType (str): Defined in type.
        """

        label: str = None
        name: str = None
        kind: str = None
        definedIn: str = None
        definedInType: str = None

    @dataclass
    class Lifecycle(VaultModel):
        """
        Model for the Lifecycle object in the response.

        Attributes:
            name (str): Name of lifecycle.
            label (str): Label of lifecycle.

        """
        name: str = None
        label: str = None

    @dataclass
    class Subtype(VaultModel):
        """
        Model for the Subype object in the response.

        Attributes:
            label (str): Label of subtype.
            value (str): URL to retrieve the metadata associated with each subtype.
        """
        label: str = None
        value: str = None

    @dataclass
    class Classification(VaultModel):
        """
        Model for the Classification object in the response.

        Attributes:
            label (str): Label of subtype.
            value (str): URL to retrieve the metadata associated with each subtype.
        """
        label: str = None
        value: str = None


@dataclass
class DocumentsResponse(VaultResponse):
    """
    Model for the following API calls responses:

    Retrieve All Documents

    Attributes:
        documents (List[DocumentNode]): The list of document nodes.

    Vault API Endpoint:
        GET /api/{version}/objects/documents

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.1/#retrieve-all-documents](https://developer.veevavault.com/api/24.1/#retrieve-all-documents)
    """
    documents: List[DocumentNode] = None

    @dataclass
    class DocumentNode(VaultModel):
        """
        Model for the Document Node object in the response.

        Attributes:
            document (Document): The document object.
        """

        document: Document = None


@dataclass
class DocumentResponse(VaultResponse):
    """
    Model for the following API calls responses:

    Retrieve Document <br/>
    Retrieve Document Version

    Attributes:
        document (Document): The document object.
        renditions (List[str]): List of all rendition types available.
        versions (List[Version]): List of all versions available.
        attachments (List[Attachment]): List of all attachments available.
        id (int): ID of the document. (Only returned for create, delete, and update)
        external_id__v (str): External ID. (Only returned for create, delete, and update)
        major_version_number__v (int): Major version number. (Only returned for create, delete, and update)
        minor_version_number__v (int): Minor version number. (Only returned for create, delete, and update)

    Vault API Endpoint:
        GET /api/{version}/objects/documents/{doc_id}

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.1/#retrieve-document](https://developer.veevavault.com/api/24.1/#retrieve-document)
    """
    document: Document = None
    renditions: Renditions = Field(default_factory=list)
    versions: List[Version] = Field(default_factory=list)
    attachments: List[Attachment] = Field(default_factory=list)

    # ------------------------------------------------------------
    # Special case: when creating, deleting, and updating docs,
    # they do not return a document node. The id, major/minor,
    # and external_id__v are at the root.

    id: int = None
    external_id__v: str = None
    major_version_number__v: int = None
    minor_version_number__v: int = None

    @dataclass
    class Attachment(VaultModel):
        """
        Model for the Attachment object in the response.

        Attributes:
            id (str): ID of attachment.
            url (str): URL to retrieve the metadata associated with each attachment.
        """

        id: str = None
        url: str = None


@dataclass
class DocumentVersionsResponse(VaultResponse):
    """
    Model for the following API calls responses:

    Retrieve Document Versions

    Attributes:
        versions (List[Version]): List of all versions available.
        renditions (Renditions): Renditions object that contains available renditions.

    Vault API Endpoint:
        GET /api/{version}/objects/documents/{doc_id}/versions

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.1/#retrieve-document-versions](https://developer.veevavault.com/api/24.1/#retrieve-document-versions)
    """
    versions: List[Version] = None
    renditions: Renditions = Field(default_factory=list)


@dataclass
class DocumentBulkResponse(VaultResponse):
    """
    Model for the following API calls responses:

    Create Multiple Documents

    Attributes:
        data (List[DocumentResponse]): List of all document responses.

    Vault API Endpoint:
        POST /api/{version}/objects/documents/batch

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.1/#create-multiple-documents](https://developer.veevavault.com/api/24.1/#create-multiple-documents)
    """
    data: List[DocumentResponse] = Field(default_factory=list)

    def has_errors(self) -> bool:
        if super().has_errors():
            return True

        document_responses = self.get_data()
        if document_responses is None or len(document_responses) == 0:
            return True
        else:
            for document_response in document_responses:
                if document_response.has_errors():
                    return True

        return False


@dataclass
class DocumentExportResponse(VaultResponse):
    """
    Model for the following API calls responses:

    Retrieve Document Export Results

    Attributes:
        data (List[ExportedDocument]): List of all exported documents.

    Vault API Endpoint:
        GET /api/{version}/objects/documents/batch/actions/fileextract/{jobid}/results

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.1/#retrieve-document-export-results](https://developer.veevavault.com/api/24.1/#retrieve-document-export-results)
    """
    data: List[ExportedDocument] = Field(default_factory=list)

    @dataclass
    class ExportedDocument(VaultModel):
        """
        Model for the Exported Document object in the response.

        Attributes:
            responseStatus (str): Status of the exported document.
            id (int): ID of the document.
            major_version_number__v (int): Major version number of the document.
            minor_version_number__v (int): Minor version number of the document.
            file (str): The path on the file staging server.
            user_id__v (int): The id value of the Vault user who initiated the document export job.
        """

        responseStatus: str = None
        id: int = None
        major_version_number__v: int = None
        minor_version_number__v: int = None
        file: str = None
        user_id__v: int = None


@dataclass(config=dict(extra="allow"))
class Renditions(VaultModel):
    """
    Model for the Renditions object in the response.

    Attributes:
        viewable_rendition__v (str): URL to retrieve the viewable rendition.
    """

    viewable_rendition__v: str = None


@dataclass
class Version(VaultModel):
    """
    Model for the Version object in the response.

    Attributes:
        number (str): Version number.
        value (str): URL to retrieve the metadata associated with each version.
    """

    number: str = None
    value: str = None
