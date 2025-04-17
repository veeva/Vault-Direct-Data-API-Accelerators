"""
Module that defines classes used to send Document requests to the Vault API.
"""
from enum import Enum
from typing import Set

from ..connector import http_request_connector
from ..connector.http_request_connector import HttpMethod
from ..model.component.document import Document
from ..model.response.document_response import DocumentFieldResponse, DocumentsResponse, DocumentResponse, \
    DocumentExportResponse, DocumentBulkResponse
from ..model.response.document_response import DocumentTypeHeirarchyResponse
from ..model.response.document_response import DocumentTypesResponse
from ..model.response.document_response import DocumentVersionsResponse
from ..model.response.jobs_response import JobCreateResponse
from ..model.response.vault_response import VaultResponse
from ..request.vault_request import VaultRequest, _ResponseOption, _RequestOption


class NamedFilter(Enum):
    """
    Enumeration class representing Retrieve all document named filters.

    Attributes:
        CART (str): Retrieves only documents in your cart.
        FAVORITES (str): Retrieves only documents which you have marked as favorites in the library.
        RECENT_DOCUMENTS (str): Retrieves only documents which you have recently accessed.
        MY_DOCUMENTS (str): Retrieves only documents which you have created.
    """

    CART: str = 'Cart'
    FAVORITES: str = 'Favorites'
    RECENT_DOCUMENTS: str = 'Recent Documents'
    MY_DOCUMENTS: str = 'My Documents'


class Scope(Enum):
    """
    Enumeration class representing Retrieve all document scopes.

    Attributes:
        ALL (str): Searches both within the document content and searchable document fields.
        CONTENTS (str): Searches only within the document content.
    """

    ALL: str = 'all'
    CONTENTS: str = 'contents'


class VersionsScope(Enum):
    """
    Enumeration class representing Retrieve all document versions scopes.

    Attributes:
        ALL (str): Retrieves all document versions, rather than only the latest version.
    """

    ALL: str = 'all'


class DocumentRequest(VaultRequest):
    """
    Class that defines methods used to call Documents endpoints.

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.1/#documents](https://developer.veevavault.com/api/24.1/#documents)
    """

    _URL_DOC_ALL_FIELDS: str = '/metadata/objects/documents/properties'
    _URL_DOC_COMMON_FIELDS: str = '/metadata/objects/documents/properties/find_common'
    _URL_DOC_TYPES: str = '/metadata/objects/documents/types'
    _URL_DOC_TYPE: str = '/metadata/objects/documents/types/{type}'
    _URL_DOC_SUBTYPE: str = '/metadata/objects/documents/types/{type}/subtypes/{subtype}'
    _URL_DOC_CLASSIFICATION: str = '/metadata/objects/documents/types/{type}/subtypes/{subtype}/classifications/{classification}'
    _URL_DOCS: str = '/objects/documents'
    _URL_DOC: str = '/objects/documents/{doc_id}'
    _URL_DOC_VERSIONS: str = '/objects/documents/{doc_id}/versions'
    _URL_DOC_VERSION: str = '/objects/documents/{doc_id}/versions/{major_version}/{minor_version}'
    _URL_DOC_FILE: str = '/objects/documents/{doc_id}/file'
    _URL_DOC_VERSION_FILE: str = '/objects/documents/{doc_id}/versions/{major_version}/{minor_version}/file'
    _URL_DOC_VERSION_THUMBNAIL: str = '/objects/documents/{doc_id}/versions/{major_version}/{minor_version}/thumbnail'
    _URL_DOCS_BATCH: str = '/objects/documents/batch'
    _URL_DOC_EXTRACT: str = '/objects/documents/batch/actions/fileextract'
    _URL_DOC_EXTRACT_VERSIONS: str = '/objects/documents/versions/batch/actions/fileextract'
    _URL_DOC_EXTRACT_RESULTS: str = '/objects/documents/batch/actions/fileextract/{jobid}/results'

    _HTTP_HEADER_VAULT_MIGRATION_MODE: str = 'X-VaultAPI-MigrationMode'

    def retrieve_all_document_fields(self) -> DocumentFieldResponse:
        """
        **Retrieve All Document Fields**

        Retrieve all standard and custom document fields and field properties.

        Returns:
          DocumentFieldResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/metadata/objects/documents/properties

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#retrieve-all-document-fields](https://developer.veevavault.com/api/24.1/#retrieve-all-document-fields)

        Example:
            ```python
            # Example Request
            request: DocumentRequest = vault_client.new_request(DocumentRequest)
            response: DocumentFieldResponse = request.retrieve_all_document_fields()

            # Example Response
            properties: List[DocumentField] = response.properties
            for document_field in properties:
                print('-----Document Field-----')
                print(f'Field Name: {document_field.name}')
                print(f'Field Type: {document_field.type}')
                print(f'Field Required: {document_field.required}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOC_ALL_FIELDS)

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=DocumentFieldResponse)

    def retrieve_common_document_fields(self, doc_ids: Set[int]) -> DocumentFieldResponse:
        """
        **Retrieve Common Document Fields**

        Retrieve all document fields and field properties which are common to (shared by) a specified set of documents.
        This allows you to determine which document fields are eligible for bulk update.

        Returns:
          DocumentFieldResponse: Modeled response from Vault

        Vault API Endpoint:
            POST /api/{version}/metadata/objects/documents/properties/find_common

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#retrieve-common-document-fields](https://developer.veevavault.com/api/24.1/#retrieve-common-document-fields)

        Example:
            ```python
            # Example Request
            request: DocumentRequest = vault_client.new_request(DocumentRequest)
            response: DocumentFieldResponse = request.retrieve_common_document_fields(doc_ids=doc_ids)

            # Example Response
            properties: List[DocumentField] = response.properties
            for document_field in properties:
                print('-----Document Field-----')
                print(f'Field Name: {document_field.name}')
                print(f'Field Type: {document_field.type}')
                print(f'Field Required: {document_field.required}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOC_COMMON_FIELDS)
        self._add_header_param(http_request_connector.HTTP_HEADER_CONTENT_TYPE,
                               http_request_connector.HTTP_CONTENT_TYPE_XFORM)

        doc_ids_str: str = ",".join(map(str, doc_ids))
        self._add_body_param('docIds', doc_ids_str)
        return self._send(http_method=HttpMethod.POST,
                          url=endpoint,
                          response_class=DocumentFieldResponse)

    def retrieve_all_document_types(self) -> DocumentTypesResponse:
        """
        **Retrieve All Document Types**

        Retrieve all document types. These are the top-level of the document type/subtype/classification hierarchy.

        Returns:
          DocumentTypesResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/metadata/objects/documents/types

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#retrieve-all-document-types](https://developer.veevavault.com/api/24.1/#retrieve-all-document-types)

        Example:
            ```python
            # Example Request
            request: DocumentRequest = vault_client.new_request(DocumentRequest)
            response: DocumentTypesResponse = request.retrieve_all_document_types()

            # Example Response
            types: List[DocumentTypesResponse.DocumentType] = response.types
            for document_type in types:
                print('-----Document Type-----')
                print(f'Label: {document_type.label}')
                print(f'Value: {document_type.value}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOC_TYPES)

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=DocumentTypesResponse)

    def retrieve_document_type(self, type: str) -> DocumentTypeHeirarchyResponse:
        """
        **Retrieve Document Type**

        Retrieve all metadata from a document type, including all of its subtypes (when available).

        Returns:
          DocumentTypeHeirarchyResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/metadata/objects/documents/types/{type}

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#retrieve-document-type](https://developer.veevavault.com/api/24.1/#retrieve-document-type)

        Example:
            ```python
            # Example Request
            request: DocumentRequest = vault_client.new_request(DocumentRequest)
            response: DocumentTypeHeirarchyResponse = request.retrieve_document_type(type=doc_type)

            # Example Response
            print(f'Name: {response.name}')
            print(f'Label: {response.label}')

            properties: List[DocumentField] = response.properties
            for document_field in properties:
                print('-----Document Field-----')
                print(f'Field Name: {document_field.name}')
                print(f'Field Type: {document_field.type}')

            subtypes: List[DocumentTypeResponse.DocumentSubType] = response.subtypes
            for document_subtype in subtypes:
                print('-----Document Subtype-----')
                print(f'Label: {document_subtype.label}')
                print(f'Value: {document_subtype.value}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOC_TYPE)
        endpoint = endpoint.replace('{type}', type)

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=DocumentTypeHeirarchyResponse)

    def retrieve_document_subtype(self, type: str, subtype: str) -> DocumentTypeHeirarchyResponse:
        """
        **Retrieve Document Subtype**

        Retrieve all metadata from a document subtype, including all of its classifications (when available).

        Returns:
          DocumentTypeHeirarchyResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/metadata/objects/documents/types/{type}/subtypes/{subtype}

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#retrieve-document-subtype](https://developer.veevavault.com/api/24.1/#retrieve-document-subtype)

        Example:
            ```python
            # Example Request
            request: DocumentRequest = vault_client.new_request(DocumentRequest)
            response: DocumentTypeHeirarchyResponse = request.retrieve_document_subtype(type=doc_type, subtype=doc_subtype)

            # Example Response
            print(f'Name: {response.name}')
            print(f'Label: {response.label}')

            properties: List[DocumentField] = response.properties
            for document_field in properties:
                print('-----Document Field-----')
                print(f'Field Name: {document_field.name}')
                print(f'Field Type: {document_field.type}')

            classifications: List[DocumentTypeResponse.DocumentSubType] = response.classifications
            for classification in classifications:
                print('-----Document Classification-----')
                print(f'Label: {classification.label}')
                print(f'Value: {classification.value}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOC_SUBTYPE)
        endpoint = endpoint.replace('{type}', type)
        endpoint = endpoint.replace('{subtype}', subtype)

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=DocumentTypeHeirarchyResponse)

    def retrieve_document_classification(self, type: str, subtype: str,
                                         classification: str) -> DocumentTypeHeirarchyResponse:
        """
        **Retrieve Document Classification**

        Retrieve all metadata from a document classification.

        Returns:
          DocumentTypeHeirarchyResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/metadata/objects/documents/types/{type}/subtypes/{subtype}/classifications/{classification}

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#retrieve-document-classification](https://developer.veevavault.com/api/24.1/#retrieve-document-classification)

        Example:
            ```python
            # Example Request
            request: DocumentRequest = vault_client.new_request(DocumentRequest)
            response = request.retrieve_document_classification(type=doc_type, subtype=doc_subtype, classification=doc_classification)

            # Example Response
            print(f'Name: {response.name}')
            print(f'Label: {response.label}')

            properties: List[DocumentField] = response.properties
            for document_field in properties:
                print('-----Document Field-----')
                print(f'Field Name: {document_field.name}')
                print(f'Field Type: {document_field.type}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOC_CLASSIFICATION)
        endpoint = endpoint.replace('{type}', type)
        endpoint = endpoint.replace('{subtype}', subtype)
        endpoint = endpoint.replace('{classification}', classification)

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=DocumentTypeHeirarchyResponse)

    def retrieve_all_documents(self, named_filter: NamedFilter = None,
                               scope: Scope = None,
                               versionscope: VersionsScope = None,
                               search_keyword: str = None,
                               limit: int = None,
                               sort: str = None,
                               start: int = None) -> DocumentsResponse:
        """
        **Retrieve All Documents**

        Retrieve the latest version of documents and binders to which you have access.

        Returns:
          DocumentsResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/objects/documents

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#retrieve-all-documents](https://developer.veevavault.com/api/24.1/#retrieve-all-documents)

        Example:
            ```python
            # Example Request
            request: DocumentRequest = vault_client.new_request(DocumentRequest)
            response: DocumentsResponse = request.retrieve_all_documents()

            # Example Response
            documents: List[DocumentsResponse.DocumentNode] = response.documents
            for documentNode in documents:
                document: Document = documentNode.document
                print(f'Document ID: {document.id})')
                print(f'Document Name: {document.name__v})')
                print(f'Major Version Number: {document.major_version_number__v})')
                print(f'Minor Version Number: {document.minor_version_number__v})')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOCS)

        if named_filter is not None:
            self._add_query_param('named_filter', named_filter.value)

        if scope is not None:
            self._add_query_param('scope', scope.value)

        if versionscope is not None:
            self._add_query_param('versionscope', versionscope.value)

        if search_keyword is not None:
            self._add_query_param('search_keyword', search_keyword)

        if limit is not None:
            self._add_query_param('limit', limit)

        if sort is not None:
            self._add_query_param('sort', sort)

        if start is not None:
            self._add_query_param('start', start)

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=DocumentsResponse)

    def retrieve_document(self, doc_id: int) -> DocumentResponse:
        """
        **Retrieve Document**

        Retrieve all metadata from a document.

        Returns:
            DocumentResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/objects/documents/{doc_id}

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#retrieve-document](https://developer.veevavault.com/api/24.1/#retrieve-document)

        Example:
            ```python
            # Example Request
            request: DocumentRequest = get_vault_client.new_request(DocumentRequest)
            response: DocumentResponse = request.retrieve_document(doc_id=doc_id)

            # Example Response
            document: Document = response.document
            print(f'Document ID: {document.id}')
            print(f'Document Name: {document.name__v}')
            print(f'Major Version Number: {document.major_version_number__v})')
            print(f'Minor Version Number: {document.minor_version_number__v})')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOC)
        endpoint = endpoint.replace('{doc_id}', str(doc_id))

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=DocumentResponse)

    def retrieve_document_versions(self, doc_id: int) -> DocumentVersionsResponse:
        """
        **Retrieve Document Versions**

        Retrieve all versions of a document.

        Returns:
            DocumentVersionsResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/objects/documents/{doc_id}/versions

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#retrieve-document-versions](https://developer.veevavault.com/api/24.1/#retrieve-document-versions)

        Example:
            ```python
            # Example Request
            request: DocumentRequest = vault_client.new_request(DocumentRequest)
            response: DocumentVersionsResponse = request.retrieve_document_versions(doc_id=doc_id)

            # Example Response
            for version in response.versions:
                print('-----Version-----')
                print(f'Version Number: {version.number}')
                print(f'URL: {version.value}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOC_VERSIONS)
        endpoint = endpoint.replace('{doc_id}', str(doc_id))

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=DocumentVersionsResponse)

    def retrieve_document_version(self, doc_id: int,
                                  major_version: int,
                                  minor_version: int) -> DocumentResponse:
        """
        **Retrieve Document Version**

        Retrieve all fields and values configured on a document version.

        Args:
            doc_id (int): The document ID.
            major_version (int): The major version number.
            minor_version (int): The minor version number.

        Returns:
            DocumentResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/objects/documents/{doc_id}/versions/{major_version}/{minor_version}

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#retrieve-document-version](https://developer.veevavault.com/api/24.1/#retrieve-document-version)

        Example:
            ```python
            # Example Request
            request: DocumentRequest = vault_client.new_request(DocumentRequest)
            response: DocumentResponse = request.retrieve_document_version(
                doc_id=doc_id,
                major_version=major_version,
                minor_version=minor_version)

            # Example Response
            document: Document = response.document
            print(f'Document ID: {document.id}')
            print(f'Document Name: {document.name__v}')
            print(f'Major Version Number: {document.major_version_number__v})')
            print(f'Minor Version Number: {document.minor_version_number__v})')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOC_VERSION)
        endpoint = endpoint.replace('{doc_id}', str(doc_id))
        endpoint = endpoint.replace('{major_version}', str(major_version))
        endpoint = endpoint.replace('{minor_version}', str(minor_version))

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=DocumentResponse)

    def download_document_file(self, doc_id: int, lock_document: bool = None) -> VaultResponse:
        """
        **Download Document File**

        Download the latest version of the source file from the document.

        Args:
            doc_id (int): The document ID.
            lock_document (bool): Set to true to Check Out this document before retrieval.

        Returns:
            VaultResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/objects/documents/{doc_id}/file

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#download-document-file](https://developer.veevavault.com/api/24.1/#download-document-file)

        Example:
            ```python
            # Example Request
            request: DocumentRequest = vault_client.new_request(request_class=DocumentRequest)
            response: VaultResponse = request.download_document_file(doc_id=doc_id)

            # Example Response
            print(f'File Name: {response.headers.get("Content-Disposition")}')
            print(f'Size: {len(response.binary_content)}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOC_FILE)
        endpoint = endpoint.replace('{doc_id}', str(doc_id))

        if lock_document is not None:
            self._add_query_param('lockDocument', lock_document)

        self._response_option = _ResponseOption.BYTES

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=VaultResponse)

    def download_document_version_file(self, doc_id: int,
                                       major_version: int,
                                       minor_version: int) -> VaultResponse:
        """
        **Download Document Version File**

        Download the file of a specific document version.

        Args:
            doc_id (int): The document ID.
            major_version (int): The major version number.
            minor_version (int): The minor version number.

        Returns:
            VaultResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/objects/documents/{doc_id}/versions/{major_version}/{minor_version}/file

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#download-document-version-file](https://developer.veevavault.com/api/24.1/#download-document-version-file)

        Example:
            ```python
            # Example Request
            request: DocumentRequest = vault_client.new_request(request_class=DocumentRequest)
            response: VaultResponse = request.download_document_version_file(doc_id=doc_id,
                                                                             major_version=major_version,
                                                                             minor_version=minor_version)

            # Example Response
            print(f'File Name: {response.headers.get("Content-Disposition")}')
            print(f'Size: {len(response.binary_content)}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOC_VERSION_FILE)
        endpoint = endpoint.replace('{doc_id}', str(doc_id))
        endpoint = endpoint.replace('{major_version}', str(major_version))
        endpoint = endpoint.replace('{minor_version}', str(minor_version))

        self._response_option = _ResponseOption.BYTES

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=VaultResponse)

    def download_document_version_thumbnail_file(self, doc_id: int,
                                                 major_version: int,
                                                 minor_version: int) -> VaultResponse:
        """
        **Download Document Version Thumbnail File**

        Download the thumbnail image file of a specific document version.

        Args:
            doc_id (int): The document ID.
            major_version (int): The major version number.
            minor_version (int): The minor version number.

        Returns:
            VaultResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/objects/documents/{doc_id}/versions/{major_version}/{minor_version}/thumbnail

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#download-document-version-thumbnail-file](https://developer.veevavault.com/api/24.1/#download-document-version-thumbnail-file)

        Example:
            ```python
            # Example Request
            request: DocumentRequest = vault_client.new_request(request_class=DocumentRequest)
            response: VaultResponse = request.download_document_version_thumbnail_file(doc_id=doc_id,
                                                                                       major_version=major_version,
                                                                                       minor_version=minor_version)

            # Example Response
            print(f'Size: {len(response.binary_content)}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOC_VERSION_THUMBNAIL)
        endpoint = endpoint.replace('{doc_id}', str(doc_id))
        endpoint = endpoint.replace('{major_version}', str(major_version))
        endpoint = endpoint.replace('{minor_version}', str(minor_version))

        self._response_option = _ResponseOption.BYTES

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=VaultResponse)

    def create_single_document(self, doc: Document) -> DocumentResponse:
        """
        **Create Single Document**

        Create a single document.

        Args:
            doc (Document): The document object.

        Returns:
            DocumentResponse: Modeled response from Vault

        Vault API Endpoint:
            POST /api/{version}/objects/documents

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#create-single-document](https://developer.veevavault.com/api/24.1/#create-single-document)

        Example:
            ```python
            # Example Request
            document: Document = Document()
            document.name__v = 'Test Document'
            document.type__v = 'VAPIL Test Doc Type'
            document.subtype__v = 'VAPIL Test Doc Subtype'
            document.classification__v = 'VAPIL Test Doc Classification'
            document.lifecycle__v = 'VAPIL Test Doc Lifecycle'

            request: DocumentRequest = vault_client.new_request(request_class=DocumentRequest)
            response: DocumentResponse = request.create_single_document(doc=document)

            # Example Response
            print(f'Document ID: {response.id}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOCS)

        self._body_params = doc.__dict__
        return self._send(http_method=HttpMethod.POST,
                          url=endpoint,
                          response_class=DocumentResponse)

    def create_multiple_documents(self, input_path: str = None,
                                  request_string: str = None,
                                  migration_mode: bool = False) -> DocumentBulkResponse:
        """
        **Create Multiple Documents**

        This endpoint allows you to create multiple documents at once with a CSV input file.

        Args:
            input_path (str): The path to the input CSV file.
            request_string (str): The csv request string.
            migration_mode (bool): Set to true to enable migration mode.

        Returns:
            DocumentBulkResponse: Modeled response from Vault

        Vault API Endpoint:
            POST /api/{version}/objects/documents/batch

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#create-multiple-documents](https://developer.veevavault.com/api/24.1/#create-multiple-documents)

        Example:
            ```python
            # Example Request
            request: DocumentRequest = vault_client.new_request(request_class=DocumentRequest)
            response: DocumentBulkResponse = request.create_multiple_documents(input_path=csv_path)

            # Example Response
            data: List[DocumentResponse] = response.data
            for document_response in data:
                print(f'Response Status: {document_response.responseStatus}')
                print(f'Document ID: {document_response.id}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOCS_BATCH)

        self._add_header_param(http_request_connector.HTTP_HEADER_CONTENT_TYPE,
                               http_request_connector.HTTP_CONTENT_TYPE_CSV)

        if migration_mode:
            self._add_query_param(self._HTTP_HEADER_VAULT_MIGRATION_MODE, migration_mode)

        if input_path is not None:
            content = None
            with open(input_path, 'r', encoding='utf-8') as file:
                content = file.read()
            self._body_params = content

        if request_string:
            self._add_raw_string(request_string)

        return self._send(http_method=HttpMethod.POST,
                          url=endpoint,
                          response_class=DocumentBulkResponse)

    def update_single_document(self, doc: Document) -> DocumentResponse:
        """
        **Update Single Document**

        Update editable field values on the latest version of a single document.

        Args:
            doc (Document): The document object.

        Returns:
            DocumentResponse: Modeled response from Vault

        Vault API Endpoint:
            PUT /api/{version}/objects/documents/{doc_id}

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.2/#update-single-document](https://developer.veevavault.com/api/24.2/#update-single-document)

        Example:
            ```python
            # Example Request
            document: Document = Document()
            document.id = doc_id
            document.name__v = 'Test Document Update'

            request: DocumentRequest = get_vault_client.new_request(request_class=DocumentRequest)
            response: DocumentResponse = request.update_single_document(doc=document)

            # Example Response
            print(f'Document ID: {response.id}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOC)
        endpoint = endpoint.replace('{doc_id}', str(doc.id))

        self._body_params = doc.__dict__
        return self._send(http_method=HttpMethod.PUT,
                          url=endpoint,
                          response_class=DocumentResponse)

    def export_documents(self, request_string: str = None,
                         include_source: bool = True,
                         include_renditions: bool = False,
                         include_allversions: bool = False) -> JobCreateResponse:
        """
        **Export Documents**

        Use this request to export a set of documents to your Vault’s file staging server.

        Args:
            request_string (str): The json request string.
            include_source (bool): Include the source file. If omitted, defaults to true.
            include_renditions (bool): Include renditions. If omitted, defaults to false.
            include_allversions (bool): Include all versions. If omitted, defaults to false.

        Returns:
            JobCreateResponse: Modeled response from Vault

        Vault API Endpoint:
            POST /api/{version}/objects/documents/batch/actions/fileextract

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#export-documents-1](https://developer.veevavault.com/api/24.1/#export-documents-1)

        Example:
            ```python
            # Example Request
            request: DocumentRequest = get_vault_client.new_request(request_class=DocumentRequest)
            response: JobCreateResponse = request.export_documents(request_string=json_string)

            # Example Response
            print(f'Job ID: {response.job_id}')
            print(f'URL: {response.url}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOC_EXTRACT)
        self._add_body_param('source', include_source)
        self._add_body_param('renditions', include_renditions)
        self._add_body_param('allversions', include_allversions)

        self._add_header_param(http_request_connector.HTTP_HEADER_CONTENT_TYPE,
                               http_request_connector.HTTP_CONTENT_TYPE_JSON)

        if request_string:
            self._add_raw_string(request_string)

        return self._send(http_method=HttpMethod.POST,
                          url=endpoint,
                          response_class=JobCreateResponse)

    def export_document_versions(self, request_string: str = None,
                                 include_source: bool = True,
                                 include_renditions: bool = False) -> JobCreateResponse:
        """
        **Export Document Versions**

        Export a specific set of document versions to your Vault’s file staging server. The files you export go to the u{userID} folder, regardless of your security profile.

        Args:
            request_string (str): The json request string.
            include_source (bool): Include the source file. If omitted, defaults to true.
            include_renditions (bool): Include renditions. If omitted, defaults to false.

        Returns:
            JobCreateResponse: Modeled response from Vault

        Vault API Endpoint:
            POST /api/{version}/objects/documents/versions/batch/actions/fileextract

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#export-document-versions](https://developer.veevavault.com/api/24.1/#export-document-versions)

        Example:
            ```python
            # Example Request
            request: DocumentRequest = get_vault_client.new_request(request_class=DocumentRequest)
            response: JobCreateResponse = request.export_document_versions(request_string=json_string)

            # Example Response
            print(f'Job ID: {response.job_id}')
            print(f'URL: {response.url}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOC_EXTRACT)
        self._add_body_param('source', include_source)
        self._add_body_param('renditions', include_renditions)

        self._add_header_param(http_request_connector.HTTP_HEADER_CONTENT_TYPE,
                               http_request_connector.HTTP_CONTENT_TYPE_JSON)

        if request_string:
            self._add_raw_string(request_string)

        return self._send(http_method=HttpMethod.POST,
                          url=endpoint,
                          response_class=JobCreateResponse)

    def retrieve_document_export_results(self, job_id: int) -> DocumentExportResponse:
        """
        **Retrieve Document Export Results**

        After submitting a request to export documents from your Vault, you can query your Vault to determine the results of the request.

        Args:
            job_id (int): The job ID.

        Returns:
            DocumentExportResponse: Modeled response from Vault

        Vault API Endpoint:
            GET /api/{version}/objects/documents/batch/actions/fileextract/{jobid}/results

        Vault API Documentation:
            [https://developer.veevavault.com/api/24.1/#retrieve-document-export-results](https://developer.veevavault.com/api/24.1/#retrieve-document-export-results)

        Example:
            ```python
            # Example Request
            request: DocumentRequest = get_vault_client.new_request(request_class=DocumentRequest)
            response: DocumentExportResponse = request.retrieve_document_export_results(job_id=job_id)

            # Example Response
            data: List[DocumentExportResponse.ExportedDocument] = response.data
            for exported_document in data:
                print('-----Exported Document-----')
                print(f'Response Status: {exported_document.responseStatus}')
                print(f'ID: {exported_document.id}')
                print(f'Major Version Number: {exported_document.major_version_number__v}')
                print(f'Minor Version Number: {exported_document.minor_version_number__v}')
            ```
        """
        endpoint = self.get_api_endpoint(endpoint=self._URL_DOC_EXTRACT_RESULTS)
        endpoint = endpoint.replace('{jobid}', str(job_id))

        self._add_header_param(http_request_connector.HTTP_HEADER_CONTENT_TYPE,
                               http_request_connector.HTTP_CONTENT_TYPE_JSON)

        return self._send(http_method=HttpMethod.GET,
                          url=endpoint,
                          response_class=DocumentExportResponse)
