"""
Module that defines classes used to represent responses from the File Staging endpoints.
"""
from __future__ import annotations

from typing import List

from pydantic import Field
from pydantic.dataclasses import dataclass

from .vault_response import VaultResponse
from ..vault_model import VaultModel


@dataclass
class FileStagingItemResponse(VaultResponse):
    """
    Model for the following API calls responses:

    Create Folder or File

    Attributes:
        data (FileStagingItem): File staging item

    Vault API Endpoint:
        POST /api/{version}/services/file_staging/items

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.2/#create-folder-or-file](https://developer.veevavault.com/api/24.2/#create-folder-or-file)
    """

    data: FileStagingItem = None


@dataclass
class FileStagingItemBulkResponse(VaultResponse):
    """
    Model for the following API calls responses:

    List Items at a Path

    Attributes:
        data (List[FileStagingItem]): List of file staging items
        responseDetails (ResponseDetails): Response details

    Vault API Endpoint:
        GET /api/{version}/services/file_staging/items/{item}

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.2/#list-items-at-a-path](https://developer.veevavault.com/api/24.2/#list-items-at-a-path)
    """

    data: List[FileStagingItem] = Field(default_factory=list)
    responseDetails: ResponseDetails = Field(default=None)

    def is_paginated(self) -> bool:
        """
        Check if response is paginated

        Returns:
            bool: True if there is a next page of results
        """

        if self.responseDetails is not None and self.responseDetails.next_page is not None:
            return True

        return False


@dataclass
class FileStagingSessionResponse(VaultResponse):
    """
    Model for the following API calls responses:

    Create Resumable Upload Session<br/>
    Get Upload Session Details

    Attributes:
        data (ResumableUploadSession): Upload session

    Vault API Endpoint:
        POST /api/{version}/services/file_staging/upload<br/>
        GET /api/{version}/services/file_staging/upload/{upload_session_id}

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.2/#create-resumable-upload-session](https://developer.veevavault.com/api/24.2/#create-resumable-upload-session)<br/>
        [https://developer.veevavault.com/api/24.2/#get-upload-session-details](https://developer.veevavault.com/api/24.2/#get-upload-session-details)
    """

    data: ResumableUploadSession = None


@dataclass
class FileStagingSessionBulkResponse(VaultResponse):
    """
    Model for the following API calls responses:

    List Upload Sessions

    Attributes:
        data (List[ResumableUploadSession]): List of upload sessions
        responseDetails (ResponseDetails): Response details

    Vault API Endpoint:
        GET /api/{version}/services/file_staging/upload

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.2/#list-upload-sessions](https://developer.veevavault.com/api/24.2/#list-upload-sessions)
    """

    data: List[ResumableUploadSession] = Field(default_factory=list)
    responseDetails: ResponseDetails = None

    def is_paginated(self) -> bool:
        """
        Check if response is paginated

        Returns:
            bool: True if there is a next page of results
        """

        if self.responseDetails is not None and self.responseDetails.next_page is not None:
            return True

        return False


@dataclass
class FileStagingSessionPartResponse(VaultResponse):
    """
    Model for the following API calls responses:

    Upload to a Session

    Attributes:
        data (ResumableUploadSessionPart): Upload session part

    Vault API Endpoint:
        PUT /api/{version}/services/file_staging/upload/{upload_session_id}

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.2/#upload-to-a-session](https://developer.veevavault.com/api/24.2/#upload-to-a-session)
    """

    data: ResumableUploadSessionPart = None


@dataclass
class FileStagingSessionPartBulkResponse(VaultResponse):
    """
    Model for the following API calls responses:

    List File Parts Uploaded to Session

    Attributes:
        data (List[ResumableUploadSessionPart]): List of uploaded parts

    Vault API Endpoint:
        GET /api/{version}/services/file_staging/upload/{upload_session_id}/parts

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.2/#list-file-parts-uploaded-to-session](https://developer.veevavault.com/api/24.2/#list-file-parts-uploaded-to-session)
    """

    data: List[ResumableUploadSessionPart] = Field(default_factory=list)


@dataclass
class FileStagingJobResponse(VaultResponse):
    """
    Model for the following API calls responses:

    Update Folder or File<br/>
    Delete Folder or File<br/>
    Commit Upload Session

    Attributes:
        data (Job): Job

    Vault API Endpoint:
        PUT /api/{version}/services/file_staging/items/{item}<br/>
        DELETE /api/{version}/services/file_staging/items/{item}<br/>
        POST /api/{version}/services/file_staging/upload/{upload_session_id}

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.2/#update-folder-or-file](https://developer.veevavault.com/api/24.2/#update-folder-or-file)<br/>
        [https://developer.veevavault.com/api/24.2/#delete-file-or-folder](https://developer.veevavault.com/api/24.2/#delete-file-or-folder)<br/>
        [https://developer.veevavault.com/api/24.2/#commit-upload-session](https://developer.veevavault.com/api/24.2/#commit-upload-session)
    """

    data: Job = None

    @dataclass
    class Job(VaultModel):
        """
        Model for the data object in the response

        Attributes:
            job_id (int): Job ID
            url (str): URL of the job
        """

        job_id: int = None
        url: str = None


@dataclass
class ResponseDetails(VaultModel):
    """
    Model for the response details object in the response.

    Attributes:
        next_page (str): The next page of results
    """

    next_page: str = None


@dataclass
class FileStagingItem(VaultModel):
    """
    Model for the data objects in the response

    Attributes:
        kind (str): file/folder
        path (str): Path of the file/folder
        name (str): Name of the file/folder
        size (int): Size of the file
        modified_date (str): Modified date of the file
    """

    kind: str = None
    path: str = None
    name: str = None
    size: int = None
    modified_date: str = None


@dataclass
class ResumableUploadSession(VaultModel):
    created_date: str = None
    expiration_date: str = None
    owner: int = None
    id: str = None
    last_uploaded_date: str = None
    path: str = None
    size: int = None
    uploaded_parts: int = None
    uploaded: int = None
    name: str = None
    overwrite: bool = None


@dataclass
class ResumableUploadSessionPart(VaultModel):
    part_number: int = None
    size: int = None
    part_content_md5: str = None
