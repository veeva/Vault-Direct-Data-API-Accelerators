"""
Module that defines classes used to represent responses from the Jobs endpoints.
"""
from __future__ import annotations

from pydantic.dataclasses import dataclass

from .vault_response import VaultResponse
from ..component.job import Job


@dataclass
class JobStatusResponse(VaultResponse):
    """
    Model for the following API calls responses:

    Retrieve Job Status

    Attributes:
        data (Job): Job

    Vault API Endpoint:
        GET /api/{version}/services/jobs/{job_id}

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.2/#retrieve-job-status](https://developer.veevavault.com/api/24.2/#retrieve-job-status)
    """

    data: Job = None


@dataclass
class JobCreateResponse(VaultResponse):
    """
    Model for the following API calls responses:

    Export Documents

    Attributes:
        url (str): URL to retrieve the current job status of the document export request.
        job_id (int): The Job ID value to retrieve the status and results of the document export request.

    Vault API Endpoint:
        POST /api/{version}/objects/documents/batch/actions/fileextract

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.1/#export-documents-1](https://developer.veevavault.com/api/24.1/#export-documents-1)
    """

    url: str = None
    job_id: int = None
