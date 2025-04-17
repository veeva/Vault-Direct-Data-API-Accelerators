"""
Module that defines classes used to represent responses from the Direct Data endpoints.
"""
from __future__ import annotations

from typing import List

from pydantic.dataclasses import dataclass
from pydantic.fields import Field

from .vault_response import VaultResponse
from ..vault_model import VaultModel


@dataclass
class DirectDataResponse(VaultResponse):
    """
    Model for the following API calls responses:

    List Items<br>
    Download Item

    Attributes:
        data (List[DirectDataItem]): List of direct data items
        responseDetails (ResponseDetails): Response details

    Vault API Endpoint:
        GET /api/{version}/services/directdata/files<br>
        GET /api/{version}/services/directdata/files/{name}

    Vault API Documentation:
        [https://developer.veevavault.com/api/24.1/#retrieve-available-direct-data-files](https://developer.veevavault.com/api/24.1/#retrieve-available-direct-data-files)
        [https://developer.veevavault.com/api/24.1/#download-direct-data-file](https://developer.veevavault.com/api/24.1/#download-direct-data-file)
    """

    data: List[DirectDataItem] = Field(default_factory=list)
    responseDetails: ResponseDetails = Field(default=None)

    @dataclass
    class ResponseDetails(VaultModel):
        """
        Model for the response details object in the response.

        Attributes:
            total (int): The total number of files
        """

        total: int = None

    @dataclass
    class DirectDataItem(VaultModel):
        """
        Model for the data objects in the response

        Attributes:
            name (str): Name of the file
            filename (str): Filename of the file
            extract_type (str): Extract type of the file
            start_time (str): Start time of the file
            stop_time (str): Stop time of the file
            record_count (int): Number of records included in the file
            size (int): Size of the file
            fileparts (int): Number of file parts
            filepart_details (List[FilePart]): List of file parts
        """

        name: str = None
        filename: str = None
        extract_type: str = None
        start_time: str = None
        stop_time: str = None
        record_count: int = None
        size: int = None
        fileparts: int = None
        filepart_details: List[FilePart] = Field(default_factory=list)

        @dataclass
        class FilePart(VaultModel):
            """
            Model for the file part detail objects in the response

            Attributes:
                filepart (int): The file part number
                size (int): The size of the file part
                url (str): The URL to download the file part
            """

            name: str = None
            filename: str = None
            filepart: int = None
            size: int = None
            url: str = None
