"""
Module that defines classes used to represent responses from the VQL endpoints.
"""
from __future__ import annotations

from typing import List, Dict

from pydantic.dataclasses import dataclass
from pydantic.fields import Field

from .vault_response import VaultResponse
from ..metadata.vault_object_field import VaultObjectField
from ..vault_model import VaultModel


@dataclass
class QueryResponse(VaultResponse):
    """
    Model for the following API calls responses:

    Submitting a Query

    Attributes:
        data (List[QueryResult]): List of query results
        responseDetails (ResponseDetails): Response details

    Vault API Endpoint:
        POST /api/{version}/query

    Vault API Documentation:
        [https://developer.veevavault.com/api/25.2/#submitting-a-query](https://developer.veevavault.com/api/25.2/#submitting-a-query)
    """

    data: List[QueryResult] = Field(default_factory=list)
    responseDetails: ResponseDetails = None
    queryDescribe: QueryDescribe = None
    record_properties: List[RecordProperty] = Field(default_factory=list)

    def is_paginated(self) -> bool:
        """
        Check if response is paginated

        Returns:
            bool: True if there is a next page of results
        """

        if self.responseDetails.next_page is not None or self.responseDetails.previous_page is not None:
            return True

        if self.responseDetails.size != self.responseDetails.total:
            return True

        return False

    @dataclass
    class QueryDescribe(VaultModel):
        """
        Model for the Query Describe object in the response.

        Attributes:
            object (QueryObject): The object metadata for the query
            fields (List[VaultObjectField]): The field metadata for the query
        """
        object: QueryObject = None
        fields: List[VaultObjectField] = Field(default_factory=list)

        @dataclass
        class QueryObject(VaultModel):
            """
            Model for the "object" node.

            Attributes:
                name (str): The object name
                label (str): The object label
                label_plural (str): The object plural label
            """
            name: str = None
            label: str = None
            label_plural: str = None

    @dataclass
    class ResponseDetails(VaultModel):
        """
        Model for the response details object in the response.

        Attributes:
            pagesize (int): The number of records displayed per page
            size (int): The total number of records displayed on the current page
            total (int): The total number of records found
            pageoffset (int): The records displayed on the current page are offset by this number of records
            next_page (str): The Pagination URL to navigate to the next page of results
            previous_page (str): The Pagination URL to navigate to the previous page of results
        """

        pagesize: int = None
        size: int = None
        total: int = None
        pageoffset: int = None
        next_page: str = None
        previous_page: str = None

        def has_next_page(self) -> bool:
            """
            Check if there is a next page of results

            Returns:
                bool: True if there is a next page of results
            """

            return self.next_page is not None and self.next_page != ""

        def has_previous_page(self) -> bool:
            """
            Check if there is a previous page of results

            Returns:
                bool: True if there is a previous page of results
            """

            return self.previous_page is not None and self.previous_page != ""

    @dataclass(config=dict(extra="allow"))
    class QueryResult(VaultModel):
        """
        Model for the query result object in the response.
        """

        def get_subquery(self, field_name: str) -> QueryResponse:
            """
            Get the subquery for the specified field.

            Args:
                field_name (str): The field name for the subquery

            Returns:
                QueryResponse: The subquery response
            """

            data: dict = getattr(self, field_name)
            query_response: QueryResponse = QueryResponse(**data)
            return query_response

    @dataclass()
    class RecordProperty(VaultModel):
        """
        Model for the record properties in the response.

        Attributes:
            id (str): The record ID
            field_additional_data (FieldAdditionalData): Additional data for the field
            field_properties (Dict[str, List[str]]): The field properties
        """
        id: str = None
        field_additional_data: FieldAdditionalData = None
        field_properties: Dict[str, List[str]] = Field(default_factory=dict)

        @dataclass(config=dict(extra="allow"))
        class FieldAdditionalData(VaultModel):
            pass
