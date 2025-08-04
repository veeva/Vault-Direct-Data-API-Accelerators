"""
Module that defines classes used to send Query requests to the Vault API.
"""
import logging
from enum import Enum

from ..connector import http_request_connector
from ..connector.http_request_connector import HttpMethod
from ..model.response.query_response import QueryResponse
from ..request.vault_request import VaultRequest

_LOGGER: logging.Logger = logging.getLogger(__name__)


class RecordPropertyType(Enum):
    """
    Enumeration class representing record property objects.

    Attributes:
        ALL (str): All
        HIDDEN (str): Hidden
        REDACTED (str): Redacted
        WEBLINK (str): Weblink
    """

    ALL: str = 'all'
    HIDDEN: str = 'hidden'
    REDACTED: str = 'redacted'
    WEBLINK: str = 'weblink'


class QueryRequest(VaultRequest):
    """
    Class that defines methods used to call Query endpoints.

    Vault API Documentation:
        [https://developer.veevavault.com/api/25.2/#vault-query-language-vql](https://developer.veevavault.com/api/25.2/#vault-query-language-vql)
    """

    HTTP_HEADER_VAULT_DESCRIBE_QUERY: str = 'X-VaultAPI-DescribeQuery'
    HTTP_HEADER_VAULT_RECORD_PROPERTIES: str = 'X-VaultAPI-RecordProperties'

    _URL_QUERY: str = '/query'

    def query(self, vql: str,
              query_describe: bool = None,
              record_property_type: RecordPropertyType = None) -> QueryResponse:
        """
        **VQL Query**

        Perform a Vault query request.

        Subsequent queries and pagination are needed to retrieve the full result set
        if the total records returned exceed the "pagesize" parameter in the response.
        See `query_by_page` method.

        Returned records can be retrieved via the `data` attribute in the response.

        Args:
            vql: The Vault Query Language (VQL) query string
            query_describe: If true, include static field metadata in the response for the data record
            record_property_type: The type of record property to return in the response

        Returns:
            QueryResponse: Modeled response from Vault

        Vault API Endpoint:
            POST /api/{version}/query

        Vault API Documentation:
            [https://developer.veevavault.com/api/25.2/#submitting-a-query](https://developer.veevavault.com/api/25.2/#submitting-a-query)

        Example:
            ```python
            # Example Query
            example_query: str = 'SELECT id, name__v FROM user__sys MAXROWS 3'
            request: QueryRequest = vault_client.new_request(request_class=QueryRequest)
            response: QueryResponse = request.query(example_query)

            # Example Response
            for query_result in response.data:
                print(f'ID: {query_result.id}')
                print(f'Name: {query_result.name__v}')
                print('-----------------------------')
            ```

        Example:
            ```python
            # Example Pagination
            if response.is_paginated():
                next_page_url: str = response.responseDetails.next_page
                page_request: QueryRequest = vault_client.new_request(request_class=QueryRequest)
                page_response: QueryResponse = page_request.query_by_page(next_page_url)

                for query_result in page_response.data:
                    print(f'ID: {query_result.id}')
                    print(f'Name: {query_result.name__v}')
                    print('-----------------------------')
            ```

        Example:
            ```python
            # Example Query Describe
            request: QueryRequest = vault_client.new_request(request_class=QueryRequest)
            response: QueryResponse = request.query(example_query, query_describe=True)

            print(f'Object Name: {response.queryDescribe.object.name}')
            print(f'Object Label: {response.queryDescribe.object.label}')
            print(f'Object Label Plural: {response.queryDescribe.object.label_plural}')
            print('-----------------------------')

            for field in response.queryDescribe.fields:
                print(f'Field Name: {field.name}')
                print(f'Field Label: {field.label}')
                print(f'Field Type: {field.type}')
                print(f'Field Required: {field.required}')
                print('-----------------------------')
            ```

        Example:
            ```python
            # Example Subquery
            example_query: str = \
                'SELECT id, name__sys, \
                (SELECT id, label__sys \
                FROM inactive_workflow_tasks__sysr) \
                FROM inactive_workflow__sys \
                MAXROWS 3'
            request: QueryRequest = vault_client.new_request(request_class=QueryRequest)
            response: QueryResponse = request.query(example_query)

            for query_result in response.data:
                print(f'ID: {query_result.id}')
                print(f'Name: {query_result.name__sys}')
                print('-----------------------------')
                subquery_response: QueryResponse = query_result.get_subquery('inactive_workflow_tasks__sysr')
                for subquery_result in subquery_response.data:
                    print(f'    ID: {subquery_result.id}')
                    print(f'    Label: {subquery_result.label__sys}')
                    print('     -----------------------------')
            ```
        """

        endpoint = self.get_api_endpoint(endpoint=self._URL_QUERY)
        self._add_header_param(http_request_connector.HTTP_HEADER_CONTENT_TYPE,
                               http_request_connector.HTTP_CONTENT_TYPE_XFORM)
        self._add_header_param(http_request_connector.HTTP_HEADER_ACCEPT,
                               http_request_connector.HTTP_CONTENT_TYPE_JSON)
        self._add_body_param('q', vql)

        if query_describe:
            self._add_header_param(self.HTTP_HEADER_VAULT_DESCRIBE_QUERY, str(query_describe).lower())
        if record_property_type:
            self._add_header_param(self.HTTP_HEADER_VAULT_RECORD_PROPERTIES, record_property_type.value)

        return self._send(http_method=HttpMethod.POST,
                          url=endpoint,
                          response_class=QueryResponse)

    def query_by_page(self, page_url: str) -> QueryResponse:
        """
        **Query by Page**

        Perform a paginated query based on the URL from a previous query
        (previous_page or next_page in the response details).

        Note that this does not support described query, which should be read
        upon the first query call if needed.

        Args:
            page_url (str): The URL from the previous_page or next_page parameter in the form POST /query/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx?pagesize=10&pageoffset=10

        Returns:
            QueryResponse: Modeled response from Vault

        Example:
            ```python
            # Example Pagination
            if response.is_paginated():
                next_page_url: str = response.responseDetails.next_page
                page_request: QueryRequest = vault_client.new_request(request_class=QueryRequest)
                page_response: QueryResponse = page_request.query_by_page(next_page_url)

                for query_result in page_response.data:
                    print(f'ID: {query_result.id}')
                    print(f'Name: {query_result.name__v}')
                    print('-----------------------------')
            ```
        """
        url: str = self.get_pagination_endpoint(page_url=page_url)
        return self._send(http_method=HttpMethod.GET,
                          url=url,
                          response_class=QueryResponse)
