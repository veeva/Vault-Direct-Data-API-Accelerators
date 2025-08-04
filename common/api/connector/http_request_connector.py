"""
Generic HTTP request module used for Vault API or other servers/APIs.

Includes a generic set of methods with no Vault specific information.
It is used internally by VAPIL.py for all HTTP requests.
It can also be used for integration specific HTTP calls (non-Vault).

Attributes:
    HTTP_HEADER_CONTENT_TYPE (str): HTTP header key for Content-Type
    HTTP_HEADER_ACCEPT (str): HTTP header key for Accept
    HTTP_HEADER_CONTENT_MD5 (str): HTTP header key for Content-MD5
    HTTP_HEADER_RANGE (str): HTTP header key for Range
    HTTP_HEADER_CONTENT_LENGTH (str): HTTP header key for Content-Length
    HTTP_CONTENT_TYPE_CSV (str): Content type for CSV (text/csv)
    HTTP_CONTENT_TYPE_JSON (str): Content type for JSON (application/json)
    HTTP_CONTENT_TYPE_SCIM_JSON (str): Content type for SCIM JSON (application/scim+json)
    HTTP_CONTENT_TYPE_OCTET (str): Content type for octet-stream (application/octet-stream)
    HTTP_CONTENT_TYPE_PLAINTEXT (str): Content type for plaintext (text/plain)
    HTTP_CONTENT_TYPE_XFORM (str): Content type for x-www-form-urlencoded (application/x-www-form-urlencoded)
    HTTP_CONTENT_TYPE_MULTIPART_FORM (str): Content type for multipart/form-data (multipart/form-data)
    HTTP_CONTENT_TYPE_MULTIPART_FORM_BOUNDARY (str): Content type boundary for multipart/form-data
"""

import logging
import os
from enum import Enum
from typing import Dict, Any

import requests

HTTP_HEADER_CONTENT_TYPE: str = 'Content-Type'
HTTP_HEADER_ACCEPT: str = 'Accept'
HTTP_HEADER_CONTENT_MD5: str = 'Content-MD5'
HTTP_HEADER_RANGE: str = 'Range'
HTTP_HEADER_CONTENT_LENGTH: str = 'Content-Length'
HTTP_CONTENT_TYPE_CSV: str = 'text/csv'
HTTP_CONTENT_TYPE_JSON: str = 'application/json'
HTTP_CONTENT_TYPE_SCIM_JSON: str = 'application/scim+json'
HTTP_CONTENT_TYPE_OCTET: str = 'application/octet-stream'
HTTP_CONTENT_TYPE_PLAINTEXT: str = 'text/plain'
HTTP_CONTENT_TYPE_XFORM: str = 'application/x-www-form-urlencoded'
HTTP_CONTENT_TYPE_MULTIPART_FORM: str = 'multipart/form-data'
HTTP_CONTENT_TYPE_MULTIPART_FORM_BOUNDARY: str = 'multipart/form-data boundary='

_LOGGER: logging.Logger = logging.getLogger(__name__)


class HttpMethod(Enum):
    """
    Enumeration class representing HTTP methods.

    Attributes:
        GET (HttpMethod ): HTTP GET method
        POST (HttpMethod ): HTTP POST method
        PUT (HttpMethod ): HTTP PUT method
        DELETE (HttpMethod ): HTTP DELETE method
    """
    GET = 'GET'
    POST = 'POST'
    PUT = 'PUT'
    DELETE = 'DELETE'


def send(http_method: HttpMethod,
         url: str,
         query_params: Dict = {},
         body: Any = None,
         headers: Dict = {},
         files: Dict = {}) -> requests.Response:
    """
    Perform an HTTP call based on the arguments provided
    (url, query_params, body, headers, files)
    for the provided HTTP Method.

    Args:
        http_method (HttpMethod): The HttpMethod for the request (GET, POST, PUT, DELETE).
        url (ResponseOption): Endpoint for the HTTP call
        query_params (Dict): Query parameters for the HTTP call if any
        body (Any): Body for the HTTP call if any
        headers (Dict): Headers for the HTTP call if any
        files (Dict): Files for the HTTP call if any

    Returns:
        Dict: Attributes from the HTTP response as a Dict
    """

    files_to_send = {}
    opened_files_for_requests = []
    if files:
        try:
            for field_name, file_path in files.items():
                filename = os.path.basename(file_path)
                file_object = open(file_path, 'rb')
                opened_files_for_requests.append(file_object) # Add to list
                files_to_send[field_name] = (filename, file_object)
        except Exception as e:
            # Clean up opened files if dictionary preparation fails
            for f_obj in opened_files_for_requests:
                f_obj.close()
            _LOGGER.error(f"Error preparing files for upload: {e}")
            raise requests.RequestException("Error preparing files for upload")

    try:
        http_response = requests.request(method=http_method.value,
                                         url=url,
                                         params=query_params,
                                         data=body,
                                         headers=headers,
                                         files=files_to_send)
    except Exception as e:
        _LOGGER.error(e)
        raise requests.RequestException("HTTP request failed")
    return http_response
