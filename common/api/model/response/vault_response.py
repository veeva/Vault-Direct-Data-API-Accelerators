"""
Module that defines classes used to represent base Vault API response messages.

Attributes:
    HTTP_RESPONSE_WARNING (str): Constant for HTTP response warning status.
    HTTP_RESPONSE_FAILURE (str): Constant for HTTP response failure status.
    HTTP_RESPONSE_SUCCESS (str): Constant for HTTP response success status.
    HTTP_HEADER_CONTENT_DISPOSITION (str): HTTP header key for Content-Disposition.
    HTTP_HEADER_CONTENT_TYPE (str): HTTP header key for Content-Type.
    HTTP_HEADER_VAULT_BURST (str): HTTP header key for Vault Burst Limit.
    HTTP_HEADER_VAULT_BURST_REMAINING (str): HTTP header key for Vault Burst Limit Remaining.
    HTTP_HEADER_VAULT_EXECUTION_ID (str): HTTP header key for Vault Execution ID.
    HTTP_HEADER_VAULT_RESPONSE_DELAY (str): HTTP header key for Vault Response Delay.
    HTTP_HEADER_VAULT_TRUNCATED_SESSION_ID (str): HTTP header key for Truncated Session ID.
    HTTP_HEADER_VAULT_USER_ID (str): HTTP header key for Vault User ID.
    HTTP_HEADER_VAULT_ID (str): HTTP header key for Vault ID.
    HTTP_HEADER_VAULT_SDK_COUNT (str): HTTP header key for Vault SDK Count.
    HTTP_HEADER_VAULT_SDK_CPU_TIME (str): HTTP header key for Vault SDK CPU Time.
    HTTP_HEADER_VAULT_SDK_ELAPSED_TIME (str): HTTP header key for Vault SDK Elapsed Time.
    HTTP_HEADER_VAULT_SDK_GROSS_MEMORY (str): HTTP header key for Vault SDK Gross Memory.
    HTTP_HEADER_DOWNTIME_EXPECTED_DURATION_MINUTES (str): HTTP header key for Expected Downtime Duration.
    HTTP_HEADER_STATUS (str): HTTP header key for Vault API Status.
"""

from typing import Any, List, Dict

from pydantic.dataclasses import dataclass
from pydantic.fields import Field

from ..vault_model import VaultModel

HTTP_RESPONSE_WARNING: str = 'WARNING'
HTTP_RESPONSE_FAILURE: str = 'FAILURE'
HTTP_RESPONSE_SUCCESS: str = 'SUCCESS'
HTTP_HEADER_CONTENT_DISPOSITION: str = 'Content-Disposition'
HTTP_HEADER_CONTENT_TYPE: str = 'Content-Type'
HTTP_HEADER_VAULT_BURST: str = 'X-VaultAPI-BurstLimit'
HTTP_HEADER_VAULT_BURST_REMAINING: str = 'X-VaultAPI-BurstLimitRemaining'
HTTP_HEADER_VAULT_EXECUTION_ID: str = 'X-VaultAPI-ExecutionId'
HTTP_HEADER_VAULT_RESPONSE_DELAY: str = 'X-VaultAPI-ResponseDelay'
HTTP_HEADER_VAULT_TRUNCATED_SESSION_ID: str = 'X-VaultAPI-TruncatedSessionId'
HTTP_HEADER_VAULT_USER_ID: str = 'X-VaultAPI-UserId'
HTTP_HEADER_VAULT_ID: str = 'X-VaultAPI-VaultId'
HTTP_HEADER_VAULT_SDK_COUNT: str = 'X-VaultAPI-SdkCount'
HTTP_HEADER_VAULT_SDK_CPU_TIME: str = 'X-VaultAPI-SdkCpuTime'
HTTP_HEADER_VAULT_SDK_ELAPSED_TIME: str = 'X-VaultAPI-SdkElapsedTime'
HTTP_HEADER_VAULT_SDK_GROSS_MEMORY: str = 'X-VaultAPI-SdkGrossMemory'
HTTP_HEADER_DOWNTIME_EXPECTED_DURATION_MINUTES: str = 'X-VaultAPI-DowntimeExpectedDurationMinutes'
HTTP_HEADER_STATUS: str = 'X-VaultAPI-Status'
HTTP_HEADER_REFERENCE_ID: str = "X-VaultAPI-ReferenceId"


@dataclass
class APIResponseError(VaultModel):
    """
    Base Vault API response error message.

    Attributes:
        message (str): The error message
        type (str): The type of error
    """

    message: str = None
    type: str = None


@dataclass
class APIResponseWarning(VaultModel):
    """
    Base Vault API response warning message.

    Attributes:
        message (str): The warning message
        type (str): The type of warning
    """

    message: str = None
    type: str = None


@dataclass
class VaultResponse(VaultModel):
    """
    Base Vault API response message

    Attributes:
        errors (List[APIResponseError]): List of errors in the response.
        warnings (List[APIResponseWarning]): List of warnings in the response.
        headers (Dict[str, str]): Dictionary of headers in the response.
        response (str): The raw response content.
        responseStatus (str): The status of the response.
        responseMessage (str): A descriptive message about the response.
        binary_content (bytes): Binary content of the response, if applicable.
        errorType (str): The type of error, if present.
    """

    errors: List[APIResponseError] = Field(default_factory=list)
    warnings: List[APIResponseWarning] = Field(default_factory=list)
    headers: Dict[str, str] = Field(default_factory=dict)
    response: str = None
    responseStatus: str = None
    responseMessage: str = None
    binary_content: bytes = None
    errorType: str = None

    def is_warning(self) -> bool:
        """
        Determine if the response status equals WARNING.

        Returns:
            bool: True if the response status equals WARNING
        """

        return self.responseStatus.casefold() == HTTP_RESPONSE_WARNING.casefold()

    def is_failure(self) -> bool:
        """
        Determine if the response status equals FAILURE.

        Returns:
            bool: True if the response status equals FAILURE
        """

        return self.responseStatus.casefold() == HTTP_RESPONSE_FAILURE.casefold()

    def is_successful(self) -> bool:
        """
        Determine if the response status equals SUCCESS.

        Returns:
            bool: True if the response status equals SUCCESS
        """

        return self.responseStatus.casefold() == HTTP_RESPONSE_SUCCESS.casefold()

    def has_errors(self) -> bool:
        """
        Determine if the response has errors.

        Returns:
            bool: True if the response has errors
        """

        return self.errors is not None and bool(self.errors)

    def has_warnings(self) -> bool:
        """
        Determine if the response has warnings.

        Returns:
            bool: True if the response has warnings
        """
        return self.warnings is not None and bool(self.warnings)

    def get_http_header_content_disposition(self) -> str:
        """
        Get the HTTP header value for Content-Disposition.

        Returns:
            str: The HTTP header value for Content-Disposition
        """
        return self.headers.get(HTTP_HEADER_CONTENT_DISPOSITION)

    def get_http_header_content_type(self) -> str:
        """
        Get the HTTP header value for Content-Type.

        Returns:
            str: The HTTP header value for Content-Type
        """
        return self._get_header_as_string_ignore_case(HTTP_HEADER_CONTENT_TYPE)

    def get_header_vault_burst_limit(self) -> int:
        """
        Get the HTTP header value for Vault Burst Limit.

        Returns:
            int: The HTTP header value for Vault Burst Limit
        """
        return self._get_header_as_integer_ignore_case(HTTP_HEADER_VAULT_BURST)

    def get_header_vault_burst_limit_remaining(self) -> int:
        """
        Get the HTTP header value for Vault Burst Limit Remaining.

        Returns:
            int: The HTTP header value for Vault Burst Limit Remaining
        """
        return self._get_header_as_integer_ignore_case(HTTP_HEADER_VAULT_BURST_REMAINING)

    def get_header_vault_execution_id(self) -> str:
        """
        Get the HTTP header value for Vault Execution ID.

        Returns:
            str: The HTTP header value for Vault Execution ID
        """
        return self._get_header_as_string_ignore_case(HTTP_HEADER_VAULT_EXECUTION_ID)

    def get_header_vault_id(self) -> int:
        """
        Get the HTTP header value for Vault ID.

        Returns:
            int: The HTTP header value for Vault ID
        """
        return self._get_header_as_integer_ignore_case(HTTP_HEADER_VAULT_ID)

    def get_header_vault_response_delay(self) -> int:
        """
        Get the HTTP header value for Vault Response Delay.

        Returns:
            int: The HTTP header value for Vault Response Delay
        """
        return self._get_header_as_integer_ignore_case(HTTP_HEADER_VAULT_RESPONSE_DELAY)

    def get_header_vault_truncated_session_id(self) -> str:
        """
        Get the HTTP header value for Truncated Session ID.

        Returns:
            str: The HTTP header value for Truncated Session ID
        """
        return self._get_header_as_string_ignore_case(HTTP_HEADER_VAULT_TRUNCATED_SESSION_ID)

    def get_header_vault_user_id(self) -> str:
        """
        Get the HTTP header value for Vault User ID.

        Returns:
            str: The HTTP header value for Vault User ID
        """
        return self._get_header_as_string_ignore_case(HTTP_HEADER_VAULT_USER_ID)

    def get_header_vault_sdk_count(self) -> int:
        """
        Get the HTTP header value for Vault SDK Count.

        Returns:
            int: The HTTP header value for Vault SDK Count
        """
        return self._get_header_as_integer_ignore_case(HTTP_HEADER_VAULT_SDK_COUNT)

    def get_header_vault_sdk_cpu_time(self) -> int:
        """
        Get the HTTP header value for Vault SDK CPU Time.

        Returns:
            int: The HTTP header value for Vault SDK CPU Time
        """
        return self._get_header_as_integer_ignore_case(HTTP_HEADER_VAULT_SDK_CPU_TIME)

    def get_header_vault_sdk_elapsed_time(self) -> int:
        """
        Get the HTTP header value for Vault SDK Elapsed Time.

        Returns:
            int: The HTTP header value for Vault SDK Elapsed Time
        """
        return self._get_header_as_integer_ignore_case(HTTP_HEADER_VAULT_SDK_ELAPSED_TIME)

    def get_header_vault_sdk_gross_memory(self) -> int:
        """
        Get the HTTP header value for Vault SDK Gross Memory.

        Returns:
            int: The HTTP header value for Vault SDK Gross Memory
        """
        return self._get_header_as_integer_ignore_case(HTTP_HEADER_VAULT_SDK_GROSS_MEMORY)

    def get_header_downtime_expected_duration_minutes(self) -> int:
        """
        Get the HTTP header value for Expected Downtime Duration.

        Returns:
            int: The HTTP header value for Expected Downtime Duration
        """
        return self._get_header_as_integer_ignore_case(HTTP_HEADER_DOWNTIME_EXPECTED_DURATION_MINUTES)

    def get_header_status(self) -> int:
        """
        Get the HTTP header value for Vault API Status.

        Returns:
            int: The HTTP header value for Vault API Status
        """
        return self._get_header_as_integer_ignore_case(HTTP_HEADER_STATUS)

    def get_header_reference_id(self) -> str:
        """
        Get the HTTP header value for Reference ID.

        Returns:
            int: The HTTP header value for Reference ID
        """
        return self._get_header_as_string_ignore_case(HTTP_HEADER_REFERENCE_ID)

    def get_header_ignore_case(self, header: str) -> Any:
        """
        Get the HTTP header value for the specified header, ignoring case.

        Args:
            header (str): The HTTP header key

        Returns:
            Any: The HTTP header value
        """
        for key, value in self.headers.items():
            if key.lower() == header.lower():
                return value if value else None
        return None

    def _get_header_as_string_ignore_case(self, header: str) -> str:
        header_value = self.get_header_ignore_case(header)
        if header_value is not None:
            return str(header_value)

    def _get_header_as_integer_ignore_case(self, header: str) -> int:
        header_value = self.get_header_ignore_case(header)
        if header_value is not None:
            return int(header_value)
