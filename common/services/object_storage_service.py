from abc import ABC, abstractmethod
from common.utilities import log_message


class ObjectStorageService(ABC):
    def __init__(self, parameters: dict):
        self.convert_to_parquet: bool = parameters['convert_to_parquet']
        self.direct_data_folder: str = parameters['direct_data_folder']
        self.archive_filepath: str = parameters['archive_filepath']
        self.extract_folder: str = parameters['extract_folder']
        self.document_content_folder: str = parameters['document_content_folder']
        self.document_text_folder: str = parameters['document_text_folder']
        self.credentials: dict | None = None
        self.client: object | None = None

    @abstractmethod
    def upload_object(self, object_path: str, data) -> dict:
        """
            Upload file to object storage
            :param object_path: Path to the object in the storage
            :param data: Data to be uploaded
            :return: Response from the upload operation
        """
        pass

    @abstractmethod
    def create_multipart_upload(self, object_path: str) -> dict:
        """
            Create a multipart upload
            :param object_path: Path to the object in the storage
        """
        pass

    @abstractmethod
    def upload_part(self, object_path: str, multipart_upload_response: dict, part_number: int, data: bytes) -> dict:
        """
            Upload a part of a multipart upload
            :param object_path: Path to the object in the storage
            :param multipart_upload_response: Response from the create_multipart_upload method
            :param part_number: Part number to be uploaded
            :param data: Data to be uploaded
        """
        pass

    @abstractmethod
    def complete_multipart_upload(self, object_path: str, multipart_upload_response: dict, parts: list) -> dict:
        """
            Complete a multipart upload
            :param object_path: Path to the object in the storage
            :param multipart_upload_response: Response from the create_multipart_upload method
            :param parts: List of parts to be uploaded
        """
        pass

    @abstractmethod
    def abort_multipart_upload(self, object_path: str, multipart_upload_response: dict) -> dict:
        """
            Abort a multipart upload
            :param object_path: Path to the object in the storage
            :param multipart_upload_response: Response from the create_multipart_upload method
        """
        pass

    @abstractmethod
    def download_object_bytes(self, object_path: str) -> bytes:
        """
            Download file from object storage
            :param object_path: Path to the object in the storage
        """
        pass

    @abstractmethod
    def download_object_to_stream(self, object_path: str) -> object:
        """
            Download file from object storage to a stream
            :param object_path: Path to the object in the storage
        """
        pass

    @abstractmethod
    def download_object_to_local(self, object_path: str, output_path: str) -> None:
        """
            Download file from object storage to local
            :param object_path: Path to the object in the storage
            :param output_path: Local path to save the downloaded file
        """
        pass

    @abstractmethod
    def check_if_object_exists(self, object_path: str):
        """
            Check if an object exists in object storage
            :param object_path: Path to the object in object storage
        """
        pass

    @abstractmethod
    def get_full_object_path(self, filename: str) -> str:
        """
            Get the full path of an object in the storage
            :param filename: Path to the object in the storage
        """
        pass

    @abstractmethod
    def get_relative_object_path(self, filename: str) -> str:
        """
            Get the URL of an object in the storage
            :param filename: Path to the object in the storage
        """
        pass

    @abstractmethod
    def get_headers_from_csv_file(self, object_path: str) -> list:
        """
        Retrieves the headers from a CSV file located at the specified object path.

        :param object_path: The path to the CSV file in object storage
        :return: A list of column headers of the provided CSV files
        """
        pass

    @abstractmethod
    def get_headers_from_parquet_file(self, object_path: str) -> list:
        """
        Retrieves the headers from a Parquet file located at the specified object path.

        :param object_path: The path to the Parquet file in object storage
        :return: A list of column headers of the provided Parquet files
        """
        pass