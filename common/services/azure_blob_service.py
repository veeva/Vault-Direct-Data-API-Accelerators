import io
import csv
import os
from io import BytesIO

from azure.common import AzureException

from common.services.object_storage_service import ObjectStorageService

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, StorageStreamDownloader
from common.utilities import log_message
import pyarrow.parquet as pq


class AzureBlobService(ObjectStorageService):
    def __init__(self, parameters: dict):
        super().__init__(parameters=parameters)
        self.account_url: str = parameters['account_url']
        self.container: str = parameters['container']
        self.credentials = DefaultAzureCredential()
        self.azure_client: BlobServiceClient = BlobServiceClient(
            account_url=self.account_url,
            credential=self.credentials
        )
        self.container_client: ContainerClient | None= None

    def get_blob_client(self, blob_name: str) -> BlobClient:
        log_message(log_level='Debug',
                    message=f'Retrieving Blob Client for {self.container}/{blob_name}')
        try:
            blob_client: BlobClient = self.azure_client.get_blob_client(
                container=self.container,
                blob=blob_name)

            log_message(log_level='Info',
                        message=f'Retrieved Blob Client for {self.container}/{blob_name}')
            return blob_client

        except Exception as e:
            log_message(log_level='Error',
                        message=f'Error getting blob client for {self.container}/{blob_name}',
                        exception=e)
            raise e

    def get_container_client(self) -> ContainerClient:
        log_message(log_level='Debug',
                    message=f'Retrieving Container Client for {self.container}')
        try:
            if self.container_client:
                return self.container_client

            container_client: ContainerClient = self.azure_client.get_container_client(
                container=self.container)

            log_message(log_level='Info',
                        message=f'Retrieved Container Client for {self.container}')
            return container_client

        except Exception as e:
            log_message(log_level='Error',
                        message=f'Error getting Container client for {self.container}',
                        exception=e)
            raise e

    def check_if_object_exists(self, object_path: str):
        log_message(log_level='Debug',
                    message=f'Checking if object exists: {self.container}/{object_path}')
        try:
            blob_client: BlobClient = self.get_blob_client(blob_name=object_path)
            exists: bool = blob_client.exists()
            if not exists:
                raise FileNotFoundError(f'File not found on {self.container}/{object_path}')
            log_message(log_level='Info',
                        message=f'Object exists: {self.container}/{object_path}')
        except AzureException as e:
            log_message(log_level='Error',
                        message=f'Error checking if object exists: {self.container}/{object_path}',
                        exception=e)
            raise e


    def upload_object(self, object_path: str, data) -> dict:
        log_message(log_level='Debug',
                    message=f'Uploading to {self.container}/{object_path}')
        try:
            blob_client: BlobClient = self.get_blob_client(blob_name=object_path)
            response: dict = blob_client.upload_blob(data=data, overwrite=True)
            log_message(log_level='Info',
                        message=f'Uploaded successfully to {self.container}/{blob_client.blob_name}')
            return response
        except Exception as e:
            log_message(log_level='Error',
                        message=f'Error uploading blob to {self.container}/{object_path}',
                        exception=e)
            raise e

    def create_multipart_upload(self, object_path: str) -> dict:
        log_message(log_level='Debug',
                    message=f'Initiating multipart upload not required for Azure Blob')
        return {}

    def upload_part(self, object_path: str, multipart_upload_response: dict, part_number: int, data: bytes) -> dict:
        log_message(log_level='Debug',
                    message=f'Staging block with ID: {part_number}')
        try:
            blob_client: BlobClient = self.get_blob_client(blob_name=object_path)
            response: dict = blob_client.stage_block(block_id=part_number,
                                                     data=data,
                                                     connection_timeout=3600)
            log_message(log_level='Info',
                        message=f'Staged block with ID: {part_number} successfully', )
            return {'PartNumber': part_number}

        except Exception as e:
            log_message(log_level='Error',
                        message=f'Error staging block with ID: {part_number}',
                        exception=e)
            raise e

    def complete_multipart_upload(self, object_path: str, multipart_upload_response: dict, parts: list) -> dict:
        log_message(log_level='Debug',
                    message=f'Committing block list for blob: {object_path}, with block IDs: {parts}')
        try:
            blob_client: BlobClient = self.get_blob_client(blob_name=object_path)
            block_ids: list = [part['PartNumber'] for part in parts]
            response: dict = blob_client.commit_block_list(block_list=block_ids)
            log_message(log_level='Info',
                        message=f'Block list committed successfully for blob: {blob_client.blob_name}, with block IDs: {block_ids}')
            return response

        except Exception as e:
            log_message(log_level='Error',
                        message=f'Error committing block list for blob: {object_path}',
                        exception=e)
            raise e

    def abort_multipart_upload(self, object_path: str, multipart_upload_response: dict) -> dict:
        log_message(log_level='Debug',
                    message=f'Abort multipart upload not required for Azure Blob')
        return {}

    def download_object_bytes(self, object_path: str) -> bytes:
        log_message(log_level='Debug',
                    message=f'Downloading blob from {self.container}/{object_path}')
        try:
            response: StorageStreamDownloader = self.get_container_client().download_blob(blob=object_path)
            log_message(log_level='Info',
                        message=f'Downloaded blob successfully from {self.container}/{object_path}')
            return response.readall()
        except Exception as e:
            log_message(log_level='Error',
                        message=f'Error downloading blob from {self.container}/{object_path}',
                        exception=e)
            raise e

    def download_object_to_stream(self, object_path: str) -> StorageStreamDownloader:
        log_message(log_level='Debug',
                    message=f'Downloading blob from {self.container}/{object_path}')
        try:
            response: StorageStreamDownloader = self.get_container_client().download_blob(blob=object_path)
            log_message(log_level='Info',
                        message=f'Downloaded blob successfully from {self.container}/{object_path}')
            return response
        except Exception as e:
            log_message(log_level='Error',
                        message=f'Error downloading blob from {self.container}/{object_path}',
                        exception=e)
            raise e

    def download_object_to_local(self, object_path: str, output_path: str):
        log_message(log_level='Debug',
                    message=f'Downloading object from {self.container}/{object_path}')
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            directory = os.path.dirname(output_path)

            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)

            response: StorageStreamDownloader = self.get_container_client().download_blob(blob=object_path)
            with open(output_path, "wb") as download_file:
                download_file.write(response.readall())
            log_message(log_level='Info',
                        message=f'Object downloaded to file: {output_path}')
        except AzureException as e:
            log_message(log_level='Exception',
                        message=f'Error getting object from Blob: {self.container}/{object_path}',
                        exception=e)
            raise e

    def get_full_object_path(self, filename: str) -> str:
        """
        Constructs the full object path in the Azure Blob Storage container.
        :param filename: The name of the file to be stored in the container.
        :return: Full object path in the format 'container/filename'.
        """
        return f"{self.account_url}{self.container}/{self.get_relative_object_path(filename)}"

    def get_relative_object_path(self, filename: str) -> str:
        """
        Constructs the relative object path for a file within the Azure Blob Storage container.
        :param filename: The name of the file to be stored in the container.
        :return: Relative object path in the format 'extract_folder/filename'.
        """
        return f"{self.direct_data_folder}/{self.extract_folder}/{filename}"

    def get_headers_from_csv_file(self, object_path: str) -> list:
        """
        This method retrieves the headers of the specified CSV in the order they appear in the file

        :param object_path: The path to the CSV file in the object storage
        :return: A string of ordered column headers of the provided CSV files
        """
        log_message(log_level='Info',
                    message=f'Retrieving CSV headers for {object_path}')

        response_stream: StorageStreamDownloader = self.download_object_to_stream(object_path=object_path)

        try:
            # 1. Read an initial chunk of bytes from the stream.
            # Both Boto3's StreamingBody and Azure's StorageStreamDownloader support a .read(size) method.
            # 4096 bytes (4KB) should be more than enough for any typical header line.
            initial_bytes_chunk = response_stream.read(4096)

            if not initial_bytes_chunk:
                log_message(log_level='Warning', message='Stream was empty or no data in initial chunk.')
                return None

            # 2. Decode these bytes to text (UTF-8 is common for CSVs).
            # We only need the first line.
            try:
                decoded_text_chunk = initial_bytes_chunk.decode('utf-8', errors='replace')
                first_line = decoded_text_chunk.splitlines(keepends=False)[0] if (
                        '\n' in decoded_text_chunk or '\r' in decoded_text_chunk) else decoded_text_chunk
            except UnicodeDecodeError as ude:
                log_message(log_level='Error', message=f"UTF-8 decoding error for headers: {ude}")
                raise ValueError("Could not decode headers as UTF-8.") from ude

            if not first_line.strip():  # Handle cases where the first line might be blank after decoding/stripping
                log_message(log_level='Warning', message='Decoded header line is empty.')
                return None

            # 3. Use io.StringIO to treat the first line string as a file for csv.reader.
            with io.StringIO(first_line) as string_as_file:
                csv_reader = csv.reader(string_as_file)
                try:
                    headers = next(csv_reader)
                    return headers

                except StopIteration:  # Handles case where the line was empty after all
                    log_message(log_level='Warning', message='No CSV headers found in the first line.')
                    return None
        except csv.Error as e:
            log_message(log_level='Error',
                        message=f'Error reading CSV file: {e}',
                        exception=e)
            return None
        except StopIteration:
            log_message(log_level='Error',
                        message='CSV file appears to be empty or corrupted')
            return None

    def get_headers_from_parquet_file(self, object_path: str) -> list:
        """
        Retrieves the headers from a Parquet file stored in Azure Blob Storage.

        :param object_path: The path to the Parquet file in the container.
        :return: A dictionary containing the headers of the Parquet file.
        """
        log_message(log_level='Debug',
                    message=f'Retrieving headers from Parquet file: {object_path}')
        try:
            stream: BytesIO = io.BytesIO()
            response_stream: StorageStreamDownloader = self.download_object_to_stream(object_path=object_path)
            response_stream.readinto(stream)
            stream.seek(0)

            parquet_file: pq.ParquetFile = pq.ParquetFile(stream)  # Load the Parquet file
            schema = parquet_file.schema_arrow
            column_names: list[str] = schema.names
            log_message(log_level='Info',
                        message=f'Retrieved headers from Parquet file: {object_path}')
            return column_names
        except Exception as e:
            log_message(log_level='Error',
                        message=f'Error retrieving headers from Parquet file: {object_path}',
                        exception=e)
            raise e