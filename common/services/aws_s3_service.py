import io
import json
import csv
import os
import time
from typing import BinaryIO

import pyarrow.parquet as pq
import pyarrow.fs

from botocore.client import BaseClient
from botocore.response import StreamingBody

from common.services.object_storage_service import ObjectStorageService

import boto3
from botocore.exceptions import ClientError
from common.utilities import log_message


class AwsS3Service(ObjectStorageService):
    def __init__(self, parameters: dict):
        super().__init__(parameters=parameters)
        self.iam_role_arn: str = parameters['iam_role_arn']
        self.bucket_name: str = parameters['bucket_name']
        self.credentials: dict = self.retrieve_credentials(step='retrieve')
        self.s3_client: BaseClient = boto3.client(
            's3',
            aws_access_key_id=self.credentials['AccessKeyId'],
            aws_secret_access_key=self.credentials['SecretAccessKey'],
            aws_session_token=self.credentials['SessionToken']
        )

    def retrieve_credentials(self, step: str) -> dict:
        sts_client = boto3.client('sts')

        if self.credentials:
            current_time = time.time()
            expiration_time = self.credentials['Expiration'].timestamp()

            if current_time < expiration_time:
                return self.credentials

        try:
            self.credentials = sts_client.assume_role(
                RoleArn=self.iam_role_arn,
                RoleSessionName=f"Direct-Data-{step}-Session"
            ).get('Credentials')

            return self.credentials

        except ClientError as e:
            log_message(log_level='Error',
                        message=f'Error assuming role: {self.iam_role_arn}',
                        exception=e,
                        context=None)
            raise e

    def check_if_object_exists(self, object_path: str):
        log_message(log_level='Debug',
                    message=f'Checking if object exists: {self.bucket_name}/{object_path}')
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=object_path)
            log_message(log_level='Info',
                        message=f'Object exists: {self.bucket_name}/{object_path}')
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                log_message(log_level='Error',
                            message=f'File not found on {self.bucket_name}/{object_path}',
                            exception=e)
            raise e


    def upload_object(self, object_path: str, data: BinaryIO) -> None:
        """
        Uploads an object to the specified S3 bucket using a memory-efficient,
        multipart-capable method.

        :param object_path: Path to the object in the storage.
        :param data: The binary stream (file-like object) to be uploaded.
        :return: None on success. Raises ClientError on failure.
        """
        log_message(log_level='Debug',
                    message=f'Uploading to {self.bucket_name}/{object_path}')
        try:
            self.s3_client.upload_fileobj(
                Fileobj=data,
                Bucket=self.bucket_name,
                Key=object_path
            )

            log_message(log_level='Info',
                        message=f'Uploaded successfully to {self.bucket_name}/{object_path}')

        except ClientError as e:
            log_message(log_level='Error',
                        message=f'Error uploading object to {self.bucket_name}/{object_path}',
                        exception=e)
            raise e

    def create_multipart_upload(self, object_path: str) -> dict:
        log_message(log_level='Debug',
                    message=f'Creating multipart upload for bucket: {self.bucket_name} and directory: {object_path}')
        try:
            response = self.s3_client.create_multipart_upload(Bucket=self.bucket_name, Key=object_path)
            log_message(log_level='Info',
                        message=f'Multipart upload created successfully for bucket: {self.bucket_name} and directory: {object_path}')
            return response
        except ClientError as e:
            log_message(log_level='Error',
                        message=f'Error creating multipart upload to S3 bucket: {self.bucket_name} and directory: {object_path}',
                        exception=e)
            raise e

    def upload_part(self, object_path: str, multipart_upload_response: dict, part_number: int, data: bytes) -> dict:
        log_message(log_level='Debug',
                    message=f'Uploading part {part_number} to bucket: {self.bucket_name} and directory: {object_path}')
        try:
            response = self.s3_client.upload_part(Bucket=self.bucket_name,
                                                  Key=object_path,
                                                  Body=data,
                                                  UploadId=multipart_upload_response['UploadId'],
                                                  PartNumber=part_number)
            log_message(log_level='Info',
                        message=f'Part {part_number} uploaded successfully to bucket: {self.bucket_name} and directory: {object_path}'
                                f'with ETag: {response["ETag"]}')
            return {'PartNumber': part_number, 'ETag': response['ETag']}
        except ClientError as e:
            log_message(log_level='Error',
                        message=f'Error uploading part {part_number} to S3 bucket: {self.bucket_name} and directory: {object_path}',
                        exception=e)
            raise e

    def complete_multipart_upload(self, object_path: str, multipart_upload_response: dict, parts: list):
        log_message(log_level='Debug',
                    message=f'Completing multipart upload for bucket: {self.bucket_name} and directory: {object_path}')
        try:
            response = self.s3_client.complete_multipart_upload(Bucket=self.bucket_name, Key=object_path,
                                                                UploadId=multipart_upload_response['UploadId'], MultipartUpload={'Parts': parts})
            log_message(log_level='Info',
                        message=f'Multipart upload completed successfully for bucket: {self.bucket_name} and directory: {object_path}')
            return response
        except ClientError as e:
            log_message(log_level='Error',
                        message=f'Error completing multipart upload to S3 bucket: {self.bucket_name} and directory: {object_path}',
                        exception=e)
            raise e

    def abort_multipart_upload(self, object_path: str, multipart_upload_response: dict):
        log_message(log_level='Debug',
                    message=f'Aborting multipart upload for bucket: {self.bucket_name} and directory: {object_path}')
        try:
            response = self.s3_client.abort_multipart_upload(Bucket=self.bucket_name, Key=object_path, UploadId=multipart_upload_response['UploadId'])
            log_message(log_level='Info',
                        message=f'Multipart upload aborted successfully for bucket: {self.bucket_name} and directory: {object_path}')
            return response
        except ClientError as e:
            log_message(log_level='Error',
                        message=f'Error aborting multipart upload to S3 bucket: {self.bucket_name} and directory: {object_path}',
                        exception=e)
            raise e

    def download_object_bytes(self, object_path: str):
        log_message(log_level='Debug',
                    message=f'Downloading object from {object_path}')
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=object_path)
            log_message(log_level='Info',
                        message=f'Object downloaded successfully from {self.bucket_name}/{object_path}')
            return response['Body'].read()
        except ClientError as e:
            log_message(log_level='Error',
                        message=f'Error getting object from S3 {self.bucket_name}/{object_path}',
                        exception=e)
            raise e

    def download_object_to_stream(self, object_path: str) -> StreamingBody:
        log_message(log_level='Debug',
                    message=f'Downloading object from {object_path}')
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=object_path)
            log_message(log_level='Info',
                        message=f'Object downloaded successfully from {self.bucket_name}/{object_path}')
            return response['Body']
        except ClientError as e:
            log_message(log_level='Error',
                        message=f'Error getting object from S3 {self.bucket_name}/{object_path}',
                        exception=e)
            raise e

    def download_object_to_local(self, object_path: str, output_path: str):
        log_message(log_level='Debug',
                    message=f'Downloading object from {self.bucket_name}/{object_path}')
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            directory = os.path.dirname(output_path)

            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)

            self.s3_client.download_file(Bucket=self.bucket_name, Key=object_path, Filename=output_path)
            log_message(log_level='Info',
                        message=f'Object downloaded to file: {output_path}')
        except ClientError as e:
            log_message(log_level='Error',
                        message=f'Error getting object from S3: {self.bucket_name}/{object_path}',
                        exception=e)
            raise e

    def get_full_object_path(self, filename: str) -> str:
        """
        Constructs the full S3 object path for a given filename.
        :param filename: The name of the file in the S3 bucket.
        """
        return f"s3://{self.bucket_name}/{self.get_relative_object_path(filename)}"

    def get_relative_object_path(self, filename: str) -> str:
        """
        Constructs the S3 object URL for a given filename.
        :param filename: The name of the file in the S3 bucket.
        """
        return f"{self.direct_data_folder}/{self.extract_folder}/{filename}"

    def get_headers_from_csv_file(self, object_path: str) -> list:
        log_message(log_level='Info',
                    message=f'Retrieving CSV headers for {object_path}')

        try:
            # This query tells S3 to return only the first line of the file.
            # The FileHeaderInfo='None' parameter treats the first line as the header.
            response = self.s3_client.select_object_content(
                Bucket=self.bucket_name,
                Key=object_path,
                ExpressionType='SQL',
                Expression="SELECT * FROM s3object s LIMIT 1",
                InputSerialization={'CSV': {'FileHeaderInfo': 'NONE'}},
                OutputSerialization={'CSV': {}}
            )

            # Collect the payload and decode it all at once
            payload_chunks = []
            for event in response['Payload']:
                if 'Records' in event:
                    payload_chunks.append(event['Records']['Payload'])

            if not payload_chunks:
                log_message(log_level='Warning', message='S3 Select returned no records.')
                return None

            # Decode the complete result to avoid splitting characters
            full_payload = b''.join(payload_chunks).decode('utf-8')

            # Use csv.reader to correctly parse the header line
            reader = csv.reader(io.StringIO(full_payload))
            headers = next(reader)
            return headers

        except Exception as e:
            log_message(log_level='Error',
                        message=f'Error using S3 Select on {object_path}',
                        exception=e)
            raise e

    def get_headers_from_parquet_file(self, object_path: str) -> list:
        log_message(log_level='Debug',
                    message=f'Retrieving headers from Parquet file: {object_path}')
        try:
            response = self.s3_client.select_object_content(
                Bucket=self.bucket_name,
                Key=object_path,
                ExpressionType='SQL',
                Expression=f'SELECT * FROM s3object s LIMIT 1',
                InputSerialization={'Parquet': {}},
                OutputSerialization={'JSON': {}}
            )

            payload_chunks = []
            for event in response['Payload']:
                if 'Records' in event:
                    payload_chunks.append(event['Records']['Payload'])

            if payload_chunks:
                full_payload_bytes = b''.join(payload_chunks)

                # 3. Decode the complete payload at once
                records = full_payload_bytes.decode('utf-8')

                first_record_str = records.split('\n')[0]
                if first_record_str:
                    first_record = json.loads(first_record_str)
                    # The keys of the first JSON record are the column headers
                    column_names = list(first_record.keys())
            log_message(log_level='Info',
                        message=f'Retrieved headers from Parquet file: {object_path}')
            return column_names
        except Exception as e:
            log_message(log_level='Error',
                        message=f'Error retrieving headers from Parquet file: {object_path}',
                        exception=e)
            raise e