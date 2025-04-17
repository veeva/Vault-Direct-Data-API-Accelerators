import os
import time
from common.services.object_storage_service import ObjectStorageService

import boto3
from botocore.exceptions import ClientError
from common.utilities import log_message


class AwsS3Service(ObjectStorageService):
    def __init__(self, parameters: dict):
        super().__init__()
        self.iam_role_arn: str = parameters['iam_role_arn']
        self.bucket_name: str = parameters['bucket_name']
        self.direct_data_folder: str = parameters['direct_data_folder']
        self.archive_filepath: str = parameters['archive_filepath']
        self.extract_folder: str = parameters['extract_folder']
        credentials: dict = self.retrieve_credentials(step='retrieve')
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken']
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

    def head_object(self, key: str):
        log_message(log_level='Debug',
                    message=f'Retrieving object metadata from {self.bucket_name}/{key}')
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            log_message(log_level='Info',
                        message=f'Object metadata successfully retrieved from {self.bucket_name}/{key}',
                        context=None)
            return response
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                log_message(log_level='Error',
                            message=f'File not found on {self.bucket_name}/{key}',
                            exception=e,
                            context=None)
            raise e

    def get_object(self, key: str):
        log_message(log_level='Debug',
                    message=f'Downloading object from {self.bucket_name}/{key}')
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            log_message(log_level='Info',
                        message=f'Object downloaded successfully from {self.bucket_name}/{key}',
                        context=None)
            return response
        except ClientError as e:
            log_message(log_level='Error',
                        message=f'Error getting object from S3 {self.bucket_name}/{key}',
                        exception=e,
                        context=None)
            raise e

    def put_object(self, key: str, body):
        log_message(log_level='Debug',
                    message=f'Uploading to {self.bucket_name}/{key}')
        try:
            self.s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=body)
            log_message(log_level='Info',
                        message=f'Uploaded successfully to {self.bucket_name}/{key}',
                        context=None)
        except ClientError as e:
            log_message(log_level='Error',
                        message=f'Error putting object to {self.bucket_name}/{key}',
                        exception=e,
                        context=None)
            raise e

    def create_multipart_upload(self, key: str) -> dict:
        log_message(log_level='Debug',
                    message=f'Creating multipart upload for bucket: {self.bucket_name} and directory: {key}')
        try:
            response = self.s3_client.create_multipart_upload(Bucket=self.bucket_name, Key=key)
            log_message(log_level='Info',
                        message=f'Multipart upload created successfully for bucket: {self.bucket_name} and directory: {key}',
                        context=None)
            return response
        except ClientError as e:
            log_message(log_level='Error',
                        message=f'Error creating multipart upload to S3 bucket: {self.bucket_name} and directory: {key}',
                        exception=e,
                        context=None)
            raise e

    def upload_part(self, key: str, upload_id: str, part_number: int, body: bytes):
        log_message(log_level='Debug',
                    message=f'Uploading part {part_number} to bucket: {self.bucket_name} and directory: {key}')
        try:
            response = self.s3_client.upload_part(Bucket=self.bucket_name, Key=key, Body=body, UploadId=upload_id, PartNumber=part_number)
            log_message(log_level='Info',
                        message=f'Part {part_number} uploaded successfully to bucket: {self.bucket_name} and directory: {key}'
                                f'with ETag: {response["ETag"]}',
                        context=None)
            return response
        except ClientError as e:
            log_message(log_level='Error',
                        message=f'Error uploading part {part_number} to S3 bucket: {self.bucket_name} and directory: {key}',
                        exception=e,
                        context=None)
            raise e

    def complete_multipart_upload(self, key: str, upload_id: str, parts: list):
        log_message(log_level='Debug',
                    message=f'Completing multipart upload for bucket: {self.bucket_name} and directory: {key}')
        try:
            response = self.s3_client.complete_multipart_upload(Bucket=self.bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})
            log_message(log_level='Info',
                        message=f'Multipart upload completed successfully for bucket: {self.bucket_name} and directory: {key}')
            return response
        except ClientError as e:
            log_message(log_level='Error',
                        message=f'Error completing multipart upload to S3 bucket: {self.bucket_name} and directory: {key}',
                        exception=e)
            raise e

    def abort_multipart_upload(self, key: str, upload_id: str):
        log_message(log_level='Debug',
                    message=f'Aborting multipart upload for bucket: {self.bucket_name} and directory: {key}')
        try:
            response = self.s3_client.abort_multipart_upload(Bucket=self.bucket_name, Key=key, UploadId=upload_id)
            log_message(log_level='Info',
                        message=f'Multipart upload aborted successfully for bucket: {self.bucket_name} and directory: {key}')
            return response
        except ClientError as e:
            log_message(log_level='Error',
                        message=f'Error aborting multipart upload to S3 bucket: {self.bucket_name} and directory: {key}',
                        exception=e)
            raise e

    def download_file(self, key: str, filename: str):
        log_message(log_level='Debug',
                    message=f'Downloading object from {self.bucket_name}/{key}')
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            response = self.s3_client.download_file(Bucket=self.bucket_name, Key=key, Filename=filename)
            log_message(log_level='Info',
                        message=f'Object downloaded to file: {filename} from {self.bucket_name}/{key}')
            return response
        except ClientError as e:
            log_message(log_level='Error',
                        message=f'Error getting object from S3 {self.bucket_name}/{key}',
                        exception=e)
            raise e
