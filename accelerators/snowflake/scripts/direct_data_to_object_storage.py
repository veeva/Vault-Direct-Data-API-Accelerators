import sys

sys.path.append('.')
from common.utilities import log_message
from common.api.model.response.direct_data_response import DirectDataResponse
from common.api.model.response.vault_response import VaultResponse
from common.services.aws_s3_service import AwsS3Service
from common.services.vault_service import VaultService


def _handle_multipart_upload(s3_service: AwsS3Service, vault_service: VaultService,
                             direct_data_item: DirectDataResponse.DirectDataItem, object_key: str):
    log_message(log_level='Info',
                message=f'Handling multipart upload')
    try:
        multipart_upload_response: dict = s3_service.create_multipart_upload(
            key=object_key)

        upload_id: str = multipart_upload_response['UploadId']
        parts: list = []

        for file_part in direct_data_item.filepart_details:
            file_part_number: int = file_part.filepart
            download_response: VaultResponse = vault_service.download_direct_data_file(
                name=file_part.name,
                filepart=file_part_number)

            upload_response: dict = s3_service.upload_part(
                key=object_key,
                upload_id=upload_id,
                part_number=file_part_number,
                body=download_response.binary_content)

            part_info: dict = {'PartNumber': file_part_number, 'ETag': upload_response['ETag']}
            parts.append(part_info)

        s3_service.complete_multipart_upload(
            key=object_key,
            upload_id=upload_id,
            parts=parts)

        log_message(log_level='Info',
                    message=f'Multipart upload completed with upload ID: {upload_id}')

    except Exception as e:
        # Abort the multipart upload in case of an error
        s3_service.abort_multipart_upload(
            key=object_key,
            upload_id=upload_id)

        log_message(log_level='Error',
                    message=f'Multipart upload aborted with upload ID: {upload_id}',
                    exception=e)
        raise e


def run(vault_service: VaultService, s3_service: AwsS3Service, direct_data_params: dict):
    log_message(log_level='Info',
                message=f'---Executing direct_data_to_object_storage.py---')
    try:
        # List the Direct Data files of the specified extract type and time window
        extract_type: str = f"{direct_data_params['extract_type']}_directdata"
        start_time: str = direct_data_params['start_time']
        stop_time: str = direct_data_params['stop_time']
        list_direct_data_files_response: DirectDataResponse = vault_service.retrieve_available_direct_data_files(
            extract_type=extract_type,
            start_time=start_time,
            stop_time=stop_time
        )

        # Download the latest Direct Data file in the response, and upload to file storage.
        direct_data_item: DirectDataResponse.DirectDataItem = list_direct_data_files_response.data[-1]

        # Exit if there are no records in the Direct Data extract
        if direct_data_item.record_count == 0:
            log_message(log_level='Info',
                        message=f'No records in the Direct Data extract.')
            return

        # Put Direct Data file to S3 Bucket if there is only one file part
        object_key: str = f"{s3_service.direct_data_folder}/{direct_data_item.filename}"
        if direct_data_item.fileparts == 1:

            download_response: VaultResponse = vault_service.download_direct_data_file(
                name=direct_data_item.filepart_details[0].name,
                filepart=1)

            s3_service.put_object(
                key=object_key,
                body=download_response.binary_content)

        # Create multi-part upload if Direct Data File has multiple parts
        else:
            _handle_multipart_upload(s3_service=s3_service,
                                     vault_service=vault_service,
                                     direct_data_item=direct_data_item,
                                     object_key=object_key)


    except Exception as exception:
        log_message(log_level='Error',
                    message=f'Error retrieving Direct Data files from Vault'
                            f' and uploading to S3 bucket',
                    exception=exception)
