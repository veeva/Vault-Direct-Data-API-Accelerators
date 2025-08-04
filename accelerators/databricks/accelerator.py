import sys

from accelerators.databricks.services.databricks_service import DatabricksService

sys.path.append('.')
from common.scripts import (direct_data_to_object_storage, download_and_unzip_direct_data_files,
                            extract_doc_content, load_data, retrieve_doc_text)
from common.services.aws_s3_service import AwsS3Service
from common.services.vault_service import VaultService
from common.utilities import read_json_file


def main():
    config_filepath: str = "path/to/connector_config.json"
    vapil_settings_filepath: str = "path/to/vapil_settings.json"

    config_params: dict = read_json_file(config_filepath)
    direct_data_params: dict = config_params['direct_data']
    s3_params: dict = config_params['s3']
    databricks_params: dict = config_params['databricks']

    extract_document_content: bool = config_params.get('extract_document_content')
    retrieve_document_text: bool = config_params.get('retrieve_document_text')

    object_storage_root: str = f's3://{s3_params["bucket_name"]}'

    s3_params['convert_to_parquet'] = config_params['convert_to_parquet']
    databricks_params['convert_to_parquet'] = config_params['convert_to_parquet']
    databricks_params['object_storage_root'] = object_storage_root

    s3_service: AwsS3Service = AwsS3Service(s3_params)
    databricks_service: DatabricksService = DatabricksService(databricks_params)
    vault_service: VaultService = VaultService(vapil_settings_filepath)

    direct_data_to_object_storage.run(vault_service=vault_service,
                                      object_storage_service=s3_service,
                                      direct_data_params=direct_data_params)

    download_and_unzip_direct_data_files.run(object_storage_service=s3_service)

    load_data.run(object_storage_service=s3_service,
                  database_service=databricks_service,
                  direct_data_params=direct_data_params)

    if extract_document_content:
        extract_doc_content.run(object_storage_service=s3_service,
                                vault_service=vault_service)

    if retrieve_document_text:
        retrieve_doc_text.run(object_storage_service=s3_service,
                              vault_service=vault_service)


if __name__ == "__main__":
    main()
