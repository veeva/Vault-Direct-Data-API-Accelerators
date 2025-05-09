import sys

from common.utilities import read_json_file
from accelerators.snowflake.scripts import direct_data_to_object_storage, extract_doc_content
from accelerators.snowflake.scripts import download_and_unzip_direct_data_files
from accelerators.snowflake.scripts import load_data
from accelerators.snowflake.services.snowflake_service import SnowflakeService

sys.path.append('.')
from common.services.aws_s3_service import AwsS3Service
from common.services.vault_service import VaultService


def main():
    config_filepath: str = "path/to/connector_config.json"
    vapil_settings_filepath: str = "path/to/vapil_settings.json"

    config_params: dict = read_json_file(config_filepath)
    convert_to_parquet: bool = config_params['convert_to_parquet']
    extract_document_content: bool = config_params['extract_document_content']

    direct_data_params: dict = config_params['direct_data']
    s3_params: dict = config_params['s3']
    snowflake_params: dict = config_params['snowflake']

    vault_service: VaultService = VaultService(vapil_settings_filepath)
    s3_service: AwsS3Service = AwsS3Service(s3_params)
    snowflake_service: SnowflakeService = SnowflakeService(snowflake_params)

    direct_data_to_object_storage.run(vault_service=vault_service,
                                      s3_service=s3_service,
                                      direct_data_params=direct_data_params)

    download_and_unzip_direct_data_files.run(s3_service=s3_service,
                                             convert_to_parquet=convert_to_parquet)

    load_data.run(s3_service=s3_service,
                  snowflake_service=snowflake_service,
                  direct_data_params=direct_data_params,
                  convert_to_parquet=convert_to_parquet)

    if extract_document_content:
        extract_doc_content.run(s3_service=s3_service,
                                 vault_service=vault_service,
                                 convert_to_parquet=convert_to_parquet)



if __name__ == "__main__":
    main()
