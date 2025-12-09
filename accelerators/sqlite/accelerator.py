import sys

from accelerators.sqlite.services.sqlite_service import SqliteService

sys.path.append('.')
from accelerators.sqlite.scripts import download_direct_data_file, unzip_direct_data_file, load_data
from common.services.vault_service import VaultService
from common.utilities import read_json_file


def main():
    config_filepath: str = "path/to/connector_config.json"
    vapil_settings_filepath: str = "path/to/vapil_settings.json"

    config_params: dict = read_json_file(config_filepath)
    direct_data_params: dict = config_params['direct_data']
    local_params: dict = config_params['local']
    sqlite_params: dict = config_params['sqlite']

    sqlite_service: SqliteService = SqliteService(sqlite_params)
    vault_service: VaultService = VaultService(vapil_settings_filepath)

    download_direct_data_file.run(vault_service=vault_service,
                                  direct_data_params=direct_data_params,
                                  local_params=local_params)

    unzip_direct_data_file.run(local_params=local_params)

    load_data.run(direct_data_params=direct_data_params,
                  local_params=local_params,
                  sqlite_service=sqlite_service)

if __name__ == "__main__":
    main()
