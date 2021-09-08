import azure.storage.filedatalake as lake
import logging
import os


def upload_files(storage_account_name: str,
                 storage_account_key: str,
                 file_system_name: str,
                 directory_name: str,
                 files_to_upload: list) -> None:

    general_logger = logging.getLogger('general_logger')
    file_logger = logging.getLogger('file_logger')
    try:
        service_client = lake.DataLakeServiceClient(
            account_url="{}://{}.dfs.core.windows.net".format(
                "https", storage_account_name),
            credential=storage_account_key)
        file_system_client = service_client.get_file_system_client(
            file_system_name)
        directory_client = file_system_client.create_directory(directory_name)
    except Exception as e:
        general_logger.exception(e)
        msg = f"Could not connect to account: {storage_account_name}."
        general_logger.debug(msg)
        raise RuntimeError(msg)
    else:
        for file in files_to_upload:
            head, tail = os.path.split(file)
            try:
                if os.path.isfile(file):
                    with directory_client.create_file(tail) as file_client:
                        with open(file) as f:
                            file_client.upload_data(f.read(), overwrite=True)
                else:
                    raise FileNotFoundError(f"File: {file} was not found.")
            except FileNotFoundError as e:
                general_logger.exception(e)
                file_logger.error(f"File: {file} was not processed")
            else:
                file_logger.info(f"File: {file} processed")
