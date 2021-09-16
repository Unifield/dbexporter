import azure.storage.filedatalake as lake
import os


class DataLake:

    def __init__(self, storage_account_name, storage_account_key):
        self.storage_account_name = storage_account_name
        self.storage_account_key = storage_account_key
        self.service_client = self._set_service_client()

        self.file_system_client = None
        self.directory_client = None

    def _set_service_client(self):
        service_client = lake.DataLakeServiceClient(
            account_url="{}://{}.dfs.core.windows.net".format(
                "https", self.storage_account_name),
            credential=self.storage_account_key)
        return service_client

    def set_file_system_client(self, filesystem_name):
        self.file_system_client = self.service_client.get_file_system_client(
            filesystem_name)

    def set_directory_client(self, dir_name):
        self.directory_client = self.file_system_client.\
            create_directory(dir_name)

    def upload_file(self, file, *loggers):
        general_logger, file_logger = loggers
        head, tail = os.path.split(file)
        try:
            if os.path.isfile(file):
                with self.directory_client.create_file(tail) as file_client:
                    with open(file, 'rb') as f:
                        file_client.upload_data(f, overwrite=True)
            else:
                raise FileNotFoundError(f"File: {file} was not found.")
        except FileNotFoundError as e:
            general_logger.exception(e)
            file_logger.error(f"File: {file} was not uploaded,"
                              f"because it doesn't exist.")
        except Exception as e:
            general_logger.exception(e)
            file_logger.error(f"File: {file} was not uploaded.")
        else:
            file_logger.info(f"File: {file} uploaded.")