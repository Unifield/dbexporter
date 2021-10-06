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

    def upload_file(self, file):
        head, tail = os.path.split(file)
        with self.directory_client.create_file(tail) as file_client:
            with open(file, 'rb') as f:
                file_client.upload_data(f, overwrite=True)

    def set_acl(self, directory, **kwargs):
        self.directory_client.set_access_control(**kwargs)
        for path in self.file_system_client.get_paths():
            if not path.is_directory:
                d, f = os.path.split(path.name)
                if d == directory:
                    self.directory_client.get_file_client(f).set_access_control(
                        **kwargs
                    )

    def close(self):
        self.directory_client.close()
        self.file_system_client.close()
        self.service_client.close()
