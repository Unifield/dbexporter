"""
When you add some new config option,
please add it in utils.get_arguments().
"""
# data_lake_storage
storage_account_name = ""
storage_account_key = ""
file_system_name = ""  # default file_system in data lake
directory_name = ""  # default directory where to upload

# db_login
user = ""
pwd = ""
database = ""
schema = ""

# misc
log_dir = ""
output_dir = ""
num_workers = 8  # number of async workers
table_list = []

# acl
owner = ""
group = ""
acl = ""
