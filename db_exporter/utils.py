import argparse
import asyncio
import logging
import os
import datetime
import sys

try:
    import config_local as cfg
except ImportError:
    raise ImportError("Please create config_local.py file based on "
                      "config.py file.")

EXPORTED_FILES = []


def set_up_loggers(root_path):
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    general_file = os.path.join(root_path, f"{timestamp}_general")
    file_log = os.path.join(root_path, f"{timestamp}_file_processing")

    # Set up general logger
    general_logger = logging.getLogger('general_logger')
    general_logger.setLevel(logging.INFO)

    file_handler_general = logging.FileHandler(general_file)
    file_handler_general.setFormatter(formatter)

    general_logger.addHandler(file_handler_general)

    # Set up file processing logger
    file_processing_logger = logging.getLogger('file_logger')
    file_processing_logger.setLevel(logging.INFO)

    output_handler = logging.StreamHandler(sys.stdout)
    output_handler.setLevel(logging.INFO)
    output_handler.setFormatter(formatter)

    file_handler_files = logging.FileHandler(file_log)
    file_handler_files.setFormatter(formatter)

    file_processing_logger.addHandler(file_handler_files)
    file_processing_logger.addHandler(output_handler)


def get_arguments(parser_type='basic'):
    """
    Allows all config settings to be provided via cmd as arguments.
    :return: Arguments provided via cmd
    """
    parser = argparse.ArgumentParser(prog='dbexporter')
    # Arguments for DataLake handling
    parser.add_argument('-an', '--account-name', type=str,
                        default=cfg.storage_account_name, action='store')
    parser.add_argument('-key', '--account-key', type=str,
                        default=cfg.storage_account_key, action='store')
    parser.add_argument('-fs', '--filesystem', type=str,
                        default=cfg.file_system_name, action='store')
    parser.add_argument('-d', '--dir', type=str,
                        default=cfg.directory_name, action='store')

    # DB arguments
    parser.add_argument('-user', '--db-user', type=str,
                        default=cfg.user, action='store')
    parser.add_argument('-pwd', '--db-pwd', type=str,
                        default=cfg.pwd, action='store')
    parser.add_argument('-db', '--database', type=str,
                        default=cfg.database, action='store')
    parser.add_argument('-s', '--schema', type=str,
                        default=cfg.schema, action='store')

    # Other settings
    parser.add_argument('-log', '--log-dir', type=str,
                        default=cfg.log_dir, action='store')
    parser.add_argument('-out', '--output-dir', type=str,
                        default=cfg.output_dir, action='store')
    parser.add_argument('-w', '--num-workers', type=int,
                        default=cfg.num_workers, action='store')
    parser.add_argument('-t', '--table', nargs='+',
                        default=cfg.table_list, action='store')

    if parser_type == 'schema':
        parser.add_argument('-so', '--schema-output-dir', type=str,
                            default='/home/dbexporter/dbexporter/'
                                    'schema_exporter/output',
                            action='store')
    return parser.parse_args()


def prepare_psql_command(db_name, table_name, user, output_path, delimiter,
                         schema, pwd):
    output_path = f"{os.path.join(output_path, table_name)}.csv"
    if schema:
        base_sql = f"\COPY {schema}.{table_name} TO '{output_path}' " \
                   f"DELIMITER '{delimiter}' CSV HEADER;"
    else:
        base_sql = f"\COPY {table_name} TO '{output_path}' " \
                   f"DELIMITER '{delimiter}' CSV HEADER;"

    if pwd:
        command = f"PGPASSWORD={pwd} psql {user} -w -d " \
                  f"{db_name} -c \"{base_sql}\""
    else:
        command = f"psql {user} -w -d " \
                  f"{db_name} -c \"{base_sql}\""
    return command, output_path


async def export_csv(arg):
    global EXPORTED_FILES
    general_logger = logging.getLogger('general_logger')
    file_logger = logging.getLogger('file_logger')

    cmd, output_path, table_name = arg
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)

    stdout, stderr = await proc.communicate()

    general_logger.info(f'[{cmd!r} exited with {proc.returncode}]')
    if stdout:
        general_logger.info(f'[stdout]\n{stdout.decode()}')
        file_logger.info(f"Table: {table_name} was processed.")
        EXPORTED_FILES.append(output_path)
    if stderr:
        general_logger.error(f'[stderr]\n{stderr.decode()}')
        file_logger.error(f"Table: {table_name} was not processed.")


async def export_worker(q):
    while True:
        code = await q.get()
        await export_csv(code)
        q.task_done()


async def main_export(cmds, n_workers):
    q = asyncio.Queue()
    workers = [asyncio.create_task(export_worker(q)) for _ in
               range(n_workers)]

    for cmd in cmds:
        await q.put(cmd)

    await q.join()  # wait for all tasks to be processed
    for worker in workers:
        worker.cancel()
    await asyncio.gather(*workers, return_exceptions=True)
