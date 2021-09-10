import argparse
import asyncio
import logging
import os
import datetime
import config_local as cfg


def set_up_loggers(root_path):
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    general_file = os.path.join(root_path, f"{timestamp}_general")
    file_log = os.path.join(root_path, f"{timestamp}_file_processing")

    # Set up general logger
    general_logger = logging.getLogger('general_logger')
    general_logger.setLevel(logging.INFO)
    log_handler1 = logging.FileHandler(general_file)
    log_handler1.setFormatter(formatter)
    general_logger.addHandler(log_handler1)

    # Set up file processing logger
    file_processing_logger = logging.getLogger('file_logger')
    file_processing_logger.setLevel(logging.INFO)
    log_handler2 = logging.FileHandler(file_log)
    log_handler2.setFormatter(formatter)
    file_processing_logger.addHandler(log_handler2)


def get_arguments():
    parser = argparse.ArgumentParser(prog='dbexporter')
    parser.add_argument('-fs', '--filesystem', type=str,
                        default=cfg.file_system_name, action='store')
    parser.add_argument('-d', '--dir', type=str,
                        default=cfg.directory_name, action='store')
    parser.add_argument('-t', '--table', type=list,
                        default=cfg.table_list, action='store')

    return parser.parse_args()


def prepare_psql_command(db_name, table_name, user, output_path, delimiter,
                         schema, pwd):
    output_path = f"{os.path.join(output_path, table_name)}.csv"
    if schema:
        base_sql = f"\COPY {schema}.{table_name} TO '{output_path}' (" \
                   f"DELIMITER '{delimiter}')"
    else:
        base_sql = f"\COPY {table_name} TO '{output_path}' (" \
                   f"DELIMITER '{delimiter}')"

    if pwd:
        command = f"PGPASSWORD={pwd} sudo -u {user} psql {user} -w -d " \
                  f"{db_name} -c \"{base_sql}\""
    else:
        command = f"sudo -u {user} psql {user} -w -d " \
                  f"{db_name} -c \"{base_sql}\""
    return command, output_path


async def export_csv(arg):
    general_logger = logging.getLogger('general_logger')

    cmd, table_name = arg
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)

    stdout, stderr = await proc.communicate()

    general_logger.info(f'[{cmd!r} exited with {proc.returncode}]')
    if stdout:
        general_logger.info(f'[stdout]\n{stdout.decode()}')
    if stderr:
        general_logger.error(f'[stderr]\n{stderr.decode()}')


async def download_worker(q):
    while True:
        code = await q.get()
        await export_csv(code)
        q.task_done()


async def main_export(cmds, n_workers):
    q = asyncio.Queue()
    workers = [asyncio.create_task(download_worker(q)) for _ in
               range(n_workers)]

    for cmd in cmds:
        await q.put(cmd)

    await q.join()  # wait for all tasks to be processed
    for worker in workers:
        worker.cancel()
    await asyncio.gather(*workers, return_exceptions=True)
