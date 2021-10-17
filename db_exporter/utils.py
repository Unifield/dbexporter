import argparse
import asyncio
import logging
import os
import datetime
import sys
import jinja2
import psycopg2
from psycopg2 import sql

try:
    import config_local as cfg
except ImportError:
    raise ImportError("Please create config_local.py file based on "
                      "config.py file.")


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
    # ACL settings
    parser.add_argument('-o', '--owner', type=str,
                        default=cfg.owner, action='store')
    parser.add_argument('-g', '--group', type=str,
                        default=cfg.group, action='store')
    parser.add_argument('-acl', '--acl', type=str,
                        default=cfg.acl, action='store')

    if parser_type == 'schema':
        parser.add_argument('-so', '--schema-output-dir', type=str,
                            default='/home/dbexporter/dbexporter/'
                                    'schema_exporter/output',
                            action='store')
    return parser.parse_args()


def get_list_of_columns(dbname: str, user: str, table_name: str,
                        schema: str = None):

    with psycopg2.connect(f"dbname={dbname} user={user}") as conn:
        with conn.cursor() as cur:
            if schema:
                query = sql.SQL("""
                SELECT
                    column_name 
                FROM
                    information_schema.columns
                WHERE
                    table_name = %s AND table_schema = %s
                ORDER BY ordinal_position 
                """)

                cur.execute(query, (table_name, schema,))
                rec = cur.fetchall()
                columns = [r[0] for r in rec]
                return columns
            else:

                query = sql.SQL("""
                SELECT
                    column_name 
                FROM
                    information_schema.columns
                WHERE
                    table_name = %s 
                ORDER BY ordinal_position 
                """)

                cur.execute(query, (table_name,))
                rec = cur.fetchall()
                columns = [r[0] for r in rec]
                return columns


def generate_select_statement(table_name: str,
                              columns: list, schema: str = None):
    with open('db_exporter/select_template.sql') as f:
        template = jinja2.Template(f.read())
        rendered = template.render(
            table=table_name,
            columns=columns,
            schema=schema
            )
        return rendered


def prepare_psql_command(db_name, table_name, user, output_path, delimiter,
                         schema, pwd):

    columns = get_list_of_columns(db_name, user, table_name, schema)
    if not columns:
        raise ValueError("Columns should not be empty list.")
    select_statement = generate_select_statement(table_name, columns, schema)
    output_path = f"{os.path.join(output_path, table_name)}.csv"

    base_sql = f"\COPY ({select_statement}) TO '{output_path}' " \
               f"DELIMITER '{delimiter}' CSV HEADER QUOTE '\\\"' " \
               f"FORCE QUOTE *;"

    if pwd:
        command = f"PGPASSWORD={pwd} psql {user} -w -d " \
                  f"{db_name} -c \"{base_sql}\""
    else:
        command = f"psql {user} -w -d " \
                  f"{db_name} -c \"{base_sql}\""
    return command, output_path


async def export_csv(arg):
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
        return output_path

    if stderr:
        general_logger.error(f'[stderr]\n{stderr.decode()}')
        file_logger.error(f"Table: {table_name} was not processed.")


async def export_worker(q, output):
    general_logger = logging.getLogger('general_logger')

    while True:
        try:
            code = await q.get()
            res = await export_csv(code)
            if res:
                output.append(res)
        except Exception as e:
            general_logger.exception(e)
        finally:
            q.task_done()


async def main_export(cmds, n_workers):
    q = asyncio.Queue()
    output = list()
    workers = [asyncio.create_task(export_worker(q, output)) for _ in
               range(n_workers)]

    for cmd in cmds:
        await q.put(cmd)

    await q.join()  # wait for all tasks to be processed
    for worker in workers:
        worker.cancel()
    await asyncio.gather(*workers, return_exceptions=True)
    return output
