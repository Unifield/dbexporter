import lake_handler as lh
import utils
import os
import shutil
import logging
import asyncio

try:
    import config_local as cfg
except ImportError:
    raise ImportError("Please create config_local.py file based on "
                      "config.py file.")

if __name__ == '__main__':
    # Read arguments
    args = utils.get_arguments()

    # Start logging
    if os.path.isdir(cfg.log_dir):
        utils.set_up_loggers(cfg.log_dir)
    else:
        try:
            os.makedirs(cfg.log_dir)
            utils.set_up_loggers(cfg.log_dir)
        except Exception as e:
            raise RuntimeError(f"Could not create dir {cfg.log_dir}")
    general_logger = logging.getLogger('general_logger')
    file_logger = logging.getLogger('file_logger')

    # Delete files inside output path if exists, else try to create output path
    if os.path.isdir(cfg.output_dir):
        for filename in os.listdir(cfg.output_dir):
            file_path = os.path.join(cfg.output_dir, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                file_logger.error(f"Could not delete old file in path:"
                                  f"{file_path}")
    else:
        try:
            os.makedirs(cfg.output_dir)
        except Exception as e:
            raise RuntimeError(f"Could not create output dir {cfg.output_dir}")

    # Generate CSVs
    cmds = []
    output_file_paths = []

    for t in args.table:
        try:
            command, output_path = utils.prepare_psql_command(
                cfg.database,
                t,
                cfg.user,
                cfg.output_dir,
                ',',
                cfg.schema,
                cfg.pwd
            )
        except Exception as e:
            general_logger.exception(e)
            general_logger.error(f"Could not pre-process table: {t}")
        else:
            cmds.append((command, t))
            output_file_paths.append(output_path)

    asyncio.run(utils.main_export(cmds, cfg.num_workers))

    to_upload = []
    for path in output_file_paths:
        if os.path.isfile(path):
            to_upload.append(path)

    # Upload files
    lh.upload_files(cfg.storage_account_name, cfg.storage_account_key,
                    args.filesystem, args.dir, to_upload)
