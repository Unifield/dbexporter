import lake_handler as lh
import utils
import os
import shutil
import logging
import asyncio


if __name__ == '__main__':
    # Read arguments
    args = utils.get_arguments()

    # Start logging
    if os.path.isdir(args.log_dir):
        utils.set_up_loggers(args.log_dir)
    else:
        try:
            os.makedirs(args.log_dir)
            utils.set_up_loggers(args.log_dir)
        except Exception as e:
            raise RuntimeError(f"Could not create dir {args.log_dir}")
    general_logger = logging.getLogger('general_logger')
    file_logger = logging.getLogger('file_logger')

    # Delete files inside output path if exists, else try to create output path
    if os.path.isdir(args.output_dir):
        for filename in os.listdir(args.output_dir):
            file_path = os.path.join(args.output_dir, filename)
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
            os.makedirs(args.output_dir)
        except Exception as e:
            raise RuntimeError(f"Could not create output dir {args.output_dir}")

    # Generate CSVs
    cmds = []
    output_file_paths = []

    for t in args.table:
        try:
            command, output_path = utils.prepare_psql_command(
                args.database,
                t,
                args.db_user,
                args.output_dir,
                ',',
                args.schema,
                args.db_pwd
            )
        except Exception as e:
            general_logger.exception(e)
            general_logger.error(f"Could not pre-process table: {t}")
        else:
            cmds.append((command, output_path, t))

    asyncio.run(utils.main_export(cmds, args.num_workers))

    # Upload files
    lh.upload_files(args.account_name, args.account_key,
                    args.filesystem, args.dir, utils.EXPORTED_FILES)
