import db_exporter.lake_handler as lh
import db_exporter.utils as utils
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

    # Create DataLake services
    try:
        dl = lh.DataLake(args.account_name, args.account_key)
        dl.set_file_system_client(args.filesystem)
        dl.set_directory_client(args.dir)
    except Exception as e:
        general_logger.exception(e)
        general_logger.error(f"Could create DataLake service")
        raise RuntimeError(f"Could create DataLake service")

    # Generate CSVs
    cmds = []

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

    #adding only data for OCP Bangladesh
    command = "psql  dbexporter -w -d ufdb -c \"\COPY (SELECT insert_date,proprietary_instance,journal_code,entry_sequence,description,reference,document_date,posting_date,period,account_code,account_name,account_type,third_parties,book_debit,book_credit,book_currency,functional_debit,functional_credit,functional_currency,reconcile,state from ufdb.t_journal_item WHERE oc = 'OCP' and proprietary_instance IN ('BD1_GOY', 'BD1_COR', 'BD1_PAL')) TO '/home/dbexporter/dbexporter/output/t_journal_item.csv'\" "\
               "DELIMITER '','' CSV HEADER QUOTE '\\\"' " \
               "FORCE QUOTE *;"
    
    #command = "psql dbexporter -w -d ufdb -c \"\COPY (SELECT insert_date,proprietary_instance,journal_code,entry_sequence,description,reference,document_date,posting_date,period,account_code,account_name,account_type,third_parties,book_debit,book_credit,book_currency,functional_debit,functional_credit,functional_currency,reconcile,state FROM ufdb.t_journal_item WHERE oc = 'OCP' AND proprietary_instance IN ('BD1_GOY', 'BD1_COR', 'BD1_PAL')) TO '/home/dbexporter/dbexporter/output/t_journal_item.csv' DELIMITER ',' CSV HEADER QUOTE '\"'"
    t = 't_journal_item'
    output_path = '/home/dbexporter/dbexporter/output/t_journal_item.csv'
    cmds.append((command, output_path, t))

    #adding only data for OCP
    command = "psql  dbexporter -w -d ufdb -c \"\COPY (SELECT proprietary_instance,	journal_code,	entry_sequence,	document_date,	book_currency,	functional_currency,	period,	posting_date,	reference,	third_parties from ufdb.t_journal_item WHERE oc = 'OCP') TO '/home/dbexporter/dbexporter/output/t_journal_item_OCP.csv'\" "\
               "DELIMITER '','' CSV HEADER QUOTE '\\\"' " \
               "FORCE QUOTE *;"
    t = 't_journal_item_ocp'
    output_path = '/home/dbexporter/dbexporter/output/t_journal_item_OCP.csv'
    cmds.append((command, output_path, t))

    # Run export and upload
    output_files = asyncio.run(utils.main_export(cmds, args.num_workers))

    # Upload files
    for file in output_files:
        try:
            if os.path.isfile(file):
                dl.upload_file(file)
            else:
                raise FileNotFoundError(f"File: {file} was not found.")
        except FileNotFoundError as e:
            general_logger.exception(e)
            file_logger.error(f"File: {file} was not uploaded, "
                              f"because it doesn't exist.")
        except Exception as e:
            general_logger.exception(e)
            file_logger.error(f"File: {file} was not uploaded.")
        else:
            file_logger.info(f"File: {file} uploaded.")

    # Set proper ACL for directory and files inside
    try:
        dl.set_acl(args.dir, acl=args.acl, owner=args.owner, group=args.group)
    except Exception as e:
        general_logger.exception(e)

    # Close services
    try:
        dl.close()
    except Exception as e:
        general_logger.exception(e)
