import asyncio
import os
import db_exporter.utils as utils

try:
    import config_local as cfg
except ImportError:
    raise ImportError("Please create config_local.py file based on "
                      "config.py file.")


def create_export_cmd(table_name, db_name, output_dir):
    output_path = os.path.join(output_dir, f"{db_name}.sql")
    cmd = f"pg_dump -t 'ufdb.{table_name}' --schema-only --no-owner " \
          f"{db_name} > {output_path}"
    return cmd, output_path


if __name__ == '__main__':
    args = utils.get_arguments(parser_type='schema')

    if not os.path.isdir(args.schema_output_dir):
        try:
            os.makedirs(args.schema_output_dir)
        except Exception as e:
            raise RuntimeError(f"Could not create output dir {args.output_dir}")

    cmds = []
    for t in args.table:
        cmd, output_path = create_export_cmd(t, args.database,
                                             args.schema_output_dir)
        cmds.append((cmd, output_path, t))

    asyncio.run(utils.main_export(cmds, args.num_workers))
