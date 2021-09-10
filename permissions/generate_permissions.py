import jinja2

try:
    import config_local as cfg
except ImportError:
    raise ImportError("Please create config_local.py file based on "
                      "config.py file.")

if __name__ == '__main__':
    with open('permission_template.sql') as tmp:
        template = jinja2.Template(tmp.read())
        rendered = template.render(database=cfg.database,
                                   user=cfg.user,
                                   schema=cfg.schema,
                                   tables=cfg.table_list)

    with open('generated_permissions.sql', 'w') as out:
        out.write(rendered)