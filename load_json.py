import noteql
import click

@click.command()
@click.option('--path', default='', help='path to list in json')
@click.option('--table', default='', help='tablename')
@click.option('--schema', default='', help='schema')
@click.option('--field', default='', help='fieldname in table')
@click.option('--dburi', default='', help='sqlalchemy db uri')
@click.option('--append', is_flag=True, help='prefix')
@click.option('--overwrite', is_flag=True, help='overwrite')
@click.argument('file_name')
def load_json_command_line(file_name, path, table, schema, field, dburi, append, overwrite):
    if not schema:
        schema = 'public'
    session = noteql.Session(schema, dburi=dburi)
    session.load_json(file_name, path_to_list=path, table_name=table, field_name=field, append=append, overwrite=overwrite)

if __name__ == "__main__":
    load_json_command_line()
