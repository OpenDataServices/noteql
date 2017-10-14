import sqlalchemy
import ijson
import os
import json
import decimal
import click

engine = sqlalchemy.create_engine('postgres://sqljson:sqljson@db/sqljson')


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)

def put_quotes_round(table_name):
    return '.'.join('"{}"'.format(part) for part in table_name.split('.'))

def load_json(file_name, path_to_list='', table_name=None, field_name=None, append=False):
    if not table_name:
        table_name = os.path.split(file_name)[1].split('.')[0]
    if not field_name:
        field_name = path_to_list.split('.')[-1] or 'json'
    table_name = put_quotes_round(table_name)

    connection = engine.connect()
    if not append:
        connection.execute('drop table if exists {}'.format(table_name))
        connection.execute('create table {}("{}" jsonb)'.format(table_name, field_name))

    with open(file_name) as f:
        for item in ijson.items(f, path_to_list + '.item'):
            connection.execute('insert into {} values (%s)'.format(table_name), json.dumps(item, cls=DecimalEncoder))


@click.command()
@click.option('--path-to-list', default='', help='path to list in json')
@click.option('--table-name', default='', help='tablename')
@click.option('--field-name', default='', help='fieldname in table')
@click.option('--append', is_flag=True, help='prefix')
@click.argument('filename')
def load_json_command_line(filename, path_to_list, table_name, field_name, append):
    load_json(filename, path_to_list, table_name, field_name, append)

if __name__ == "__main__":
    load_json_command_line()
