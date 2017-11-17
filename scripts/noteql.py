import sqlalchemy
import ijson
import os
import simplejson as json
import decimal
import click
import jinja2
import functools
import lxml.etree
import html
from IPython.core.display import display, HTML




@functools.lru_cache(128)
def get_engine(dburi):
    if not dburi:
        dburi = 'postgres://noteql:noteql@db/noteql'
    return sqlalchemy.create_engine(dburi)


table = jinja2.Template(
'''
<table>
    <thead>
    <tr>
      {% for header in headers %}
        <th style="text-align: left; vertical-align: top">{{ header }}</th>
      {% endfor %}
    </tr>
    </thead>
    <tbody>
      {% for row in data %}
        <tr>
          {% for cell in row %}
              <td style="text-align: left; vertical-align: top">
                <pre>{{ cell }}</pre>
              </td>
          {% endfor %}
        </tr>
      {% endfor %}
    </tbody>
</table>
'''
)

def generate_rows(result, limit):
    for num, row in enumerate(result):
        if num == limit:
            break
        yield [json.dumps(item, indent=2) if isinstance(item, dict) else html.escape(str(item)) for item in row]

class Session:
    def __init__(self, schema, dburi=None, drop=False):
        self.schema = schema
        self.engine = get_engine(dburi)
        with self.engine.begin() as connection:
            if drop:
                connection.execute('drop schema if extist {};'.format(self.schema))
            connection.execute('create schema if not exists {};'.format(self.schema))

    def run_sql(self, sql, limit=20):
        with self.engine.begin() as connection:
            connection.execute('set search_path = {};'.format(self.schema))
            result = connection.execute(sql)
            if result.returns_rows:
                context = {
                    "data": [row for row in generate_rows(result, limit)],
                    "headers": result.keys()
                }
                display(HTML(table.render(context)))
            else:
                return "Success"

    def load_json(self, file_name, path_to_list='', table_name=None, field_name=None, append=False):
        load_json(file_name, path_to_list, self.schema + "." + table_name, field_name, append=False)

    def load_xml(self, file_name, tag, table_name=None, field_name=None, append=False):
        load_xml(file_name, tag, self.schema + "." + table_name, field_name, append=False)


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)

def put_quotes_round(table_name):
    return '.'.join('"{}"'.format(part) for part in table_name.split('.'))

def load_json(file_name, path_to_list='', table_name=None, field_name=None, append=False, dburi=None):
    if not table_name:
        table_name = os.path.split(file_name)[1].split('.')[0]
    if not field_name:
        field_name = path_to_list.split('.')[-1] or 'json'
    table_name = put_quotes_round(table_name)

    engine = get_engine(dburi)

    with engine.begin() as connection:
        if not append:
            connection.execute('drop table if exists {}'.format(table_name))
            connection.execute('create table {}("{}" jsonb)'.format(table_name, field_name))

        with open(file_name) as f:
            for item in ijson.items(f, path_to_list + '.item'):
                connection.execute('insert into {} values (%s)'.format(table_name), json.dumps(item, cls=DecimalEncoder))

def load_xml(file_name, tag, table_name=None, field_name=None, append=False, dburi=None):
    if not table_name:
        table_name = os.path.split(file_name)[1].split('.')[0]
    if not field_name:
        field_name = tag
    table_name = put_quotes_round(table_name)
    engine = get_engine(dburi)

    with engine.begin() as connection:
        if not append:
            connection.execute('drop table if exists {}'.format(table_name))
            connection.execute('create table {}("{}" xml)'.format(table_name, field_name))

        with open(file_name, 'rb') as f:
            context = lxml.etree.iterparse(f, tag=tag)
            for action, elem in context:
                connection.execute('insert into {} values (%s)'.format(table_name), lxml.etree.tostring(elem, encoding='unicode'))


@click.command()
@click.option('--path', default='', help='path to list in json')
@click.option('--table', default='', help='tablename')
@click.option('--field', default='', help='fieldname in table')
@click.option('--dburi', default='', help='sqlalchemy db uri')
@click.option('--append', is_flag=True, help='prefix')
@click.argument('filename')
def load_json_command_line(filename, path, table, field, append, dburi):
    load_json(filename, path, table, field, append, dburi)


if __name__ == "__main__":
    load_json_command_line()
