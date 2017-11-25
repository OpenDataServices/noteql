import sqlalchemy
import ijson
import os
import json
import decimal
import click
import jinja2
import functools
import lxml.etree
import html
import pandas
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

def double_quote(word):
    return '"{}"'.format(word)

def put_quotes_round(table_name):
    split = table_name.split('.')
    schema_name = None
    if len(split) > 1:
        return (double_quote(split[0]), double_quote('.'.join(split[1:])))
    else:
        return (None, double_quote(split[0]))
        

def table_exists(connection, table, schema):
    result = connection.execute("select exists(select * from information_schema.tables where table_name=%s and table_schema=%s)", table, schema)
    return result.fetchall()[0][0]


class Session:
    def __init__(self, schema, dburi=None, drop_schema=False, overwrite=False):
        self.schema = schema
        self.dburi = dburi
        self.engine = get_engine(dburi)
        self.overwrite = overwrite
        with self.engine.begin() as connection:
            if drop_schema:
                connection.execute('drop schema if exists {} cascade;'.format(self.schema))
            connection.execute('create schema if not exists {};'.format(self.schema))

    def run_sql(self, sql, limit=20):
        with self.engine.begin() as connection:
            connection.execute('set local search_path = {};'.format(self.schema))
            result = connection.execute(sql)
            if result.returns_rows:
                context = {
                    "data": [row for row in generate_rows(result, limit)],
                    "headers": result.keys()
                }
                display(HTML(table.render(context)))
            else:
                return "Success"

    def get_dataframe(self, sql, index_col=None, coerce_float=True, params=None, parse_dates=None, chunksize=None):
        with self.engine.begin() as connection:
            connection.execute('set local search_path = {};'.format(self.schema))
            return pandas.read_sql_query(sql, connection, index_col=None, coerce_float=True, params=None, parse_dates=None, chunksize=None)

    def load_json(self, file_name, path_to_list='', table_name=None, field_name=None, append=False, overwrite=None):
        if overwrite is None:
            overwrite = self.overwrite
        if not table_name:
            table_name = os.path.split(file_name)[1].split('.')[0]
        if not field_name:
            field_name = path_to_list.split('.')[-1] or 'json'
        schema_name, table_name = put_quotes_round(table_name)
        if schema_name is None:
            schema_name = double_quote(self.schema)
            full_name = table_name
        else:
            full_name = schema_name + "." + table_name

        with self.engine.begin() as connection:
            connection.execute('set local search_path = {};'.format(self.schema))
            ## remove quotes when looking at actual table.
            if table_exists(connection, table_name[1:-1], schema_name[1:-1]) and not overwrite and not append:
                print("WARNING: Table already exists not loading. Set overwrite=True to drop table first or append=True if you want to add rows to existing table")
                return
            if not append:
                connection.execute('drop table if exists {}'.format(full_name))

            connection.execute('create table if not exists {}("{}" jsonb)'.format(full_name, field_name))

            with open(file_name) as f:
                num = -1
                for num, item in enumerate(ijson.items(f, path_to_list + '.item')):
                    connection.execute('insert into {} values (%s)'.format(full_name), json.dumps(item, cls=DecimalEncoder))
                print("Total rows loaded {}".format(num + 1))


    def load_xml(self, file_name, tag, table_name=None, field_name=None, append=False, overwrite=None):
        if overwrite is None:
            overwrite = self.overwrite
        if not table_name:
            table_name = os.path.split(file_name)[1].split('.')[0]
        if not field_name:
            field_name = tag
        schema_name, table_name = put_quotes_round(table_name)
        if schema_name is None:
            schema_name = double_quote(self.schema)
            full_name = table_name
        else:
            full_name = schema_name + "." + table_name

        with self.engine.begin() as connection:
            connection.execute('set local search_path = {};'.format(self.schema))
            if table_exists(connection, table_name[1:-1], schema_name[1:-1]) and not overwrite and not append:
                print("WARNING: Table already exists not loading. Set overwrite=True to drop table first or append=True if you want to add rows to existing table")
                return
            if not append:
                connection.execute('drop table if exists {}'.format(full_name))

            connection.execute('create table if not exists {}("{}" xml)'.format(full_name, field_name))

            with open(file_name, 'rb') as f:
                context = lxml.etree.iterparse(f, tag=tag)
                num = 0
                for action, elem in context:
                    num += 1
                    connection.execute('insert into {} values (%s)'.format(full_name), lxml.etree.tostring(elem, encoding='unicode'))
                print("Total rows loaded {}".format(num))


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)



