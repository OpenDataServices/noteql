import sqlalchemy
import ijson
import os
import json
import decimal
import functools
import lxml.etree
import html
import pandas
import xmltodict

LOCAL_DB_MADE = False


@functools.lru_cache(128)
def get_engine(dburi):
    if not dburi:
        dburi = os.environ.get("NOTEQL_DBURI")
        if not dburi:
            dburi = "postgres://noteql:noteql@db/noteql"
    return sqlalchemy.create_engine(dburi)


def generate_rows(result, limit):
    for num, row in enumerate(result):
        if num == limit:
            break
        yield [
            json.dumps(item, indent=2)
            if isinstance(item, dict)
            else html.escape(str(item))
            for item in row
        ]


def double_quote(word):
    return '"{}"'.format(word)


def put_quotes_round(table_name):
    split = table_name.split(".")
    schema_name = None
    if len(split) > 1:
        return (double_quote(split[0]), double_quote(".".join(split[1:])))
    else:
        return (None, double_quote(split[0]))


def table_exists(connection, table, schema):
    result = connection.execute(
        "select exists(select * from information_schema.tables where table_name=%s and table_schema=%s)",
        table,
        schema,
    )
    return result.fetchall()[0][0]


def create_local_db():
    global LOCAL_DB_MADE
    if LOCAL_DB_MADE:
        return
    import subprocess

    cmd = """
        pip install --upgrade psycopg2-binary > pip.log
        echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections
        sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
        wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
        DEBIAN_FRONTEND=noninteractive sudo apt-get update
        DEBIAN_FRONTEND=noninteractive sudo apt-get -y install postgresql
        service postgresql start
        sudo -u postgres psql -c "CREATE USER root WITH SUPERUSER"
    """
    process = subprocess.run(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    print(process.stderr.decode())
    if process.returncode == 0:
        print("Local DB Made")
        LOCAL_DB_MADE = True


def local_db_session():
    create_local_db()
    return Session("public", "postgresql+psycopg2://root@/postgres")


class Session:
    def __init__(self, schema, dburi=None, drop_schema=False, overwrite=False):
        self.schema = schema
        self.dburi = dburi
        self.engine = get_engine(dburi)
        self.overwrite = overwrite
        with self.engine.begin() as connection:
            if drop_schema:
                connection.execute(
                    "drop schema if exists {} cascade;".format(self.schema)
                )
            connection.execute("create schema if not exists {};".format(self.schema))

    def get_results(self, sql, limit=-1):
        with self.engine.begin() as connection:
            connection.execute("set local search_path = {};".format(self.schema))
            sql_result = connection.execute(sql)
            if sql_result.returns_rows:
                results = {
                    "data": [row for row in generate_rows(sql_result, limit)],
                    "headers": sql_result.keys(),
                }
                return results
            else:
                return "Success"

    def run_sql(self, sql, limit=20):
        results = self.get_results(sql, limit)
        if results == "Success":
            return results

    def create_table(self, table, sql):
        sql = '''
            DROP TABLE IF EXISTS {table};
            CREATE TABLE {table}
            AS
            {sql}
        '''.format(table=table, sql=sql)
        return self.run_sql(sql)

    def get_dataframe(
        self,
        sql,
        index_col=None,
        coerce_float=True,
        params=None,
        parse_dates=None,
        chunksize=None,
    ):
        with self.engine.begin() as connection:
            connection.execute("set local search_path = {};".format(self.schema))
            return pandas.read_sql_query(
                sql,
                connection,
                index_col=None,
                coerce_float=True,
                params=None,
                parse_dates=None,
                chunksize=None,
            )

    def show_dataframe(self, sql, title=None, title_size='h3', **kwargs):
        from IPython.display import display
        from IPython.display import HTML

        if title:
            display(HTML(f'</br></br><{title_size}>{title}<{title_size}/>'))

        display(self.get_dataframe(sql, **kwargs))
        

    def load_json(
        self,
        json_file,
        path_to_list="",
        single_cell=False,
        table_name=None,
        field_name=None,
        append=False,
        overwrite=None,
    ):
        file_object = False
        if hasattr(json_file, "read"):
            file_object = True
            if hasattr(json_file, "name"):
                file_name = json_file.name
            else:
                file_name = "import"
        else:
            file_name = os.path.split(json_file)[1]

        if overwrite is None:
            overwrite = self.overwrite
        if not table_name:
            table_name = file_name.split(".")[0]
        if not field_name:
            field_name = path_to_list.split(".")[-1] or "json"
        schema_name, table_name = put_quotes_round(table_name)
        if schema_name is None:
            schema_name = double_quote(self.schema)
            full_name = table_name
        else:
            full_name = schema_name + "." + table_name

        with self.engine.begin() as connection:
            connection.execute("set local search_path = {};".format(self.schema))
            ## remove quotes when looking at actual table.
            if (
                table_exists(connection, table_name[1:-1], schema_name[1:-1])
                and not overwrite
                and not append
            ):
                print(
                    "WARNING: Table already exists not loading. Set overwrite=True to drop table first or append=True if you want to add rows to existing table"
                )
                return
            if not append:
                connection.execute("drop table if exists {}".format(full_name))

            connection.execute(
                'create table if not exists {}(id serial, "{}" jsonb)'.format(
                    full_name, field_name
                )
            )

            def load(f):
                if single_cell:
                    connection.execute(
                        "insert into {}({}) values (%s)".format(full_name, field_name),
                        f.read(),
                    )
                    print("Total rows loaded 1")
                    return
                num = -1
                for num, item in enumerate(
                    ijson.items(
                        f, path_to_list + ("." if path_to_list else "") + "item"
                    )
                ):
                    connection.execute(
                        "insert into {}({}) values (%s)".format(full_name, field_name),
                        json.dumps(item, cls=DecimalEncoder),
                    )
                print("Total rows loaded {}".format(num + 1))

            if file_object:
                load(json_file)
            else:
                with open(json_file) as f:
                    load(f)

    def load_xml(
        self,
        file_name,
        tag,
        table_name=None,
        field_name=None,
        context=None,
        context_name="context",
        json_field=None,
        append=False,
        overwrite=None,
    ):
        if overwrite is None:
            overwrite = self.overwrite
        if not table_name:
            table_name = os.path.split(file_name)[1].split(".")[0]
        if not field_name:
            field_name = tag
        schema_name, table_name = put_quotes_round(table_name)
        if schema_name is None:
            schema_name = double_quote(self.schema)
            full_name = table_name
        else:
            full_name = schema_name + "." + table_name

        with self.engine.begin() as connection:
            connection.execute("set local search_path = {};".format(self.schema))
            if (
                table_exists(connection, table_name[1:-1], schema_name[1:-1])
                and not overwrite
                and not append
            ):
                print(
                    "WARNING: Table already exists not loading. Set overwrite=True to drop table first or append=True if you want to add rows to existing table"
                )
                return
            if not append:
                connection.execute("drop table if exists {}".format(full_name))

            extra_create = ""
            extra_params = ""
            if context:
                extra_create += ", {} JSONB".format(context_name)
                extra_params += ", %s"
            if json_field:
                extra_create += ", {} JSONB".format(json_field)
                extra_params += ", %s"

            connection.execute(
                'create table if not exists {}("{}" xml {})'.format(
                    full_name, field_name, extra_create
                )
            )

            all_rows = []
            with open(file_name, "rb") as f:
                tree = lxml.etree.iterparse(f, tag=tag)
                num = 0
                for action, elem in tree:
                    num += 1
                    xml_string = lxml.etree.tostring(elem, encoding="unicode")
                    args = [xml_string]
                    if context:
                        args.append(json.dumps(context))
                    if json_field:
                        json_data = json.dumps(
                            xmltodict.parse(
                                xml_string,
                                force_cdata=True,
                            )
                        )
                        args.append(json_data)
                    all_rows.append(tuple(args))
            if all_rows:
                connection.execute(
                    "insert into {} values (%s {})".format(full_name, extra_params),
                    *all_rows
                )
            print("Total rows loaded {}".format(num))

    def load_dataframe(
        self,
        dataframe,
        table_name,
        if_exists="fail",
        index=True,
        index_label=None,
        chunksize=None,
        dtype=None,
        method=None,
    ):
        with self.engine.begin() as connection:
            connection.execute("set local search_path = {};".format(self.schema))
            dataframe.to_sql(
                table_name,
                connection,
                if_exists=if_exists,
                index=index,
                index_label=index_label,
                chunksize=chunksize,
                dtype=dtype,
                method=method,
            )

    def add_flatten_function(self):
        self.run_sql(
            """
            -- Reference: https://www.postgresql.org/docs/current/queries-with.html
            CREATE OR REPLACE FUNCTION flatten (jsonb)
                RETURNS TABLE (
                    path text,
                    object_property integer,
                    array_item integer)
                LANGUAGE sql
                IMMUTABLE
                STRICT
                PARALLEL SAFE
                AS $$
                WITH RECURSIVE t (
                    key,
                    value,
                    object_property,
                    array_item
                ) AS (
                    SELECT
                        j.key,
                        j.value,
                        1,
                        0
                    FROM
                        jsonb_each($1) AS j
                    UNION ALL (
                        WITH prev AS (
                            SELECT
                                *
                            FROM
                                t -- recursive reference to query "t" must not appear more than once
                        )

                        SELECT
                            prev.key || '/' || next.key,
                            next.value,
                            1,
                            0
                        FROM
                            prev,
                            jsonb_each(prev.value) next
                        WHERE
                            jsonb_typeof(prev.value) = 'object'

                        UNION ALL

                        SELECT
                            prev.key,
                            next.value,
                            0,
                            1
                        FROM
                            prev,
                            jsonb_array_elements(prev.value) next
                        WHERE
                            jsonb_typeof(prev.value) = 'array'
                            AND jsonb_typeof(prev.value -> 0) = 'object'
                    )
                )
                SELECT
                    key AS path,
                    object_property,
                    array_item
                FROM
                    t;

            $$;
            """
        )


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)
