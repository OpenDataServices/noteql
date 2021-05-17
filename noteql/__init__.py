import sqlalchemy
import ijson
import os
import json
import decimal
import datetime
import lxml.etree
import urllib.error
import html
import pandas
import jinjasql
import xmltodict
import pyparsing as pp
import subprocess
from IPython.core.magic import Magics, magics_class, line_cell_magic
from IPython.display import display, HTML
from IPython import get_ipython
from noteql.schema_queries import queries
from urllib.parse import urlencode
from jinja2.utils import Markup


LOCAL_DB_MADE = False


def get_engine(dburi):
    if not dburi:
        dburi = os.environ.get("NOTEQL_DBURI")
    return sqlalchemy.create_engine(dburi)


def generate_rows(result, limit):
    for num, row in enumerate(result):
        if num == limit:
            break
        yield [json.dumps(item, indent=2) if isinstance(item, dict) else html.escape(str(item)) for item in row]


def double_quote(word):
    return '"{}"'.format(word)


def put_quotes_round(table_name):
    split = table_name.split(".")
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
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(process.stderr.decode())
    if process.returncode == 0:
        print("Local DB Made")
        LOCAL_DB_MADE = True


def local_db_session():
    create_local_db()
    return Session("postgresql+psycopg2://root@/postgres", "public")


def safe(value):
    return Markup(value)


def identity(value):
    return Markup(f""" "{value.replace('"', '""')}" """)


class Session:
    def __init__(
        self,
        dburi=None,
        schema=None,
        drop_schema=False,
        overwrite=False,
        df_viewer=None,
        df_viewer_kw=None,
        datasette_url=None,
    ):
        self.schema = schema
        self.dburi = dburi
        self.df_viewer = df_viewer
        self.df_viewer_kw = df_viewer_kw or {}

        self.datasette_url = datasette_url

        if datasette_url:
            self.database_type = "datasette"
        else:
            self.engine = get_engine(dburi)
            # test connection
            with self.engine.begin() as connection:
                pass
            self.database_type = self.engine.dialect.name

        param_style = "format"
        if self.database_type == "sqlite":
            param_style = "qmark"
        if self.database_type == "datasette":
            param_style = "named"

        self.jinjarender = jinjasql.JinjaSql(param_style=param_style)
        self.jinjarender.env.filters["s"] = safe
        self.jinjarender.env.filters["i"] = identity

        self.overwrite = overwrite
        if schema:
            with self.engine.begin() as connection:
                if drop_schema:
                    connection.execute("drop schema if exists {} cascade;".format(self.schema))
                connection.execute("create schema if not exists {};".format(self.schema))

        self.ipython = get_ipython()
        self.ipython.register_magics(Noteql)
        self.set()

    def tables(self):
        database_queries = queries.get(self.database_type)
        if not database_queries:
            print(f"Database {self.database_type} not supported")
            return

        sql = database_queries["table"]
        return self.get_dataframe(sql)

    def fields(self, table):
        database_queries = queries.get(self.database_type)
        if not database_queries:
            print(f"Database {self.database_type} not supported")
            return
        sql = database_queries["field"]
        return self.get_dataframe(sql, params=[table])

    def set(self):
        print(
            f'Using db connection {self.dburi or self.datasette_url} {"schema:" + self.schema if self.schema else ""}'
        )
        self.last_set = datetime.datetime.utcnow()

    def get_results(self, sql, limit=-1, params=None):
        with self.engine.begin() as connection:
            if self.schema:
                connection.execute("set local search_path = {};".format(self.schema))
            if params:
                sql_result = connection.execute(sql, params)
            else:
                sql_result = connection.execute(sql)
            if sql_result.returns_rows:
                results = {
                    "data": [row for row in generate_rows(sql_result, limit)],
                    "headers": sql_result.keys(),
                }
                return results
            else:
                return "Success"

    def run_sql(self, sql, limit=20, params=None):
        results = self.get_results(sql, limit, params=params)
        if results == "Success":
            return results

    def create_table(self, table, sql, params=None):
        self.run_sql(
            """
            DROP TABLE IF EXISTS {table};
        """.format(
                table=table
            )
        )

        sql = """
            CREATE TABLE {table}
            AS
            {sql}
        """.format(
            table=table, sql=sql
        )
        return self.run_sql(sql, params=params)

    def get_dataframe(
        self,
        sql,
        index_col=None,
        coerce_float=True,
        params=None,
        parse_dates=None,
        chunksize=None,
    ):
        if self.datasette_url:
            url_params = {"_shape": "array", "sql": sql}
            if params:
                url_params.update(params)

            try:
                return pandas.read_json(f"{self.datasette_url}?{urlencode(url_params)}")
            except urllib.error.HTTPError as e:
                print("Reponse error:", e.fp.read().decode())
                raise

        with self.engine.begin() as connection:
            if self.schema:
                connection.execute("set local search_path = {};".format(self.schema))
            return pandas.read_sql_query(
                sql,
                connection,
                index_col=None,
                coerce_float=True,
                params=params,
                parse_dates=parse_dates,
                chunksize=chunksize,
            )

    def show_dataframe(self, sql, title=None, title_size="h3", **kwargs):
        if title:
            self.show_title(title, title_size)

        df = self.get_dataframe(sql, **kwargs)

        if self.df_viewer:
            df = self.df_viewer(df, **self.df_viewer_kw)
        display(df)

    def show_title(self, title, title_size="h3"):
        display(HTML(f"</br><{title_size}>{title}<{title_size}/>"))

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
            if self.schema:
                connection.execute("set local search_path = {};".format(self.schema))
            # remove quotes when looking at actual table.
            if table_exists(connection, table_name[1:-1], schema_name[1:-1]) and not overwrite and not append:
                print(
                    "WARNING: Table already exists not loading. Set overwrite=True to drop table first or append=True if you want to add rows to existing table"
                )
                return
            if not append:
                connection.execute("drop table if exists {}".format(full_name))

            connection.execute('create table if not exists {}(id serial, "{}" jsonb)'.format(full_name, field_name))

            def load(f):
                if single_cell:
                    connection.execute(
                        "insert into {}({}) values (%s)".format(full_name, field_name),
                        f.read(),
                    )
                    print("Total rows loaded 1")
                    return
                num = -1
                for num, item in enumerate(ijson.items(f, path_to_list + ("." if path_to_list else "") + "item")):
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
            if self.schema:
                connection.execute("set local search_path = {};".format(self.schema))
            if table_exists(connection, table_name[1:-1], schema_name[1:-1]) and not overwrite and not append:
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
                'create table if not exists {}("{}" xml {})'.format(full_name, field_name, extra_create)
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
                    *all_rows,
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
            if self.schema:
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


@magics_class
class Noteql(Magics):
    def get_parsers(self):
        arg_params = (pp.Word(pp.alphanums + "_") + pp.Suppress("=") + pp.QuotedString("'", escQuote="'"))(
            "arg_params"
        )

        create = pp.Keyword("create", caseless=True).suppress() + (
            pp.Word(pp.alphanums + "_") | pp.QuotedString('"', escQuote='"', unquoteResults=False)
        )("create")
        df_arrows = (pp.Word(pp.alphanums + "_") + pp.Keyword("<<").suppress())("df")

        session = (
            pp.Keyword("session", caseless=True).suppress()
            + pp.Optional(pp.Keyword("as", caseless=True).suppress())
            + pp.Word(pp.alphanums + "_")("session")
        )

        commands = [arg_params, create, df_arrows, session]

        for cmd_string in [
            "df",
            "sql",
            "row",
            "rows",
            "col",
            "cols",
            "cell",
            "record",
            "records",
            "headings",
        ]:
            commands.append(
                pp.Word(pp.alphanums + "_")(cmd_string)
                + pp.Suppress("=")
                + pp.Keyword(cmd_string, caseless=True).suppress()
            )

        title = pp.Keyword("title", caseless=True).suppress() + pp.QuotedString("'", escQuote="'")("title")
        nojinja = (pp.Keyword("nojinja", caseless=True) | pp.Keyword("noj", caseless=True))("nojinja")
        show = pp.Keyword("show", caseless=True)("show")
        rest = pp.Word(pp.printables)("rest")

        commands.extend([title, nojinja, show, rest])

        magic_line_parser = pp.ZeroOrMore(pp.Group(pp.MatchFirst(commands)), stopOn=pp.LineEnd())

        cell_parser = pp.OneOrMore(
            pp.SkipTo(
                (pp.LineStart() + pp.Group(pp.Keyword("%%nql") + magic_line_parser)) | pp.StringEnd(),
                include=True,
            )
        )

        return magic_line_parser, cell_parser

    def find_session(self):
        ns = self.shell.user_ns

        latest_session = None

        for key, value in ns.items():
            if isinstance(value, Session):
                if not latest_session or value.last_set > latest_session.last_set:
                    latest_session = value

        if not latest_session:
            print("Need to define noteql seesion")
            return

        return latest_session

    def execute_part(self, parsed_line, sql):
        ns = self.shell.user_ns

        session = self.find_session()

        actions = {}
        arg_params = {}
        jinja = True

        for item in parsed_line:

            if item.getName() == "rest":
                print(f"Syntax error in %%nql not expecting {item}")
                return

            if item.getName() == "session":
                param_name = item[0]
                variable = ns.get(param_name)
                if not variable:
                    print(f"Error in %%nql, variable {param_name} does not exist in your notebook")
                    return
                if not isinstance(variable, Session):
                    print(f"Error in %%nql, variable {param_name} is not a seesion")
                    return
                variable.set()
                session = variable

            if item.getName() == "arg_params":
                param_name, param_value = item
                arg_params[param_name] = param_value

            for command in [
                "df",
                "sql",
                "create",
                "row",
                "rows",
                "col",
                "cols",
                "cell",
                "record",
                "records",
                "headings",
            ]:
                if item.getName() == command:
                    if command in actions:
                        print(f"Error in %%nql, multiple {command.upper()} statements specified")
                        return
                    actions[command] = item[0]

            if item.getName() == "show":
                actions["show"] = True

            if item.getName() == "nojinja":
                jinja = False

            if item.getName() == "title":
                session.show_title(item[0])

        params = None
        if jinja:
            namespace_copy = ns.copy()
            namespace_copy.update(arg_params)
            sql, params = session.jinjarender.prepare_query(sql, namespace_copy)
        if not params:
            params = None

        sql_variable = actions.get("sql")
        if sql_variable:
            if params:
                print("Error in %%nql, can have params if using SQL command")
                return
            ns[sql_variable] = sql

        if not actions:
            actions["show"] = True

        df = None

        df_name = actions.get("df")
        col_name = actions.get("col")
        cols_name = actions.get("cols")
        row_name = actions.get("row")
        rows_name = actions.get("rows")
        cell_name = actions.get("cell")
        record_name = actions.get("record")
        records_name = actions.get("records")
        headings_name = actions.get("headings")

        show = "show" in actions

        if any(
            [
                show,
                df_name,
                col_name,
                cols_name,
                row_name,
                rows_name,
                cell_name,
                record_name,
                records_name,
                headings_name,
            ]
        ):
            df = session.get_dataframe(sql, params=params)
            if df_name:
                ns[df_name] = df

            if col_name:
                ns[col_name] = next(iter(df.to_dict("list").values()))

            if cols_name:
                ns[cols_name] = list(df.to_dict("list").values())

            if row_name:
                ns[row_name] = df.to_dict("split")["data"][0]

            if rows_name:
                ns[rows_name] = df.to_dict("split")["data"]

            if cell_name:
                ns[cell_name] = next(iter(df.to_dict("list").values()))[0]

            if record_name:
                ns[record_name] = df.to_dict("records")[0]

            if records_name:
                ns[records_name] = df.to_dict("records")

            if headings_name:
                ns[headings_name] = list(df.columns)

            if show:
                if session.df_viewer:
                    df = session.df_viewer(df, **session.df_viewer_kw)
                display(df)

        create_name = actions.get("create")

        if create_name:
            session.create_table(create_name, sql, params)

        return df

    @line_cell_magic
    def nql(self, line, cell=None):

        if cell:
            dfs = []
            magic_line_parser, cell_parser = self.get_parsers()

            parsed_line = magic_line_parser.parseString(line)
            parsed_cell = cell_parser.parseString(cell)

            dfs.append(self.execute_part(parsed_line, parsed_cell[0]))

            for parsed_line, sql in zip(parsed_cell[1::2], parsed_cell[2::2]):
                if not sql.strip():
                    print("Error in %%nql, empty sql statement")
                dfs.append(self.execute_part(parsed_line[1:], sql))
            return dfs
        else:
            ns = self.shell.user_ns
            namespace_copy = ns.copy()
            params = None
            session = self.find_session()
            line = line.replace("{", "{{").replace("}", "}}")
            sql, params = session.jinjarender.prepare_query(line, namespace_copy)
            if not params:
                params = None

            return session.get_dataframe(sql, params=params)
