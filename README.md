# noteql
Doing sql in notebooks.

Quick and easy way to get analysing data in postgreql/sqlite.

Adds `%%nql` and `%nql` magics.

## QuickStart

See [example notebook](https://github.com/OpenDataServices/noteql/blob/main/Basic%20Example.ipynb)

## Install

To install in a notebook:

```
!pip install noteql > pip.log
```

## Setup

You will need an [SQLAlchemy URL](https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls) to connect. A session object needs to be made like the following.

```python
import noteql

session = noteql.Session('postgresql://user:password@localhost/dbname')

# This will create a schema in postgres if it does not exist and change the search_path to it.
# session = noteql.Session('postgresql://user:password@localhost/dbname', 'myschema')

# If you are using colab notebooks this will install postgres in the notebook and provide the session for it.
# session = noteql.local_db_session()
```

The session object contains methods for programmatic access to the library including some helpers not found in the magic `%%nql` commands.  Also, by making any Session object it will install the magics.


## Magic Usage

### Pandas Dataframe

Run the following to display a dataframe. It is recommended to add a limit if selecting from large table.

```python
%%nql

SELECT * FROM mytable LIMIT 10
```
or
```python
%nql SELECT * FROM mytable LIMIT 10
```

Many options are available on the `%%nql` line for example you can assign the output dataframe to a python variable buy the special command `DF`.

```python
%%nql mydataframe=DF

SELECT * FROM mytable LIMIT 10
```

or you can use assignment for the `%nql` magic.

```python
mydataframe = %nql SELECT * FROM mytable
```

The above commands will not show the dataframe you just made. The `SHOW` command can be added so you can save and show in one step.

```python
%%nql mydataframe=DF SHOW

SELECT * FROM mytable
```

### Table/View Creation

If you have a writable database and you want to save the results of a query you can use the `CREATE` command. This will drop the table first so be warned! This is so that the steps can be repeated without error. This will fail if there are multiple SQL statements in the cell separated by `;`.

```python
%%nql CREATE mynewtable

SELECT * FROM mytable
```

If you do not want a permentent table just a database view you can do:

```python
%%nql CREATE mynewview

SELECT * FROM mytable
```


### Multiple Statements in one cell

Sometimes putting all your SQL statements in separate cells can make your notebook look messy or hard to follow. If you start any line in your SQL with `%%nql` then it will work the same as using it at the top of a new cell.

This example will create a new table and then preview it in one cell:

```python
%%nql CREATE mynewtable
SELECT * FROM mytable

%%nql SHOW
--show my new table
SELECT * FROM mynewtable LIMIT 10
```

If you put multiple SHOW commands in one cell then the results will be shown after each other.

### Passing Parameters and Templating.

When you need to pass varibles to your query you can do it using [jinjasql](https://github.com/sripathikrishnan/jinjasql).  Your local variables are passed as the context to render the SQL statement.

So if you define some varibales:

```python
my_name = "david"
ages = [2,3,4]
name_field = "name"
select_statement = "SELECT * FROM people"
```

You can now use them in your queries:

```python
%%nql

SELECT * FROM people WHERE name = {{my_name}}
```

NOTE: you should not put quotes round your string variiables. Parameters are passed to the database in a safe paramatarised way.

You can use [Jinja syntax](https://jinja.palletsprojects.com/en/3.0.x/templates/) for loops and conditionals:

```python
%%nql

SELECT * FROM people WHERE age in
(
  {% for age in ages %}
    {% if not loop.first %}, {% endif%}
    {{age}}
  {% endfor}
)
```
This generates an `in` clause using the `ages` list.

There is also a shortcut in jinjasql which is equilent to the above.

```python
%%nql

SELECT * FROM people WHERE age in {{ ages | inclause }}
```

This libarary adds a filter `s` which means you can put raw sql in variables into your query.


```python
%%nql

{{ select_statement | s }} WHERE age in {{ ages | inclause }}
```

Note: you have to be sure your sql is escaped correctly and that there is no untrusted input.


There is also `i` standing for identifier. Use this when putting in field, schema or table names.

```python
%%nql

SELECT {{name_field | i}} FROM people
```

This will put `"` round your table names to make sure they will be understood by the database.


### Assigning outputs to python data.

Dataframes are a useful output but sometimes you want access to standard python datatypes, especially if you are going to use them to template for other queries.

There are many commands to get out the data you need, for example `var=RECORDS` returns a list of dictionaries to a variable:

```python
%%nql all_people=RECORDS

SELECT * FROM people LIMIT 2
```

Variable `all_people` will now contain something like `[{"name": "david", "age": 56},  {"name": "fred", "age": 46}]`

There is a singular version `RECORD` which would get out just the first result i.e `{"name": "david", "age": 56}`.

You can also assign to multiple variables in one go:

```python
%%nql all_people=RECORDS person=RECORD

SELECT * FROM people LIMIT 2
```

There are COLS and COL which return list(s) of the columns:

```python
%%nql all_column=COLS column=COL

SELECT * FROM people LIMIT 2
```

`all_column` will contain `[["david", "fred"], [56, 46]]` and `column` will contain just `["david", "fred"]`

There are ROWS and ROW return list(s) of the rows:

```python
%%nql all_rows=ROWS row=ROW

SELECT * FROM people LIMIT 2
```

`all_rows` will contain `[["david", 56], ["fred", 46]]` and `row` will contain just `["david", 56]`


There is `CELL` which just gets out the first cell of the first row.

```python
%%nql my_name=CELL

SELECT * FROM people LIMIT 2
```

`my_name` will just be the string `'david'`

HEADINGS gives you th column headings of the output.

```python
%%nql headings=HEADINGS

SELECT * FROM people LIMIT 2
```

The variable headings will now contain `["name", "age"]`


### Save the SQL in a variable

Sometimes you want to write a some sql that would be useful in other queries. The SQL command saves the sql as a string in a variable.
This saves 'SELECT * FROM mytable' in a variable `somesql`:

```python
%%nql somesql=SQL
SELECT * FROM mytable
```

Then you can substitute it using templating:

```python
%%nql
{{somesql | s}} WHERE name='david'
```
The filter `s` is needed to mark the variable as SQL and not just a string. 

This could be done by multiple calls to %%nql in one cell and is useful if you want to see the results of a query while also using it in another query.

```python
%%nql all_names=SQL SHOW
SELECT distinct(name) FROM mytable

%%nql
SELECT * FROM other_table WHERE name in ({{ all_names | s }})
```

This will ouput two tables one with the distinct names and the other with the results of the second query.  This is also useful for seeing the output of WITH (CTE) at the same time as results.

```python
%%nql all_names=SQL SHOW
SELECT distinct(name) FROM mytable

%%nql
with mytable as ({{mytable | s}})

SELECT * FROM other_table join mytable using(name)
```

### Multiple sessions in one notebook

If you define another session then that session will be used instead:

```python
session2 = noteql.Session('postgresql://user:password@localhost/dbname')

-- this will use session2
%nql SELECT 1;
```

To swap between sessions you can `use` on the session you want:

```python
session.use()
```
or use the `SESSION` command:

```python
%%nql SESSION session

SELECT * FROM mytable
```

## Using the session directly.

### Tables and Fields

The `session.tables` function gives you all the tables in the specified database (or schema) and the `session.fields` gives the fields in a table.

```python
session.tables()

session.fields('mytable')
```

TODO document how to use of the load commands on the session.
