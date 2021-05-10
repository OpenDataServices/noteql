# noteql
Doing sql in notebooks.

Quick and easy way to get analysing data in postgreql/sqlite.

Adds `%%nql` and `%nql` magics.

## Install

To install in a notebook:

```
!pip install git+https://github.com/opendataservices/noteql@main#egg=noteql > pip.log
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

The session object contains methods for programmatic access to the library including some helpers not found in the magic `%%nql` commands.  It also installs these magics.


## Magic Usage

### Pandas Dataframe 

Simple display dataframe of results. It is recommended to add a limit if selecting from large table.

```python
%%nql

SELECT * FROM mytable LIMIT 10
```
or 
```python
%nql SELECT * FROM mytable LIMIT 10
```

Many options are available on the `%%nql` line for example `DF` to put results in a dataframe.

```python
%%nql DF mydataframe

SELECT * FROM mytable LIMIT 10
```

or you can use assignment for the `%nql` magic.

```python
mydataframe = %nql SELECT * FROM mytable
```

The above commands will not show the dataframe you just made. The `SHOW` command can be added so you can save and show in one step.

```python
%%nql DF mydataframe SHOW

SELECT * FROM mytable
```

### Table Creation 

If you have a writable database and you want to save the results of a query to a table use the `CREATE` command. This will drop the table first so be warned! This is so that the steps can be repeated without error. This will fail if there are multiple SQL statements in the cell separated by `;`

```python
%%nql CREATE mynewtable

SELECT * FROM mytable
```

### Parameter passing. 

The simplest is to use python format strings. The library has access to your local variables to format them. This works for both `%%nql` and `%nql`.

```python
myvariable = 24
%nql SELECT * FROM mytable WHERE id = '{myvariable}'
```

This is bad practice when you have untrusted input and will not be ideal if you have strings that need escaping. So there is a `PARAMS` command to pass a parameterized variable to the db. This uses the style of params native to the database driver, the following examples are for postgresql.

In another cell you can define a variable like:

```python
my_params = [1,2]
my_named_params = {"name": "value"}
```

Then parse give them to the command like:

```python
%%nql PARAMS my_params

SELECT * FROM mytable where id in (%s, %s)
```

For named params:

```python
%%nql PARAMS my_named_params

SELECT * FROM mytable where name = %(name)s
```

For simple named params there is a special syntax on the `%%nql` line so you can add param_name='param_value'.  This only support strings with quotes (using SQL escaping) or local variables without quotes:

```python
%%nql name='value' 

SELECT * FROM mytable where name = %(name)s
```

For SQLite the params look like:

```sql
--positional
SELECT * FROM mytable where name = ?

--named
SELECT * FROM mytable where name = :name
```

### Multiple Statements in one cell

Sometime putting all your SQL statements in separate cells can make your notebook looks messy or hard to follow. If you start any line in your SQL with `%%nql` then it will work the same as using it at the top of the cell.

This example will create a new table and them preview it in one cell:

```python
%%nql CREATE mynewtable
SELECT * FROM mytable 

%%nql SHOW
--show my new table
SELECT * FROM mynewtable LIMIT 10
```

If you put multiple SHOW commands in one cell then the results will be shown after each other.

### Multiple sessions in one notebook

If you define another session then that session will be used from then on:

```python
session2 = noteql.Session('postgresql://user:password@localhost/dbname')

-- this will use session2
%nql SELECT 1;
```

To use the other session you can either call `use` on the session you want:

```python
session.use()
```
or use the SESSION command:

```python 
%%nql SESSION session

SELECT * FROM mytable
```

## Using the session directly.

All functions in the magics are availible on the session object too.

TODO document how to use these and the load commands on the session.
