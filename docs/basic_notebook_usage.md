# Basic Notebook Usage

## Quick Example

This can exist in either a single or multiple notebook cells:

```python
import noteql

session = noteql.Session('example')
session.load_json('example.json', path_to_list='people', table_name='import', field_name='person')

session.run_sql('''
   drop table if exists people;
   select  person ->> 'id' as person_id, person into people from import;
''')

session.run_sql('select * from people')
```

With the file ```example.json``` in the same directory as the notebook containing:

```json
{"people": [
  {"id": "1", "name": "Kat", "age": "45"},
  {"id": "2", "name": "Fred", "age": "45", someField: "foo"},
  {"id": "3", "name": "Di", "age": "45"}
],
 "other": "not_looked_at"
}
```

This will show you a table of all the data in people data.


## The Session

The session has to be given a name as its first argument. The name should describe the working set of data you are creating. Underneath it creates a schema in postgres. 

So whenever loading data or running anything in sql within this session, it defaults to this schema. The purpose of this is that if you are doing multiple analysis project, with potentially the same table names, this makes sure they are kept seperate. 

The simplist invocation of a session is:

```python
session = noteql.Session('my_working_set_name')
```

There is nothing to stop you having many sessions at the same time.

If you are not running this with the docker setup, you will also have to specify a sqlalchemy dburi: 

```python
# replace user/password/database/localhost with your custom connection options.
session = noteql.Session('my_working_set_name', dburi='postgresql://user:password@localhost/database')
```

Sometimes you want to delete all the data and start again:

```python
# WARNING this will delete all data in the schema!
session = noteql.Session('my_working_set_name', drop_schema=True)
```

## Loading JSON







