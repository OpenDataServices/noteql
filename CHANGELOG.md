# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]


## [0.5.0] - 2021-05-22

- First Public Release. Below outlines initial *stable* features.

### Added

- `%%nql` and `%nql` magics.
- Multiple `%%nql` lines in one cell.
- Commands for assigning outputs of cell magic:
  - DF for dataframes
  - HEADINGS for headings
  - COL/ROW for first row/column as list.
  - COLS/ROWS for all rows/columns as list of list.
  - RECORD for dict of first row.
  - RECORDS for dicts of all rows.
  - CELL for value of first field in first row.
  - SQL for sql written in the cell.
- JinjaSQL support:
  - `sql` filter for inserting raw sql
  - `ident` filter for inserting field/table names.
  - `fields` filter for inserting list of field names.
- Other commands:
  - `CREATE <tablename>` Drop table if exists and creates table with the result of the SQL query.
  - `VIEW <view name>` Drop view if exists and creates view using the SQL query.
  - SHOW for showing the dataframe even if you are doing another operation. 
  - TITLE for adding title to dataframe output and for naming sheets in excel output.
  - SESSION for changing noteql `Session`.
  - `CSV 'filename'` output results to CSV file.
  - `EXCEL 'filename'` for changing saving output to xlsx file.  Uses TITLE to name the sheet.
- Schema functions, only for SQLite and PostgreSQL:
  - `session.tables()` to list tables.
  - `session.fields('tablename')` to list fields within table.
- `session.load_dataframe('dataframe', 'table_name')` load dataframe into database.
