queries = {
    "postgresql": {
        "table": """
        SELECT
          c.relname as "Name",
          CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
          pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) as "Size",
          fields.fields
        FROM pg_catalog.pg_class c
        JOIN
            (SELECT a.attrelid, string_agg(a.attname::text, ', ') fields
            FROM (select * from pg_catalog.pg_attribute order by attnum) a
            WHERE a.attnum > 0 AND NOT a.attisdropped
            group by 1) fields on c.oid = fields.attrelid

             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r','p','v','m','S','f','')
              AND n.nspname <> 'pg_catalog'
              AND n.nspname <> 'information_schema'
              AND n.nspname !~ '^pg_toast'
          AND pg_catalog.pg_table_is_visible(c.oid)
        ORDER BY 1,2;
     """,
        "field": """
            WITH getoid as (
            SELECT c.oid
            FROM pg_catalog.pg_class c

            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace

            WHERE c.relname = %s
              AND pg_catalog.pg_table_is_visible(c.oid)
            )

            SELECT a.attname "Field",
              pg_catalog.format_type(a.atttypid, a.atttypmod) "Type",
              pg_catalog.col_description(a.attrelid, a.attnum) "Description"
            FROM pg_catalog.pg_attribute a
            WHERE a.attrelid in (select oid from getoid) AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum;
      """,
    },
    "sqlite": {
        "table": """
        SELECT
            name, sql
        FROM
            sqlite_master
        WHERE
            name NOT LIKE 'sqlite_%';
        """,
        "field": """
             SELECT name, upper(case when type = '' then 'TEXT' else type end) type FROM PRAGMA_TABLE_INFO(?)
        """,
    },
}
