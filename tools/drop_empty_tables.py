import psycopg2
from psycopg2 import sql

from monster import utils

# === Determine if a table is empty ===
def is_table_empty(conn, schema, table):
    with conn.cursor() as cur:
        query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table)
        )
        cur.execute(query)
        row_count = cur.fetchone()[0]
        return row_count == 0

# === Helper: Check if `value` is numeric and all values are zero ===
def is_value_column_numeric_and_all_zero(conn, schema, table):
    with conn.cursor() as cur:
        # Get column type
        cur.execute("""
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = 'value'
        """, (schema, table))
        result = cur.fetchone()
        if not result:
            print(f"Skipping {schema}.{table} (no 'value' column)")
            return False

        data_type = result[0]
        numeric_types = ('integer', 'bigint', 'smallint', 'real', 'double precision', 'numeric')

        if data_type not in numeric_types:
            print(f"Skipping {schema}.{table} (unsupported 'value' type: {data_type})")
            return False

        # Check if all values are zero (value != 0)
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE value <> 0").format(
                sql.Identifier(schema),
                sql.Identifier(table)
            )
        )
        non_zero_count = cur.fetchone()[0]
        return non_zero_count == 0

# === Drop tables based on both conditions ===
def drop_zero_or_empty_tables(conn, schema):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
        """, (schema,))
        tables = cur.fetchall()

        for (table,) in tables:
            try:
                if is_table_empty(conn, schema, table):
                    cur.execute(
                        sql.SQL("DROP TABLE {}.{}").format(
                            sql.Identifier(schema),
                            sql.Identifier(table)
                        )
                    )
                    print(f"Dropped table: {schema}.{table} (empty)")
                elif is_value_column_numeric_and_all_zero(conn, schema, table):
                    cur.execute(
                        sql.SQL("DROP TABLE {}.{}").format(
                            sql.Identifier(schema),
                            sql.Identifier(table)
                        )
                    )
                    print(f"Dropped table: {schema}.{table} (`value` column all 0)")
                else:
                    print(f"Kept table: {schema}.{table} (contains non-zero `value`)")
            except Exception as e:
                print(f"Error processing {schema}.{table}: {e}")

        conn.commit()


if __name__ == '__main__':
    config      = utils.parse_config()
    connection  = utils.init_tsdb_connection(config)
    schemas     = ['idrac']
    try:
        with psycopg2.connect(connection) as conn:
          for schema_name in schemas:
            print(f"Processing schema: {schema_name}")
            drop_zero_or_empty_tables(conn, schema_name)
    except Exception as e:
        print(f"Error: {e}")