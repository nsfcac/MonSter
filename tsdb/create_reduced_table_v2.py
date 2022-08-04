import logging


def create_reduced_table_v2(conn: object, table: str):
    """Creates reduced table.

    :param conn object: connection object from psycopg2.
    :param table str: table name.
    """
    query = f"""
        CREATE TABLE IF NOT EXISTS reduced_{table}_v2 (
            timestamp TIMESTAMPTZ NOT NULL,
            nodeid INT4 NOT NULL,
            source TEXT,
            fqdd TEXT,
            value FLOAT4,
            FOREIGN KEY (nodeid) REFERENCES public.nodes(nodeid));
    """
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
    except Exception as err:
        logging.error(f"create_reduced_table_v2 error : {err}")
    finally:
        cursor.close()
