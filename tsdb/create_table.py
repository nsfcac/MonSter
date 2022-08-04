import logging


def create_table(conn: object, table: str):
    """Creates table.
    
    :param object conn: connection object from psycopg2.
    :param str table: table name.
    """
    query = f"""
        CREATE TABLE IF NOT EXISTS {table} (
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
        logging.error(f"create_table error : {err}")
    finally:
        cursor.close()
