import logging


def create_regular_table(conn: object, table: str):
    """Creates regular table.
    
    :param conn object: connection object from psycopg2.
    :param table str: table name.
    """
    cursor = conn.cursor()
    try:
        query = f"""CREATE TABLE IF NOT EXISTS {table} (
                timestamp TIMESTAMPTZ NOT NULL,
                nodeid INT4 NOT NULL,
                source TEXT,
                fqdd TEXT,
                value FLOAT4,
                FOREIGN KEY (nodeid) REFERENCES public.nodes(nodeid));"""
        cursor.execute(query)
        conn.commit()
    except Exception as err:
        logging.error(f"create_regular_table error : {err}")
    finally:
        cursor.close()
