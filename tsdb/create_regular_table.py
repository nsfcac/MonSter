import logging

from utils.check_source import check_source


def create_regular_table(conn: object, source: str):
    """Creates regular table based on source.
    
    :param conn object: connection object from psycopg2.
    :param source str: metric source
    """
    cursor = conn.cursor()
    try:
        table = check_source(source)
        if not table:
            return
        create_table_query = f"""CREATE TABLE IF NOT EXISTS {table} (
                timestamp TIMESTAMPTZ NOT NULL,
                nodeid INT4 NOT NULL,
                source TEXT,
                fqdd TEXT,
                value FLOAT4,
                FOREIGN KEY (nodeid) REFERENCES public.nodes(nodeid));"""
        cursor.execute(create_table_query)
        conn.commit()
    except Exception as err:
        logging.error(f"create_regular_table error : {err}")
    finally:
        cursor.close()
