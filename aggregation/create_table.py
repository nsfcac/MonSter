import logging


def create_table(conn: object, table: str) -> any:
    try:
        cursor = conn.cursor()

        create_table_query = f"""CREATE TABLE IF NOT EXISTS aggr_{table} (
                time TIMESTAMPTZ NOT NULL,
                nodeid INT4 NOT NULL,
                source TEXT,
                fqdd TEXT,
                value FLOAT8,
                FOREIGN KEY (nodeid) REFERENCES public.nodes(nodeid));"""

        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()

    except Exception as err:
        logging.error(f"Create tables error : {err}")
