import logging

from utils.check_source import check_source


def create_table(source: str, conn: object) -> any:
    try:
        cursor = conn.cursor()

        table = check_source(source)

        create_table_query = f"""CREATE TABLE IF NOT EXISTS {table} (
                timestamp TIMESTAMPTZ NOT NULL,
                nodeid INT4 NOT NULL,
                source TEXT,
                fqdd TEXT,
                value FLOAT4,
                FOREIGN KEY (nodeid) REFERENCES public.nodes(nodeid));"""
        cursor.execute(create_table_query)

        conn.commit()
        cursor.close()

    except Exception as err:
        logging.error(f"Create tables error : {err}")
