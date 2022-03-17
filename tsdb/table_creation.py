import logging

from utils.check_source import check_source


def create_regular_table(conn: object, source: str) -> None:

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


def create_aggregated_table(conn: object, table: str) -> None:

    try:
        print(f"Creating table idrac8.aggr_{table} if not exists...")

        cursor = conn.cursor()

        create_table_query = f"""CREATE TABLE IF NOT EXISTS aggr_{table} (
                time TIMESTAMPTZ NOT NULL,
                nodeid INT4 NOT NULL,
                source TEXT,
                fqdd TEXT,
                avg FLOAT4,
                min FLOAT4,
                max FLOAT4,
                FOREIGN KEY (nodeid) REFERENCES public.nodes(nodeid));"""

        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()

    except Exception as err:
        logging.error(f"Create tables error : {err}")
