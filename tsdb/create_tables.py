from utils.check_source import check_source

import logging


def create_tables(source: str, conn: object) -> any:
    try:
        cursor = conn.cursor()

        table = check_source(source)

        # Creates the table for the FanSensor metrics
        create_table = f"""CREATE TABLE IF NOT EXISTS {table} (
                timestamp TIMESTAMPTZ NOT NULL,
                nodeid INT4 NOT NULL,
                source TEXT,
                fqdd TEXT,
                value FLOAT4,
                FOREIGN KEY (nodeid) REFERENCES public.nodes(nodeid));"""
        cursor.execute(create_table)

        conn.commit()
        cursor.close()

    except Exception as err:
        logging.error(f"Create tables error : {err}")
