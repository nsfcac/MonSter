import logging


def create_reduced_table(conn: object, table: str) -> None:

    cursor = conn.cursor()

    try:
        create_table_query = f"""CREATE TABLE IF NOT EXISTS reduced_{table} (
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

    except Exception as err:
        logging.error(f"Create tables error : {err}")

    finally:
        cursor.close()
