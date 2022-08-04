import logging


def insert_deduplicated_metrics(conn: object, table: str, metrics: list):
    """Inserts deduplicated metrics into table.

    :param object conn: connection object from psycopg2.
    :param str table: table name.
    :param list metrics: deduplicated metrics.
    """
    query = f"""
        INSERT INTO idrac8.reduced_{table}_v2
        VALUES (%s, %s, %s, %s, %s);
    """
    cursor = conn.cursor()
    try:
        cursor.executemany(query, metrics)
        conn.commit()
    except Exception as err:
        logging.error(f"insert_deduplicated_metrics error : {err}")
    finally:
        cursor.close()
