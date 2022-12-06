import logging

logger = logging.getLogger("insert_deduplicated_records")


def insert_deduplicated_records(conn: object, table: str, records: list):
    """Inserts deduplicated records into table.

    :param object conn: connection object from psycopg2
    :param str table: table name
    :param list records: deduplicated records
    """

    query = f"""
        INSERT INTO idrac8.{table}
        VALUES (%s, %s, %s, %s, %s);
    """
    cursor = conn.cursor()
    try:
        cursor.executemany(query, records)
        conn.commit()
    except Exception as err:
        logger.error("%s", err)
    finally:
        cursor.close()
