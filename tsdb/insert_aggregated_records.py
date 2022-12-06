import logging

logger = logging.getLogger("insert_aggregated_records")


def insert_aggregated_records(conn: object, table: str, records: list):
    """Inserts aggregated records into given table.

    :param object conn: connection object from psycopg2
    :param str table: table name
    :param list records: aggregated records
    """

    cursor = conn.cursor()
    try:
        for record in records:
            value = -1 if record[4] is None else record[4]
            query = f"""
                INSERT INTO {table} (timestamp, nodeid, source, fqdd, value)
                VALUES (
                  '{record[0]}',
                  '{record[1]}',
                  '{record[2]}',
                  '{record[3]}',
                  '{value}'
                );
            """
            cursor.execute(query)
        conn.commit()
    except Exception as err:
        logger.error("%s", err)
    finally:
        cursor.close()
