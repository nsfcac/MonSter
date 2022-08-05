import logging

logger = logging.getLogger("query_table")


def query_table(conn: object, query: str) -> list:
    """Gets records using query input.

    :param object conn: connection object from psycopg2.
    :param str query: query to be executed.
    :returns list: table records.
    """
    records = []
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        records = cursor.fetchall()
        conn.commit()
    except Exception as err:
        logger.error("%s", err)
    finally:
        cursor.close()

    return records
