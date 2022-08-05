import logging
from datetime import datetime

logger = logging.getLogger("delete_records")


def delete_records(conn: object, table: str, start_date: datetime, end_date: datetime):
    """Deletes records from table within given interval.

    :param object conn: connection object from psycopg2.
    :param str table: table name.
    :param datetime start_date: query start date.
    :param datetime end_date: query end date.
    """
    query = f"""
        DELETE FROM {table}
        WHERE timestamp >= '{start_date}'
        AND timestamp < '{end_date}';
    """
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
    except Exception as err:
        logger.error("%s", err)
    finally:
        cursor.close()
