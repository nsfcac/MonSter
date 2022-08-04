import logging
from datetime import datetime


def delete_metrics(conn: object, table: str, start_date: datetime, end_date: datetime):
    """Delete metrics from table within given interval.

    :param object conn: connection object from psycopg2.
    :param str table: table name.
    :param datetime start_date: query start date.
    :param datetime end_date: query end date.
    """
    query = f"""
        DELETE FROM {table}
        WHERE timestamp >= '{start_date}'
        AND timestamp < '{end_date}'
    """
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
    except Exception as err:
        logging.error(f"delete_metrics error : {err}")
    finally:
        cursor.close()
