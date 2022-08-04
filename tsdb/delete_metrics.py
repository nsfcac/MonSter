import logging
from datetime import datetime


def delete_metrics(conn: object, table: str, start_date: datetime, end_date: datetime):
    """Delete metrics from table within given interval.

    :param conn object: connection object from psycopg2.
    :param table str: table name.
    :param start_date datetime: query start date.
    :param end_date datetime: query end date.
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
