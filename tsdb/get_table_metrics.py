import logging

from datetime import datetime


def get_table_metrics(conn: object, table: str, start_date: datetime, end_date: datetime) -> list:
    """Gets metrics from given table.

    :param object conn: connection object from psycopg2.
    :param str table: table name.
    :param datetime start_date: query start date.
    :param datetime end_date: query end date.
    :returns list: original metrics.
    """
    query = f"""
        SELECT * FROM idrac8.{table}
        WHERE timestamp >= '{start_date}'
        AND timestamp < '{end_date}'
        ORDER BY timestamp;
    """
    records = []
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        records = cursor.fetchall()
        conn.commit()
    except Exception as err:
        logging.error(f"get_table_metrics error : {err}")
    finally:
        cursor.close()

    return records
