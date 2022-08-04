import logging

from datetime import datetime


def aggregate_metrics_v2(conn: object, table: str, 
                         start_date: datetime, end_date: datetime, 
                         aggregation_time: int = 10) -> list:
    """Aggregates metrics from given table.

    :param conn object: connection object from psycopg2.
    :param table str: table name.
    :param start_date datetime: query start date.
    :param end_date datetime: query end date.
    :param aggregation_time int: determines bucket intervals to aggregate, defaults to 10.
    :returns list: aggregated metrics
    """
    query = f"""
        SELECT public.time_bucket('{aggregation_time} min', timestamp) AS time, nodeid, source, fqdd, AVG(value) AS value
        FROM idrac8.{table}
        WHERE timestamp >= '{start_date}'
        AND timestamp < '{end_date}'
        GROUP BY time, nodeid, source, fqdd
        ORDER BY time;
    """
    records = []
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        records = cursor.fetchall()
        conn.commit()
    except Exception as err:
        logging.error(f"aggregate_metrics_v2 error : {err}")
    finally:
        cursor.close()

    return records
