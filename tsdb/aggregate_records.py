import logging
from datetime import datetime

logger = logging.getLogger("aggregate_records")


def aggregate_records(conn: object, table: str,
                      start_date: datetime, end_date: datetime,
                      aggregation_time: int = 10) -> list:
    """Aggregates records from given table.

    :param object conn: connection object from psycopg2
    :param str table: table name
    :param datetime start_date: query start date
    :param datetime end_date: query end date
    :param int aggregation_time: determines bucket intervals to aggregate
    :returns list: aggregated records
    """

    query = f"""
        SELECT public.time_bucket('{aggregation_time} min', timestamp) AS time,
            nodeid, source, fqdd, AVG(value) AS value
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
        logger.error("%s", err)
    finally:
        cursor.close()

    return records
