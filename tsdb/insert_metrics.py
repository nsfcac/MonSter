import logging
from datetime import datetime

logger = logging.getLogger("insert_metrics")


def insert_metrics(conn: object, metrics: list, table: str):
    """Inserts metrics into table.

    :param object conn: connection object from psycopg2
    :param list metrics: idrac8 metrics
    :param str table: table name
    """

    cursor = conn.cursor()
    try:
        for metric in metrics:
            metric_datetime = datetime.fromtimestamp(metric["time"] // 1e9)
            processed_time = metric_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
            nodeid = metric["nodeid"]
            query = f"""
                INSERT INTO {table} (timestamp, nodeid, source, fqdd, value)
                VALUES (
                  '{processed_time}',
                  (SELECT nodeid
                    FROM public.nodes
                    WHERE bmc_ip_addr='{nodeid}'),
                  '{metric["source"]}',
                  '{metric["fqdd"]}',
                  '{metric["value"]}'
                );
            """
            cursor.execute(query)
        conn.commit()
    except Exception as err:
        logger.error("%s", err)
    finally:
        cursor.close()
