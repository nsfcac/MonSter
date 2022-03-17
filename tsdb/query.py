import logging

from datetime import datetime, timedelta

from utils.check_source import check_source


AGGREGATION_TIME_PERIOD = 7


def insert_metrics(conn: object, metrics: list, source: str) -> None:

    try:
        cursor = conn.cursor()

        table = check_source(source)

        for metric in metrics:
            processed_time = datetime.fromtimestamp(
                metric["time"] // 1e9).strftime('%Y-%m-%d %H:%M:%S.%f')
            nodeid = metric["nodeid"]
            insert_metric_query = f"""
                INSERT INTO {table} (timestamp, nodeid, source, fqdd, value)
                VALUES (
                  '{processed_time}', 
                  (SELECT nodeid FROM public.nodes WHERE bmc_ip_addr='{nodeid}'), 
                  '{metric["source"]}', 
                  '{metric["fqdd"]}', 
                  '{metric["value"]}'
                );"""
            cursor.execute(insert_metric_query)

        conn.commit()
        cursor.close()

    except Exception as err:
        logging.error(f"Insert rpmreading data error : {err}")


def insert_aggregated_metrics(conn: object, table: str, metrics: list) -> None:

    try:
        print(f"Inserting metrics into idrac8.aggr_{table}...")

        cursor = conn.cursor()

        query = f"""
          INSERT INTO idrac8.aggr_{table}
          VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        cursor.executemany(query, metrics)
        conn.commit()
        cursor.close()

    except Exception as err:
        logging.error(f"Insert aggr_{table} data error : {err}")


def aggregate(conn: object, table: str, time_interval: int) -> list:

    try:
        print(f"Aggregating idrac8.{table} metrics...")

        cursor = conn.cursor()
        end_date = datetime.today()
        start_date = end_date - timedelta(days=AGGREGATION_TIME_PERIOD)

        query = f"""
            SELECT public.time_bucket_gapfill('{time_interval} min', timestamp) AS time,
              nodeid, source, fqdd, AVG(value) AS avg, MIN(value) AS min, MAX(value) AS max
            FROM idrac8.{table}
            WHERE timestamp >= '{start_date}'
	            AND timestamp < '{end_date}'
            GROUP BY time, nodeid, source, fqdd
            ORDER BY time;
        """
        cursor.execute(query)

        records = cursor.fetchall()

        conn.commit()
        cursor.close()

        return records

    except Exception as err:
        logging.error(f"Aggregate {table} data error : {err}")
