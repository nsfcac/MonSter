import logging

from datetime import datetime, timedelta


def aggregate_metrics(conn: object, table: str, timedelta_days: int, aggregation_time: int) -> list:

    cursor = conn.cursor()

    try:
        end_date = datetime.today()
        start_date = end_date - timedelta(days=timedelta_days)

        query = f"""
            SELECT public.time_bucket_gapfill('{aggregation_time} min', timestamp) AS time, nodeid, source, fqdd, AVG(value) AS avg, MIN(value) AS min, MAX(value) AS max
            FROM idrac8.{table}
            WHERE timestamp >= '{start_date}'
	            AND timestamp < '{end_date}'
            GROUP BY time, nodeid, source, fqdd
            ORDER BY time;
        """

        cursor.execute(query)
        records = cursor.fetchall()
        conn.commit()

        return records

    except Exception as err:
        logging.error(f"Aggregate {table} data error : {err}")

    finally:
        cursor.close()
