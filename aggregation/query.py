import logging

from datetime import datetime, timedelta


def aggregate(conn: object, table: str, time_interval: int) -> any:
    try:

        print(f"Aggregating idrac8.{table}...")

        cursor = conn.cursor()
        end_date = datetime.today()
        start_date = end_date - timedelta(days=7)

        query = f"""
            SELECT public.time_bucket_gapfill('{time_interval} min', timestamp) AS time,
              nodeid, source, fqdd, AVG(value) AS value
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


def insert(conn: object, table: str, data: list) -> any:
    try:

        print(f"Inserting into idrac8.aggr_{table}...")

        cursor = conn.cursor()

        query = f"""
          INSERT INTO idrac8.aggr_{table}
          VALUES (%s, %s, %s, %s, %s)
        """

        cursor.executemany(query, data)
        conn.commit()
        cursor.close()

    except Exception as err:
        logging.error(f"Insert aggr_{table} data error : {err}")
