import logging

from datetime import datetime, timedelta


def aggregate_insert_query(conn: object, table: str, time_interval: int) -> any:
    try:
        cursor = conn.cursor()
        end_date = datetime.today()
        start_date = end_date - timedelta(days=3)

        print(start_date)
        print(end_date)

        query = f"""INSERT INTO idrac8.aggr_{table}
            SELECT public.time_bucket_gapfill('{time_interval} min', timestamp) AS time,
              nodeid, source, fqdd, AVG(value) AS value
            FROM idrac8.{table}
            WHERE timestamp >= '{start_date}'
	            AND timestamp < '{end_date}'
            GROUP BY time, nodeid, source, fqdd
            ORDER BY time;
        """
        cursor.execute(query)
        conn.commit()
        cursor.close()

    except Exception as err:
        logging.error(f"Insert rpmreading data error : {err}")
