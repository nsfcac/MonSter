import logging
import time

from dotenv import dotenv_values
import psycopg2

tsdb_config = dotenv_values(".env")
CONNECTION = f"dbname={tsdb_config['DBNAME']} user={tsdb_config['USER']} password={tsdb_config['PASSWORD']} options='-c search_path=idrac9'"

logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)


def aggregation_query(conn: object, table: str, start: int, nodeid: int, time_interval: int) -> any:
    try:
        if nodeid != 0:
            query = f"""
              SELECT public.time_bucket_gapfill('{time_interval} min', timestamp) AS time,
                nodeid, fqdd AS label, max(value) AS value
              FROM {table} 
              WHERE timestamp >= '2021-10-{start}T13:00:00' 
                AND timestamp < '2021-10-30T13:00:00'
                AND nodeid = {nodeid}
              GROUP BY time, nodeid, label 
              ORDER BY time;
            """
        else:
            query = f"""SELECT public.time_bucket_gapfill('5 min', timestamp) AS time,
                        nodeid, fqdd AS label, max(value) AS value
                        FROM {table} 
                        WHERE timestamp >= '2021-10-{start}T13:00:00' 
                          AND timestamp < '2021-10-30T13:00:00'
                        GROUP BY time, nodeid, label 
                        ORDER BY time;
            """
        cursor = conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            print(table, row)
        cursor.close()

    except Exception as err:
        logging.error(f"Insert rpmreading data error : {err}")


def main():

    try:
        y1 = []
        y2 = []
        y3 = []
        x = [1, 2, 3, 4]
        conn = psycopg2.connect(CONNECTION)

        for start in range(29, 25, -1):
            start_time = time.time()
            aggregation_query(conn, "rpmreading", start, 489, 5)

            time_diff = float(time.time() - start_time)
            y1.append(time_diff)

        for start in range(29, 25, -1):
            start_time = time.time()
            aggregation_query(conn, "rpmreading", start, 489, 10)

            time_diff = float(time.time() - start_time)
            y2.append(time_diff)

        for start in range(29, 25, -1):
            start_time = time.time()
            aggregation_query(conn, "rpmreading", start, 489, 15)

            time_diff = float(time.time() - start_time)
            y3.append(time_diff)

        print(x, y1, y2, y3)

    except Exception as err:
        logging.error(f"main error : {err}")


if __name__ == "__main__":
    main()
