import logging


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
