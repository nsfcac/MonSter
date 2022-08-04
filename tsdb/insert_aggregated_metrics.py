import logging


def insert_aggregated_metrics(conn: object, table: str, metrics: list):
    """Inserts aggregated metrics into given table.

    :param object conn: connection object from psycopg2.
    :param str table: table name.
    :param list metrics: aggregated metrics.
    """
    cursor = conn.cursor()
    try:
        for metric in metrics:
            value = -1 if metric[4] is None else metric[4]
            query = f"""
                INSERT INTO {table} (timestamp, nodeid, source, fqdd, value)
                VALUES (
                  '{metric[0]}', 
                  '{metric[1]}', 
                  '{metric[2]}', 
                  '{metric[3]}', 
                  '{value}'
                );"""
            cursor.execute(query)
        conn.commit()
    except Exception as err:
        logging.error(f"insert_aggregated_metrics error : {err}")
    finally:
        cursor.close()
