import logging

from datetime import datetime


def insert_metrics(conn: object, metrics: list, table: str) -> None:
    """Inserts metrics list into table.
    
    :param conn object: connection object from psycopg2.
    :param table str: table name.
    """
    cursor = conn.cursor()

    try:
        for metric in metrics:
            processed_time = datetime.fromtimestamp(metric["time"] // 1e9).strftime('%Y-%m-%d %H:%M:%S.%f')
            nodeid = metric["nodeid"]
            query = f"""
                INSERT INTO {table} (timestamp, nodeid, source, fqdd, value)
                VALUES (
                  '{processed_time}', 
                  (SELECT nodeid FROM public.nodes WHERE bmc_ip_addr='{nodeid}'), 
                  '{metric["source"]}', 
                  '{metric["fqdd"]}', 
                  '{metric["value"]}'
                );"""

            cursor.execute(query)
        conn.commit()
    except Exception as err:
        logging.error(f"insert_metrics error : {err}")
    finally:
        cursor.close()
