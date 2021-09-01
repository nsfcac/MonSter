from utils.check_source import check_source

from datetime import datetime
import logging


def insert_metrics(metrics: list, source: str, conn: object) -> any:
    try:
        cursor = conn.cursor()

        table = check_source(source)

        for metric in metrics:
            processed_time = datetime.fromtimestamp(
                metric["time"] // 1e9).strftime('%Y-%m-%d %H:%M:%S.%f')
            nodeid = metric["nodeid"]
            insert_rpmreading = f"""
                INSERT INTO {table} (timestamp, nodeid, source, fqdd, value)
                VALUES (
                  '{processed_time}', 
                  (SELECT nodeid FROM public.nodes WHERE bmc_ip_addr='{nodeid}'), 
                  '{metric["source"]}', 
                  '{metric["fqdd"]}', 
                  '{metric["value"]}'
                );"""
            cursor.execute(insert_rpmreading)

        conn.commit()
        cursor.close()
    except Exception as err:
        logging.error(f"Insert rpmreading data error : {err}")
