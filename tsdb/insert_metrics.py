from datetime import datetime
import logging


def insert_metrics(metrics: list, source: str, conn: object) -> any:
    try:
        cursor = conn.cursor()

        if source == "#Thermal.v1_4_0.Fan":
            table = "rpmreading"
        elif source == "#Thermal.v1_4_0.Temperature":
            table = "temperaturereading"
        elif source == "#Power.v1_4_0.PowerControl":
            table = "wattsreading"
        elif source == "#Power.v1_3_0.Voltage":
            table = "voltagereading"

        for metric in metrics:
            processed_time = datetime.fromtimestamp(
                metric["time"] // 1e9).strftime('%Y-%m-%d %H:%M:%S.%f')
            nodeid = metric["nodeid"].split(".")[3]
            insert_rpmreading = f"""
                INSERT INTO {table} (timestamp, nodeid, source, fqdd, value)
                VALUES (
                  '{processed_time}', 
                  '{nodeid}', 
                  '{metric["source"]}', 
                  '{metric["fqdd"]}', 
                  '{metric["value"]}'
                );"""
            cursor.execute(insert_rpmreading)

        conn.commit()
        cursor.close()
    except Exception as err:
        logging.error(f"Insert rpmreading data error : {err}")
