from datetime import datetime
import logging


def parse_thermal_metrics(thermals: list, conn: object):
    try:
        cursor = conn.cursor()
        entries = []
        for data in thermals:
            node_id = data['node']
            measurement = data['measurement']
            label = data['label']
            value = data['value']
            time = datetime.fromtimestamp(
                data['time'] // 1000000000).strftime('%Y-%m-%d %H:%M:%S')
            insert_record_query = f"""INSERT INTO thermal_metrics (node_id, label, measurement, value, time)
                                VALUES ('{node_id}', '{label}', '{measurement}', {value}, '{time}')"""
            cursor.execute(insert_record_query)
            conn.commit()

    except Exception as err:
        logging.error(f"Parse thermal metrics error : {err}")
