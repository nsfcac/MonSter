from utils.check_source import check_source

import logging


def clear_metrics(source: str, conn: object) -> any:
    try:
        cursor = conn.cursor()

        table = check_source(source)

        # Creates the table for the FanSensor metrics
        create_rpmreading_table = f"""DELETE FROM {table};"""
        cursor.execute(create_rpmreading_table)

        conn.commit()
        cursor.close()

    except Exception as err:
        logging.error(f"Create tables error : {err}")
