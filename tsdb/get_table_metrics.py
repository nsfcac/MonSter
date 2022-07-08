import logging

from datetime import datetime


def get_table_metrics(conn: object, table: str, start_date: datetime, end_date: datetime) -> list:

    cursor = conn.cursor()

    try:
        query = f"""SELECT * FROM idrac8.{table}
                    WHERE timestamp >= '{start_date}'
	                AND timestamp < '{end_date}'
                    ORDER BY timestamp;"""

        cursor.execute(query)
        records = cursor.fetchall()
        conn.commit()

        return records

    except Exception as err:
        logging.error(f"Get {table} metrics error : {err}")

    finally:
        cursor.close()
