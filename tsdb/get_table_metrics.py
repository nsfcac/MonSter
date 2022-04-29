import logging

from datetime import datetime, timedelta


def get_table_metrics(conn: object, table: str, timedelta_days: int) -> list:

    cursor = conn.cursor()

    try:
        end_date = datetime.today()
        start_date = end_date - timedelta(days=timedelta_days)

        query = f"""SELECT * FROM idrac8.{table}
                    WHERE timestamp >= '{start_date}'
	                AND timestamp < '{end_date}';"""

        cursor.execute(query)
        records = cursor.fetchall()
        conn.commit()

        return records

    except Exception as err:
        logging.error(f"Get {table} metrics error : {err}")

    finally:
        cursor.close()
