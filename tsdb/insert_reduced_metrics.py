import logging


def insert_reduced_metrics(conn: object, table: str, metrics: list) -> None:

    cursor = conn.cursor()

    try:
        query = f"""
          INSERT INTO idrac8.reduced_{table}
          VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        cursor.executemany(query, metrics)
        conn.commit()

    except Exception as err:
        logging.error(f"Insert reduced_{table} data error : {err}")

    finally:
        cursor.close()
