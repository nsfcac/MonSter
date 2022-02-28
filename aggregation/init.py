import time
import logging
import psycopg2

from dotenv import dotenv_values
from query import create_table, aggregate, insert

tsdb_config = dotenv_values(".env")
CONNECTION = f"dbname={tsdb_config['DBNAME']} user={tsdb_config['USER']} password={tsdb_config['PASSWORD']} options='-c search_path=idrac8'"

logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)


def main():

    start_time = time.time()

    tables = [
        "rpmreading",
        "systempowerconsumption",
        "temperaturereading",
    ]

    conn = psycopg2.connect(CONNECTION)

    for table in tables:
        create_table(conn, table)
        data = aggregate(conn, table, 10)
        insert(conn, table, data)

    print("\n--- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    main()
