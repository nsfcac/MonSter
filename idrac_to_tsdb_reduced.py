import time
import logging
import psycopg2

from dotenv import dotenv_values
from tsdb.table_creation import create_aggregated_table
from tsdb.query import aggregate, insert_aggregated_metrics
from utils.deduplicate import deduplicate
from analysis.create_chart import create_chart

tsdb_config = dotenv_values(".env")
CONNECTION = f"dbname={tsdb_config['DBNAME']} user={tsdb_config['USER']} password={tsdb_config['PASSWORD']} options='-c search_path=idrac8'"

logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)

AGGREGATION_TIME_INTERVAL = 10


def main():

    try:
        start_time = time.time()

        tables = [
            "rpmreading",
            "systempowerconsumption",
            "temperaturereading",
        ]

        conn = psycopg2.connect(CONNECTION)

        for table in tables:
            create_aggregated_table(conn, table)
            records = aggregate(conn, table, AGGREGATION_TIME_INTERVAL)
            deduplicated_records = deduplicate(records)

            # create_chart(records, deduplicated_records)
            # insert_aggregated_metrics(conn, table, deduplicated_records)
            # insert_aggregated_metrics(conn, table, records)

        print("\n--- %s seconds ---" % (time.time() - start_time))

    except Exception as err:
        print(err)


if __name__ == "__main__":
    main()
