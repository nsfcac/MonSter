import logging

import psycopg2
from dotenv import dotenv_values

from tsdb.aggregate_metrics import aggregate_metrics
from tsdb.create_reduced_table import create_reduced_table
from tsdb.insert_reduced_metrics import insert_reduced_metrics
from utils.deduplicate import deduplicate

logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)

TSDB_CONFIG = dotenv_values(".env")

CONNECTION_STRING = f"dbname={TSDB_CONFIG['DBNAME']} user={TSDB_CONFIG['USER']} password={TSDB_CONFIG['PASSWORD']} options='-c search_path=idrac8'"

TABLES = [
    "rpmreading",
    "systempowerconsumption",
    "temperaturereading",
]

AGGREGATION_TIME = 10
TIMEDELTA_DAYS = 7


def main():

    with psycopg2.connect(CONNECTION_STRING) as conn:
        try:
            for table in TABLES:
                create_reduced_table(conn, table)
                records = aggregate_metrics(
                    conn, table, TIMEDELTA_DAYS, AGGREGATION_TIME)
                deduplicated_records = deduplicate(records)
                insert_reduced_metrics(conn, table, deduplicated_records)

        except Exception as err:
            logging.error(f"Main error : {err}")


if __name__ == "__main__":
    main()
