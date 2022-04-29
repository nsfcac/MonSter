import logging
from dotenv import dotenv_values

import psycopg2

from tsdb.aggregate_metrics import aggregate_metrics
from tsdb.get_table_metrics import get_table_metrics
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


def volume_comparison():

    with psycopg2.connect(CONNECTION_STRING) as conn:
        try:
            for table in TABLES:
                original = get_table_metrics(conn, table, TIMEDELTA_DAYS)
                aggregated = aggregate_metrics(
                    conn, table, TIMEDELTA_DAYS, AGGREGATION_TIME)
                deduplicated = deduplicate(aggregated)

                print(
                    f"{table} original length {TIMEDELTA_DAYS} days: {len(original)}")
                print(
                    f"{table} aggregated length {TIMEDELTA_DAYS} days: {len(aggregated)}")
                print(
                    f"{table} aggregated + deduplicated length {TIMEDELTA_DAYS} days: {len(deduplicated)}\n")

        except Exception as err:
            logging.error(
                f"Volume comparison error : {err}")


if __name__ == '__main__':
    volume_comparison()
