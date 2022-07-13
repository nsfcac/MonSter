import logging
from datetime import datetime, timedelta
from dotenv import dotenv_values

import psycopg2
import pytz
from analysis.mape import compute_mapes

from tsdb.get_table_metrics import get_table_metrics
from utils.deduplicate import deduplicate
from utils.reconstruction import reconstruction

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

TIMEDELTA_DAYS = 1


def volume_comparison():

    with psycopg2.connect(CONNECTION_STRING) as conn:
        try:
            table_mapes = {}
            end_date = datetime.now(pytz.timezone(
                'US/Central')).replace(second=0, microsecond=0)
            start_date = end_date - timedelta(days=TIMEDELTA_DAYS)

            for table in TABLES:
                print(table)
                original = get_table_metrics(conn, table, start_date, end_date)
                print(f"original length: {len(original)}")

                deduplicated = deduplicate(original)
                print(f"deduplicated length: {len(deduplicated)}")
                print(
                    f"deduplicated / original * 100: {len(deduplicated) / len(original) * 100}%")

                reconstructed = reconstruction(
                    deduplicated, end_date, time_gap=1)

                print(f"reconstructed length: {len(reconstructed)}")
                print(
                    f"reconstructed / original * 100: {len(reconstructed) / len(original) * 100}%")

                recon_orig_mape = compute_mapes(original, reconstructed)
                table_mapes[table] = recon_orig_mape

            print(table_mapes)

        except Exception as err:
            logging.error(
                f"Volume comparison error : {err}")


if __name__ == '__main__':
    volume_comparison()
