import logging
from datetime import datetime, timedelta

import psycopg2
import pytz
from dotenv import dotenv_values

from tsdb.create_table import create_table
from tsdb.get_records import get_records
from tsdb.insert_deduplicated_records import insert_deduplicated_records
from utils.deduplication import deduplicate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z"
)

logger = logging.getLogger("reduce_deduplicate")

TSDB_CONFIG = dotenv_values(".env")

CONNECTION_STRING = f"""
  dbname={TSDB_CONFIG['DBNAME']}
  user={TSDB_CONFIG['USER']}
  password={TSDB_CONFIG['PASSWORD']}
  options='-c search_path=idrac8'
"""

TABLES = [
    "rpmreading",
    "systempowerconsumption",
    "temperaturereading",
]

TIMEDELTA_DAYS = 14


def main():
    """Deduplicates records based on TIMEDELTA_DAYS
        and stores them in deduplicated tables.
    """

    end_date = pytz.utc.localize(datetime.strptime("07/30/2022-00:00",
                                                   "%m/%d/%Y-%H:%M"))
    start_date = end_date - timedelta(days=TIMEDELTA_DAYS)

    with psycopg2.connect(CONNECTION_STRING) as conn:
        for table in TABLES:
            try:
                deduplicated_table = "deduplicated_" + table
                logger.info("Creating %s table if not exists",
                            deduplicated_table)
                create_table(conn, deduplicated_table)

                logger.info("Getting records from %s", table)
                original_records = get_records(conn, table, start_date,
                                               end_date)
                logger.info("Retrieved %s records from %s",
                            len(original_records), table)

                logger.info("Deduplicating records...")

                deduplicated_records = deduplicate(original_records,
                                                   formula="cv")

                deduplicated_length_percentage = (len(deduplicated_records) /
                                                  len(original_records) * 100)

                logger.info("Deduplicated down to %s%% of original records",
                            deduplicated_length_percentage)

                insert_deduplicated_records(conn, deduplicated_table,
                                            deduplicated_records)

                logger.info("Inserted %s records into %s",
                            len(deduplicated_records), deduplicated_table)
            except Exception as err:
                logger.error("%s", err)


if __name__ == "__main__":
    main()
