import logging
import re
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

CONNECTION_STRING = f"dbname={TSDB_CONFIG['DBNAME']} user={TSDB_CONFIG['USER']} password={TSDB_CONFIG['PASSWORD']} options='-c search_path=idrac8'"

TABLES = [
    "deduplicated_rpmreading",
    "deduplicated_systempowerconsumption",
    "deduplicated_temperaturereading",
]

TIMEDELTA_DAYS = 7


def main():
    """Deduplicates records based on TIMEDELTA_DAYS and stores them in deduplicated tables.
    """
    
    end_date = datetime.now(pytz.utc).replace(second=0, microsecond=0)
    end_date -= timedelta(days=TIMEDELTA_DAYS)
    start_date = end_date - timedelta(days=TIMEDELTA_DAYS)
    
    with psycopg2.connect(CONNECTION_STRING) as conn:
        for table in TABLES:
            try:
                logger.info("Creating %s table if not exists", table)
                create_table(conn, table)
                
                original_table = table.split("_")[1]                
                logger.info("Getting records from %s", original_table)
                records = get_records(conn, original_table, start_date, end_date)
                logger.info("Retrieved %s records from %s", len(records), original_table)
                
                logger.info("Deduplicating records...")
                deduplicated_records = deduplicate(records)
                logger.info("Deduplicated down to %s records", len(deduplicated_records))
                
                insert_deduplicated_records(conn, table, deduplicated_records)
                logger.info("Inserted %s records into %s", len(deduplicated_records), table)
            except Exception as err:
                logger.error("%s", err)


if __name__ == "__main__":
    main()
