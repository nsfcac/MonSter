import logging
from datetime import datetime, timedelta

import psycopg2
import pytz
from dotenv import dotenv_values

from tsdb.create_reduced_table_v2 import create_reduced_table_v2
from tsdb.get_table_metrics import get_table_metrics
from tsdb.insert_deduplicated_metrics import insert_deduplicated_metrics
from utils.deduplicate import deduplicate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z"
)

logger = logging.getLogger("reduce_deduplicate")

TSDB_CONFIG = dotenv_values(".env")

CONNECTION_STRING = f"dbname={TSDB_CONFIG['DBNAME']} user={TSDB_CONFIG['USER']} password={TSDB_CONFIG['PASSWORD']} options='-c search_path=idrac8'"

TABLES = [
    "rpmreading",
    "systempowerconsumption",
    "temperaturereading",
]

TIMEDELTA_DAYS = 7


def main():
    """Deduplicates metrics based on TIMEDELTA_DAYS and stores them in reduced tables.
    """
    
    end_date = datetime.now(pytz.utc).replace(second=0, microsecond=0)
    end_date -= timedelta(days=TIMEDELTA_DAYS)
    start_date = end_date - timedelta(days=TIMEDELTA_DAYS)
    
    with psycopg2.connect(CONNECTION_STRING) as conn:
        for table in TABLES:
            try:
                logger.info("Creating reduced %s table if not exists", table)
                create_reduced_table_v2(conn, table)
                
                logger.info("Getting records from %s", table)
                records = get_table_metrics(conn, table, start_date, end_date)
                logger.info("Retrieved %s metrics from %s", len(records), table)
                
                logger.info("Deduplicating records...")
                deduplicated_records = deduplicate(records)
                logger.info("Deduplicated down to %s records", len(deduplicated_records))
                
                insert_deduplicated_metrics(conn, table, deduplicated_records)
                logger.info("Inserted %s records into reduced %s", len(deduplicated_records), table)
            except Exception as err:
                logging.error(f"reduce_deduplicate error : {err}")


if __name__ == "__main__":
    main()
