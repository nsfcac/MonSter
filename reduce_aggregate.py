import logging
from datetime import datetime, timedelta

import psycopg2
import pytz
from dotenv import dotenv_values

from tsdb.aggregate_records import aggregate_records
from tsdb.create_table import create_table
from tsdb.insert_aggregated_records import insert_aggregated_records

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z"
)

logger = logging.getLogger("reduce_aggregate")

TSDB_CONFIG = dotenv_values(".env")

CONNECTION_STRING = f"dbname={TSDB_CONFIG['DBNAME']} user={TSDB_CONFIG['USER']} password={TSDB_CONFIG['PASSWORD']} options='-c search_path=idrac8'"

TABLES = [
    "aggregated_rpmreading",
    "aggregated_systempowerconsumption",
    "aggregated_temperaturereading",
]

TIMEDELTA_DAYS = 7


def main():
    """Aggregates records from deduplicated tables older than TIMEDELTA_DAYS.
    """

    end_date = datetime.now(pytz.utc).replace(second=0, microsecond=0)
    end_date -= timedelta(days=TIMEDELTA_DAYS)
    start_date = end_date - timedelta(days=TIMEDELTA_DAYS)
    
    logger.info("start_date: %s", start_date)
    logger.info("end_date: %s", end_date)

    with psycopg2.connect(CONNECTION_STRING) as conn:
        for table in TABLES:
            try:
                logger.info("Creating %s table if not exists", table)
                create_table(conn, table)
                
                deduplicated_table = table.replace("aggregated", "deduplicated")
                logger.info("Aggregating records from %s", deduplicated_table)
                
                aggregated_records = aggregate_records(conn, deduplicated_table, start_date, end_date)
                logger.info("Aggregated down to %s records", len(aggregated_records))
                
                logger.info("Inserting %s aggregated records into %s", len(aggregated_records), table)
                insert_aggregated_records(conn, table, aggregated_records)
            except Exception as err:
                logger.error("%s", err)


if __name__ == "__main__":
    main()
